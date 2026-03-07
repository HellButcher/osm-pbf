//! Convenience helpers for accessing OSM PBF primitives.
//!
//! # Tag access
//!
//! [`NodeRef`], [`WayRef`], and [`RelationRef`] provide zero-argument
//! `.tags() -> Tags<'msg>` using the string table embedded in their
//! [`PrimitiveGroupRef`]. For lower-level access, [`NodeView`], [`WayView`],
//! and [`RelationView`] can be wrapped into the corresponding `Ref` type
//! using [`AbstractRef::new`].
//!
//! # Uniform node access
//!
//! Both regular [`NodeRef`] and [`DenseNodeRef`] values are wrapped in the
//! [`AnyNodeRef`] enum, which exposes `id`, `lat`, and `lon` uniformly.
//! Match on the variant to access tags or decoded coordinates:
//! - `Regular(n)` — call `n.tags()`, `n.lat_deg()`, `n.lon_deg()` directly.
//! - `Dense(n)` — call `n.tags()`, `n.lat_deg()`, `n.lon_deg()` directly.
//!
//! # Context-aware element refs
//!
//! [`NodeRef`], [`WayRef`], [`RelationRef`], and [`ChangeSetRef`] are type
//! aliases for [`AbstractRef`], which bundles a protobuf view together with a
//! [`PrimitiveGroupRef`]. This makes tag access and coordinate decoding
//! argument-free:
//!
//! ```ignore
//! let lat = node_ref.lat_deg();        // no block argument needed
//! let name = node_ref.tags().get("name");
//! ```
//!
//! # PrimitiveGroup iteration
//!
//! [`PrimitiveGroupRef`] is the primary entry point for group-level iteration.
//! Construct one from a group view and its parent block, then call:
//!
//! | Method | Yields |
//! |--------|--------|
//! | [`PrimitiveGroupRef::iter_nodes`] | [`NodeRef<'msg>`] (regular nodes only) |
//! | [`PrimitiveGroupRef::iter_dense_nodes`] | [`DenseNodeRef<'msg>`] (dense nodes only) |
//! | [`PrimitiveGroupRef::iter_all_nodes`] | [`AnyNodeRef<'msg>`] (all nodes) |
//! | [`PrimitiveGroupRef::iter_ways`] | [`WayRef<'msg>`] |
//! | [`PrimitiveGroupRef::iter_relations`] | [`RelationRef<'msg>`] |
//! | [`PrimitiveGroupRef::iter_changesets`] | [`ChangeSetRef<'msg>`] |
//! | [`PrimitiveGroupRef::iter_elements`] | [`Element<'msg>`] (all element types) |
//! | [`PrimitiveGroupRef::iter_filtered_elements`] | [`Element<'msg>`] (filtered by [`ElementTypes`]) |
//!
//! # PrimitiveBlock iteration
//!
//! [`PrimitiveBlockView`] provides folded iterators that chain across all groups:
//!
//! | Method | Yields |
//! |--------|--------|
//! | [`PrimitiveBlockView::iter_groups`] | [`PrimitiveGroupRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_nodes`] | [`NodeRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_dense_nodes`] | [`DenseNodeRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_all_nodes`] | [`AnyNodeRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_ways`] | [`WayRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_relations`] | [`RelationRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_changesets`] | [`ChangeSetRef<'msg>`] |
//! | [`PrimitiveBlockView::iter_elements`] | [`Element<'msg>`] |
//! | [`PrimitiveBlockView::iter_filtered_elements`] | [`Element<'msg>`] (filtered by [`ElementTypes`]) |
//!
//! `iter_filtered_elements` skips entire groups whose element type is not in
//! the [`ElementTypes`] bitset — groups are never decoded for filtered-out types.
//!
//! # Coordinate decoding
//!
//! [`decode_coordinate`] converts a raw stored value to degrees using the
//! block's `lat_offset` / `lon_offset` and `granularity` fields.

use std::str;

use protobuf::{ProtoBytes, Proxied, RepeatedView};

use crate::protos::{
    ChangeSet, DenseNodesView, Node, PrimitiveBlockView, PrimitiveGroupView, Relation, Way,
};

// ── ElementTypes ──────────────────────────────────────────────────────────────

bitflags::bitflags! {
    /// A bitset of [`Element`] variant kinds for use with
    /// [`PrimitiveBlockView::iter_filtered_elements`] and
    /// [`PrimitiveGroupRef::iter_filtered_elements`].
    ///
    /// ```ignore
    /// let types = ElementTypes::NODES | ElementTypes::WAYS;
    /// for element in block.iter_filtered_elements(types) { ... }
    /// ```
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct ElementTypes: u8 {
        /// Regular and dense nodes.
        const NODES      = 0b0001;
        /// Ways.
        const WAYS       = 0b0010;
        /// Relations.
        const RELATIONS  = 0b0100;
        /// Changesets.
        const CHANGESETS = 0b1000;
    }
}

// ── Tags ──────────────────────────────────────────────────────────────────────

/// Map-like view of OSM element tags, backed by parallel key/value index
/// arrays and the block's string table.
///
/// Tag keys and values are stored as `u32` string-table indices. This type
/// resolves them to `&str` on access. Tags with non-UTF-8 bytes are silently
/// skipped (the OSM spec requires UTF-8 for all tag strings).
///
/// Per the OSM PBF spec, string-table index `0` is reserved: it always holds
/// an empty string and is never used as an actual key or value reference.
/// Any key or value with index `0` is therefore treated as absent.
#[derive(Clone, Copy)]
pub struct Tags<'msg> {
    keys: RepeatedView<'msg, u32>,
    vals: RepeatedView<'msg, u32>,
    stringtable: RepeatedView<'msg, ProtoBytes>,
}

impl<'msg> Tags<'msg> {
    #[inline]
    pub(crate) fn new(
        keys: RepeatedView<'msg, u32>,
        vals: RepeatedView<'msg, u32>,
        stringtable: RepeatedView<'msg, ProtoBytes>,
    ) -> Self {
        Self {
            keys,
            vals,
            stringtable,
        }
    }

    /// Returns the number of tags.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns `true` if there are no tags.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.len() == 0
    }

    /// Returns the value for `key`, or `None` if the key is not present.
    ///
    /// Performs a linear scan — O(n) in the number of tags.
    pub fn get(&self, key: &str) -> Option<&'msg str> {
        for i in 0..self.keys.len() {
            let k_idx = self.keys.get(i)? as usize;
            let k_bytes: &'msg [u8] = self.stringtable.get(k_idx)?;
            if k_bytes == key.as_bytes() {
                let v_idx = self.vals.get(i)? as usize;
                let v_bytes: &'msg [u8] = self.stringtable.get(v_idx)?;
                return str::from_utf8(v_bytes).ok();
            }
        }
        None
    }

    /// Returns `true` if this element has a tag with the given key.
    pub fn contains_key(&self, key: &str) -> bool {
        for i in 0..self.keys.len() {
            let Some(k_idx) = self.keys.get(i) else { break };
            let Some(k_bytes): Option<&'msg [u8]> = self.stringtable.get(k_idx as usize) else {
                break;
            };
            if k_bytes == key.as_bytes() {
                return true;
            }
        }
        false
    }

    /// Returns an iterator over `(key, value)` string pairs.
    #[inline]
    pub fn iter(&self) -> TagsIter<'msg> {
        TagsIter {
            keys: self.keys,
            vals: self.vals,
            stringtable: self.stringtable,
            pos: 0,
        }
    }
}

impl<'msg> IntoIterator for Tags<'msg> {
    type Item = (&'msg str, &'msg str);
    type IntoIter = TagsIter<'msg>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        TagsIter {
            keys: self.keys,
            vals: self.vals,
            stringtable: self.stringtable,
            pos: 0,
        }
    }
}

/// Iterator over OSM tag key/value string pairs.
pub struct TagsIter<'msg> {
    keys: RepeatedView<'msg, u32>,
    vals: RepeatedView<'msg, u32>,
    stringtable: RepeatedView<'msg, ProtoBytes>,
    pos: usize,
}

impl<'msg> Iterator for TagsIter<'msg> {
    type Item = (&'msg str, &'msg str);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos >= self.keys.len() {
                return None;
            }
            let k_idx = self.keys.get(self.pos)? as usize;
            let v_idx = self.vals.get(self.pos)? as usize;
            self.pos += 1;
            let Some(k_bytes): Option<&'msg [u8]> = self.stringtable.get(k_idx) else {
                continue;
            };
            let Some(v_bytes): Option<&'msg [u8]> = self.stringtable.get(v_idx) else {
                continue;
            };
            let (Ok(k), Ok(v)) = (str::from_utf8(k_bytes), str::from_utf8(v_bytes)) else {
                continue;
            };
            return Some((k, v));
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.keys.len().saturating_sub(self.pos)))
    }
}

// ── DenseNodeTags ─────────────────────────────────────────────────────────────

/// Map-like tag access for a single decoded [`DenseNodeRef`].
///
/// Backed by a slice of the parent `DenseNodes.keys_vals` flat array. Tags are
/// encoded as consecutive `(key_idx, val_idx)` pairs; a `0` sentinel delimits
/// nodes. This type's range `[kv_start, kv_end)` already excludes the sentinel.
///
/// ### Note on `keys_vals` type
/// The [OSM PBF spec] declares `keys_vals` as `repeated uint32`, but the proto
/// files in this repository use `int32`. All raw `i32` values are reinterpreted
/// as `u32` at the point of use: `0` is the node delimiter, and positive values
/// are valid string-table indices (values that appear negative as `i32` are
/// large indices in very large string tables and are handled correctly).
///
/// [OSM PBF spec]: https://wiki.openstreetmap.org/wiki/PBF_Format
#[derive(Clone, Copy)]
pub struct DenseNodeTags<'msg> {
    keys_vals: RepeatedView<'msg, i32>,
    kv_start: usize,
    kv_end: usize,
    stringtable: RepeatedView<'msg, ProtoBytes>,
}

impl<'msg> DenseNodeTags<'msg> {
    /// Returns the number of tags.
    #[inline]
    pub fn len(&self) -> usize {
        self.kv_end.saturating_sub(self.kv_start) / 2
    }

    /// Returns `true` if there are no tags.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.kv_start >= self.kv_end
    }

    /// Returns the value for `key`, or `None` if the key is not present.
    pub fn get(&self, key: &str) -> Option<&'msg str> {
        let mut pos = self.kv_start;
        while pos < self.kv_end {
            let k_idx = self.keys_vals.get(pos)? as u32;
            let v_idx = self.keys_vals.get(pos + 1)? as u32;
            pos += 2;
            if k_idx == 0 {
                break;
            }
            let Some(k_bytes): Option<&'msg [u8]> = self.stringtable.get(k_idx as usize) else {
                continue;
            };
            if k_bytes == key.as_bytes() {
                if v_idx == 0 {
                    return None;
                }
                let v_bytes: Option<&'msg [u8]> = self.stringtable.get(v_idx as usize);
                return v_bytes.and_then(|b| str::from_utf8(b).ok());
            }
        }
        None
    }

    /// Returns `true` if this node has a tag with the given key.
    pub fn contains_key(&self, key: &str) -> bool {
        let mut pos = self.kv_start;
        while pos < self.kv_end {
            let Some(k_idx) = self.keys_vals.get(pos) else {
                break;
            };
            let k_idx = k_idx as u32;
            if k_idx == 0 {
                break;
            }
            let Some(k_bytes): Option<&'msg [u8]> = self.stringtable.get(k_idx as usize) else {
                pos += 2;
                continue;
            };
            if k_bytes == key.as_bytes() {
                return true;
            }
            pos += 2;
        }
        false
    }

    /// Returns an iterator over `(key, value)` string pairs.
    #[inline]
    pub fn iter(&self) -> DenseNodeTagsIter<'msg> {
        DenseNodeTagsIter {
            keys_vals: self.keys_vals,
            kv_pos: self.kv_start,
            kv_end: self.kv_end,
            stringtable: self.stringtable,
        }
    }
}

impl<'msg> IntoIterator for DenseNodeTags<'msg> {
    type Item = (&'msg str, &'msg str);
    type IntoIter = DenseNodeTagsIter<'msg>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        DenseNodeTagsIter {
            keys_vals: self.keys_vals,
            kv_pos: self.kv_start,
            kv_end: self.kv_end,
            stringtable: self.stringtable,
        }
    }
}

/// Iterator over tag key/value string pairs of a single [`DenseNodeRef`].
pub struct DenseNodeTagsIter<'msg> {
    keys_vals: RepeatedView<'msg, i32>,
    kv_pos: usize,
    kv_end: usize,
    stringtable: RepeatedView<'msg, ProtoBytes>,
}

impl<'msg> Iterator for DenseNodeTagsIter<'msg> {
    type Item = (&'msg str, &'msg str);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.kv_pos >= self.kv_end {
                return None;
            }
            let k_idx = self.keys_vals.get(self.kv_pos)? as u32;
            let v_idx = self.keys_vals.get(self.kv_pos + 1)? as u32;
            self.kv_pos += 2;
            if k_idx == 0 || v_idx == 0 {
                return None;
            }
            let Some(k_bytes): Option<&'msg [u8]> = self.stringtable.get(k_idx as usize) else {
                continue;
            };
            let Some(v_bytes): Option<&'msg [u8]> = self.stringtable.get(v_idx as usize) else {
                continue;
            };
            let (Ok(k), Ok(v)) = (str::from_utf8(k_bytes), str::from_utf8(v_bytes)) else {
                continue;
            };
            return Some((k, v));
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.kv_end.saturating_sub(self.kv_pos) / 2))
    }
}

// ── DenseNodeRef ─────────────────────────────────────────────────────────────────

/// A single node decoded from a [`DenseNodes`] group.
///
/// The `lat` and `lon` fields hold the DELTA-decoded raw values — the same
/// format as [`NodeView::lat`] / [`NodeView::lon`]. Use [`decode_coordinate`]
/// to convert them to degrees.
///
/// ### DenseNodes encoding (per OSM PBF spec)
/// Coordinates and IDs are delta-encoded across the sequence: each stored value
/// is the *difference* from the previous node's value. `DenseNodeRefsIter`
/// accumulates these deltas to produce the absolute values stored here.
/// Tags are stored in the flat `keys_vals` array as interleaved `(key, val)`
/// index pairs separated by `0` sentinels — one per node, even for tagless
/// nodes (unless the *entire* group is tagless, in which case `keys_vals` is
/// omitted).
///
/// [`DenseNodes`]: crate::protos::DenseNodes
pub struct DenseNodeRef<'msg> {
    /// Node ID (DELTA-decoded).
    id: i64,
    /// Raw latitude value (DELTA-decoded; same encoding as [`NodeView::lat`]).
    lat: i64,
    /// Raw longitude value (DELTA-decoded; same encoding as [`NodeView::lon`]).
    lon: i64,
    keys_vals: RepeatedView<'msg, i32>,
    kv_start: usize,
    kv_end: usize,
    group: PrimitiveGroupRef<'msg>,
}

impl<'msg> DenseNodeRef<'msg> {
    #[inline]
    pub fn id(&self) -> i64 {
        self.id
    }

    #[inline]
    pub fn lat(&self) -> i64 {
        self.lat
    }

    #[inline]
    pub fn lon(&self) -> i64 {
        self.lon
    }

    /// Decodes the latitude to degrees using the parent block's offset and
    /// granularity.
    #[inline]
    pub fn lat_deg(&self) -> f64 {
        decode_coordinate(
            self.lat,
            self.group.block.lat_offset(),
            self.group.block.granularity(),
        )
    }

    /// Decodes the longitude to degrees using the parent block's offset and
    /// granularity.
    #[inline]
    pub fn lon_deg(&self) -> f64 {
        decode_coordinate(
            self.lon,
            self.group.block.lon_offset(),
            self.group.block.granularity(),
        )
    }

    /// Returns map-like access to this node's tags.
    #[inline]
    pub fn tags(&self) -> DenseNodeTags<'msg> {
        DenseNodeTags {
            keys_vals: self.keys_vals,
            kv_start: self.kv_start,
            kv_end: self.kv_end,
            stringtable: self.group.stringtable,
        }
    }
}

// ── DenseNodeRefsIter ─────────────────────────────────────────────────────────

/// Iterator that delta-decodes a [`DenseNodesView`] and yields individual
/// [`DenseNodeRef`] values.
///
/// Obtain via [`DenseNodesView::iter_nodes`].
pub struct DenseNodeRefsIter<'msg> {
    dense: DenseNodesView<'msg>,
    group: PrimitiveGroupRef<'msg>,
    /// Index of the next node to decode.
    pos: usize,
    /// Current position in `keys_vals` (advances past each node's tag slice).
    kv_pos: usize,
    acc_id: i64,
    acc_lat: i64,
    acc_lon: i64,
}

impl<'msg> DenseNodeRefsIter<'msg> {
    #[inline]
    fn new(dense: DenseNodesView<'msg>, group: PrimitiveGroupRef<'msg>) -> Self {
        Self {
            dense,
            group,
            pos: 0,
            kv_pos: 0,
            acc_id: 0,
            acc_lat: 0,
            acc_lon: 0,
        }
    }
}

impl<'msg> Iterator for DenseNodeRefsIter<'msg> {
    type Item = DenseNodeRef<'msg>;

    fn next(&mut self) -> Option<DenseNodeRef<'msg>> {
        if self.pos >= self.dense.id().len() {
            return None;
        }

        // Delta-decode coordinate columns.
        self.acc_id += self.dense.id().get(self.pos)?;
        self.acc_lat += self.dense.lat().get(self.pos)?;
        self.acc_lon += self.dense.lon().get(self.pos)?;
        self.pos += 1;

        // Locate the keys_vals slice for this node.
        let keys_vals = self.dense.keys_vals();
        let kv_len = keys_vals.len();
        let kv_start = self.kv_pos;
        let kv_end = if kv_len == 0 {
            // keys_vals omitted — all nodes in this group are tagless.
            kv_start
        } else {
            // TODO: try to optimize, by not requiring the kv_end bound on each iteration. (then DenseTaghs is not an exact-size iterator anymore)

            // Scan for the 0-delimiter that terminates this node's tag pairs.
            loop {
                if self.kv_pos >= kv_len {
                    // Malformed: no delimiter found; consume remaining.
                    break self.kv_pos;
                }
                if keys_vals.get(self.kv_pos) == Some(0) {
                    let end = self.kv_pos;
                    self.kv_pos += 1; // skip delimiter
                    break end;
                }
                self.kv_pos += 2; // advance past (key, val) pair
            }
        };

        Some(DenseNodeRef {
            id: self.acc_id,
            lat: self.acc_lat,
            lon: self.acc_lon,
            keys_vals,
            kv_start,
            kv_end,
            group: self.group,
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.dense.id().len().saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl std::iter::ExactSizeIterator for DenseNodeRefsIter<'_> {}

// ── AnyNodeRef ───────────────────────────────────────────────────────────────────

/// A reference to a single OSM node, regardless of whether it came from the
/// `nodes` field (a regular [`NodeRef`]) or from a [`DenseNodes`] group (a
/// decoded [`DenseNodeRef`]).
///
/// Both variants carry a [`PrimitiveGroupRef`] for full block context.
/// Use `id()`, `lat()`, `lon()`, `lat_deg()`, and `lon_deg()` directly on
/// `AnyNodeRef`. For tag access, match on the variant: `Regular(n)` provides
/// `n.tags()` returning [`Tags`]; `Dense(n)` provides `n.tags()` returning
/// [`DenseNodeTags`].
pub enum AnyNodeRef<'msg> {
    /// A regular (non-dense) node with full block context.
    Regular(NodeRef<'msg>),
    /// A node decoded from a `DenseNodes` group.
    Dense(DenseNodeRef<'msg>),
}

impl<'msg> AnyNodeRef<'msg> {
    /// Returns the node ID.
    #[inline]
    pub fn id(&self) -> i64 {
        match self {
            Self::Regular(n) => n.id(),
            Self::Dense(n) => n.id(),
        }
    }

    /// Returns the raw latitude value (same encoding as [`NodeView::lat`]).
    #[inline]
    pub fn lat(&self) -> i64 {
        match self {
            Self::Regular(n) => n.lat(),
            Self::Dense(n) => n.lat(),
        }
    }

    /// Returns the raw longitude value (same encoding as [`NodeView::lon`]).
    #[inline]
    pub fn lon(&self) -> i64 {
        match self {
            Self::Regular(n) => n.lon(),
            Self::Dense(n) => n.lon(),
        }
    }

    /// Decodes the latitude to degrees using the parent block's offset and
    /// granularity.
    #[inline]
    pub fn lat_deg(&self) -> f64 {
        match self {
            Self::Regular(n) => n.lat_deg(),
            Self::Dense(n) => n.lat_deg(),
        }
    }

    /// Decodes the longitude to degrees using the parent block's offset and
    /// granularity.
    #[inline]
    pub fn lon_deg(&self) -> f64 {
        match self {
            Self::Regular(n) => n.lon_deg(),
            Self::Dense(n) => n.lon_deg(),
        }
    }
}

// -- PrimitiveGroupRef ────────────────────────────────────────────────────────

/// A [`PrimitiveGroupView`] together with its owning block's context.
///
/// Carries everything needed to decode elements without passing separate
/// arguments: the group view itself, the block view (for granularity and
/// coordinate offsets), and the pre-extracted string table.
#[derive(Clone, Copy)]
pub struct PrimitiveGroupRef<'msg> {
    view: PrimitiveGroupView<'msg>,
    block: PrimitiveBlockView<'msg>,
    stringtable: RepeatedView<'msg, ProtoBytes>,
}

impl<'msg> PrimitiveGroupRef<'msg> {
    /// Creates a new `PrimitiveGroupRef` from a group view and its parent block.
    ///
    /// The string table is extracted from the block once and cached.
    #[inline]
    pub fn new(view: PrimitiveGroupView<'msg>, block: PrimitiveBlockView<'msg>) -> Self {
        let stringtable = block.stringtable().s();
        Self {
            view,
            block,
            stringtable,
        }
    }

    /// Returns the underlying group view.
    #[inline]
    pub fn view(self) -> PrimitiveGroupView<'msg> {
        self.view
    }

    /// Returns the parent block view.
    #[inline]
    pub fn block(self) -> PrimitiveBlockView<'msg> {
        self.block
    }

    /// Returns the raw string-table byte array.
    #[inline]
    pub fn stringtable(self) -> RepeatedView<'msg, ProtoBytes> {
        self.stringtable
    }

    /// Returns an iterator over regular (non-dense) nodes as [`NodeRef`] values.
    #[inline]
    pub fn iter_nodes(self) -> impl Iterator<Item = NodeRef<'msg>> {
        self.view
            .nodes()
            .into_iter()
            .map(move |view| NodeRef::new(view, self))
    }

    /// Returns an iterator that delta-decodes and yields individual
    /// [`DenseNodeRef`] values from the dense node group.
    ///
    /// Returns an empty iterator if this group has no dense nodes.
    #[inline]
    pub fn iter_dense_nodes(self) -> DenseNodeRefsIter<'msg> {
        DenseNodeRefsIter::new(self.view.dense(), self)
    }

    /// Returns an iterator over **all** nodes as uniform [`AnyNodeRef`] values —
    /// regular nodes first, then dense nodes.
    #[inline]
    pub fn iter_all_nodes(self) -> impl Iterator<Item = AnyNodeRef<'msg>> {
        self.iter_nodes()
            .map(AnyNodeRef::Regular)
            .chain(self.iter_dense_nodes().map(AnyNodeRef::Dense))
    }

    /// Returns an iterator over all ways as [`WayRef`] values.
    #[inline]
    pub fn iter_ways(self) -> impl Iterator<Item = WayRef<'msg>> {
        self.view
            .ways()
            .into_iter()
            .map(move |view| WayRef::new(view, self))
    }

    /// Returns an iterator over all relations as [`RelationRef`] values.
    #[inline]
    pub fn iter_relations(self) -> impl Iterator<Item = RelationRef<'msg>> {
        self.view
            .relations()
            .into_iter()
            .map(move |view| RelationRef::new(view, self))
    }

    /// Returns an iterator over all changesets as [`ChangeSetRef`] values.
    #[inline]
    pub fn iter_changesets(self) -> impl Iterator<Item = ChangeSetRef<'msg>> {
        self.view
            .changesets()
            .into_iter()
            .map(move |view| ChangeSetRef::new(view, self))
    }

    /// Returns an iterator over **all** elements as [`Element`] values,
    /// in the order: nodes (regular then dense), ways, relations, changesets.
    #[inline]
    pub fn iter_elements(self) -> impl Iterator<Item = Element<'msg>> {
        self.iter_all_nodes()
            .map(Element::Node)
            .chain(self.iter_ways().map(Element::Way))
            .chain(self.iter_relations().map(Element::Relation))
            .chain(self.iter_changesets().map(Element::Changeset))
    }

    /// Returns an iterator over elements in this group, filtered by `types`.
    ///
    /// If this group's element type is not in `types`, the group is skipped
    /// entirely — its elements are never decoded. Per the OSM PBF spec, each
    /// `PrimitiveGroup` holds exactly one element type, so this check is O(1).
    #[inline]
    pub fn iter_filtered_elements(self, types: ElementTypes) -> impl Iterator<Item = Element<'msg>> {
        group_type_flags(self.view)
            .intersects(types)
            .then(move || self.iter_elements())
            .into_iter()
            .flatten()
    }
}

/// Returns the [`ElementTypes`] flag describing the element type stored in `view`.
///
/// Per the OSM PBF spec, each `PrimitiveGroup` holds exactly one element type.
/// Returns [`ElementTypes::empty()`] for an empty group.
fn group_type_flags(view: PrimitiveGroupView<'_>) -> ElementTypes {
    if !view.nodes().is_empty() || !view.dense().id().is_empty() {
        ElementTypes::NODES
    } else if !view.ways().is_empty() {
        ElementTypes::WAYS
    } else if !view.relations().is_empty() {
        ElementTypes::RELATIONS
    } else if !view.changesets().is_empty() {
        ElementTypes::CHANGESETS
    } else {
        ElementTypes::empty()
    }
}

// -- AbstractRef ───────────────────────────────────────────────────────────────

/// A decoded OSM element view together with its enclosing [`PrimitiveGroupRef`].
///
/// Wraps a protobuf `View` type (e.g. [`NodeView`], [`WayView`]) and carries
/// enough context to resolve tags and decode coordinates without the caller
/// needing to pass a string table or block parameters separately.
///
/// Dereferences to the underlying `T::View<'msg>`, so all methods of the raw
/// view are accessible through auto-deref.
pub struct AbstractRef<'msg, T: Proxied> {
    view: T::View<'msg>,
    group: PrimitiveGroupRef<'msg>,
}

impl<'msg, T: Proxied> AbstractRef<'msg, T> {
    /// Wraps `view` together with its `group` context.
    #[inline]
    pub fn new(view: T::View<'msg>, group: PrimitiveGroupRef<'msg>) -> Self {
        Self { view, group }
    }

    /// Returns the enclosing group (including block and string table).
    #[inline]
    pub fn group(&self) -> PrimitiveGroupRef<'msg> {
        self.group
    }
}

impl<'msg, T: Proxied> std::ops::Deref for AbstractRef<'msg, T> {
    type Target = T::View<'msg>;

    #[inline]
    fn deref(&self) -> &T::View<'msg> {
        &self.view
    }
}

pub type NodeRef<'msg> = AbstractRef<'msg, Node>;
pub type WayRef<'msg> = AbstractRef<'msg, Way>;
pub type RelationRef<'msg> = AbstractRef<'msg, Relation>;
pub type ChangeSetRef<'msg> = AbstractRef<'msg, ChangeSet>;

// ── NodeRef impls ─────────────────────────────────────────────────────────────

impl<'msg> NodeRef<'msg> {
    /// Returns map-like access to this node's tags.
    ///
    /// Uses the string table embedded in the [`PrimitiveGroupRef`]; no extra
    /// argument needed.
    #[inline]
    pub fn tags(&self) -> Tags<'msg> {
        Tags::new(self.view.keys(), self.view.vals(), self.group.stringtable)
    }

    /// Decodes the latitude to degrees using the parent block's offset and
    /// granularity.
    ///
    /// Equivalent to `decode_coordinate(self.lat(), block.lat_offset(), block.granularity())`.
    #[inline]
    pub fn lat_deg(&self) -> f64 {
        decode_coordinate(
            self.view.lat(),
            self.group.block.lat_offset(),
            self.group.block.granularity(),
        )
    }

    /// Decodes the longitude to degrees using the parent block's offset and
    /// granularity.
    ///
    /// Equivalent to `decode_coordinate(self.lon(), block.lon_offset(), block.granularity())`.
    #[inline]
    pub fn lon_deg(&self) -> f64 {
        decode_coordinate(
            self.view.lon(),
            self.group.block.lon_offset(),
            self.group.block.granularity(),
        )
    }
}

// ── WayRef impls ──────────────────────────────────────────────────────────────

impl<'msg> WayRef<'msg> {
    /// Returns map-like access to this way's tags.
    #[inline]
    pub fn tags(&self) -> Tags<'msg> {
        Tags::new(self.view.keys(), self.view.vals(), self.group.stringtable)
    }
}

// ── RelationRef impls ─────────────────────────────────────────────────────────

impl<'msg> RelationRef<'msg> {
    /// Returns map-like access to this relation's tags.
    #[inline]
    pub fn tags(&self) -> Tags<'msg> {
        Tags::new(self.view.keys(), self.view.vals(), self.group.stringtable)
    }
}

// ── Element ───────────────────────────────────────────────────────────────────

/// An OSM element decoded from a [`PrimitiveGroup`].
///
/// [`PrimitiveGroup`]: crate::protos::PrimitiveGroup
pub enum Element<'msg> {
    /// A node (regular or dense) with full block context.
    ///
    /// Match on the inner [`AnyNodeRef`] to distinguish regular from dense
    /// nodes and to access tags or decoded coordinates.
    Node(AnyNodeRef<'msg>),
    /// A way with full block context for tag access.
    Way(WayRef<'msg>),
    /// A relation with full block context for tag access.
    Relation(RelationRef<'msg>),
    /// A changeset. Note: changesets carry no tags in the PBF format.
    Changeset(ChangeSetRef<'msg>),
}

// ── Coordinate decoding ───────────────────────────────────────────────────────

/// Decodes a raw stored coordinate value to degrees.
///
/// Uses the official OSM PBF formula (where all values are from the enclosing
/// [`PrimitiveBlock`]):
///
/// ```text
/// latitude  = 1e-9 * (lat_offset + granularity * lat)
/// longitude = 1e-9 * (lon_offset + granularity * lon)
/// ```
///
/// Example:
///
/// ```ignore
/// let lat_deg = decode_coordinate(node.lat(), block.lat_offset(), block.granularity());
/// let lon_deg = decode_coordinate(node.lon(), block.lon_offset(), block.granularity());
/// ```
///
/// `granularity` defaults to 100 (nanodegrees per unit) and offsets default to
/// 0; both are stored in the `PrimitiveBlock` header.
///
/// [`PrimitiveBlock`]: crate::protos::PrimitiveBlock
#[inline]
pub fn decode_coordinate(raw: i64, offset: i64, granularity: i32) -> f64 {
    (offset + granularity as i64 * raw) as f64 / 1_000_000_000.0
}

// ── PrimitiveBlockView inherent methods ───────────────────────────────────────

impl<'msg> PrimitiveBlockView<'msg> {
    /// Returns an iterator over all groups in this block as [`PrimitiveGroupRef`]
    /// values, each carrying the block's coordinate context and string table.
    #[inline]
    pub fn iter_groups(self) -> impl Iterator<Item = PrimitiveGroupRef<'msg>> {
        self.primitivegroup()
            .into_iter()
            .map(move |view| PrimitiveGroupRef::new(view, self))
    }

    /// Returns an iterator over all regular (non-dense) nodes across all groups.
    #[inline]
    pub fn iter_nodes(self) -> impl Iterator<Item = NodeRef<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_nodes)
    }

    /// Returns an iterator over all dense nodes across all groups.
    #[inline]
    pub fn iter_dense_nodes(self) -> impl Iterator<Item = DenseNodeRef<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_dense_nodes)
    }

    /// Returns an iterator over all nodes across all groups as [`AnyNodeRef`] values.
    #[inline]
    pub fn iter_all_nodes(self) -> impl Iterator<Item = AnyNodeRef<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_all_nodes)
    }

    /// Returns an iterator over all ways across all groups.
    #[inline]
    pub fn iter_ways(self) -> impl Iterator<Item = WayRef<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_ways)
    }

    /// Returns an iterator over all relations across all groups.
    #[inline]
    pub fn iter_relations(self) -> impl Iterator<Item = RelationRef<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_relations)
    }

    /// Returns an iterator over all changesets across all groups.
    #[inline]
    pub fn iter_changesets(self) -> impl Iterator<Item = ChangeSetRef<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_changesets)
    }

    /// Returns an iterator over all elements across all groups, in group order.
    #[inline]
    pub fn iter_elements(self) -> impl Iterator<Item = Element<'msg>> {
        self.iter_groups().flat_map(PrimitiveGroupRef::iter_elements)
    }

    /// Returns an iterator over elements across all groups, filtered by `types`.
    ///
    /// Groups whose element type is not in `types` are skipped entirely —
    /// their contents are never decoded. Per the OSM PBF spec, each group
    /// holds exactly one element type, so the per-group check is O(1).
    ///
    /// ```ignore
    /// // Iterate only nodes and ways, skipping relation and changeset groups:
    /// let types = ElementTypes::NODES | ElementTypes::WAYS;
    /// for element in block.iter_filtered_elements(types) { ... }
    /// ```
    #[inline]
    pub fn iter_filtered_elements(self, types: ElementTypes) -> impl Iterator<Item = Element<'msg>> {
        self.iter_groups()
            .flat_map(move |g| g.iter_filtered_elements(types))
    }
}
