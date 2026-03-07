//! Convenience helpers for accessing OSM PBF primitives.
//!
//! # Tag access
//!
//! [`NodeView`], [`WayView`], and [`RelationView`] gain a `.tags(stringtable)`
//! method that returns a [`Tags`] value supporting map-like lookups and
//! iteration over `(&str, &str)` key/value pairs.
//!
//! # Uniform node access
//!
//! [`DenseNodesView`] gains an `.iter_nodes(stringtable)` method that
//! delta-decodes the dense representation and yields individual [`DenseNode`]
//! values.
//!
//! Both regular [`NodeView`] and [`DenseNode`] values are wrapped in the
//! [`NodeRef`] enum, which exposes `id`, `lat`, and `lon` uniformly.
//!
//! # PrimitiveGroup iteration
//!
//! [`PrimitiveGroupView`] gains iterator methods:
//!
//! | Method | Yields |
//! |--------|--------|
//! | [`iter_nodes`] | `NodeView<'msg>` (regular nodes only) |
//! | [`iter_dense_nodes`] | [`DenseNode<'msg>`] (dense nodes only) |
//! | [`iter_all_nodes`] | [`NodeRef<'msg>`] (all nodes, both kinds) |
//! | [`iter_ways`] | `WayView<'msg>` |
//! | [`iter_relations`] | `RelationView<'msg>` |
//! | [`iter_changesets`] | `ChangeSetView<'msg>` |
//! | [`iter_elements`] | [`Element<'msg>`] (everything) |
//!
//! # Coordinate decoding
//!
//! [`decode_coordinate`] converts a raw stored value to degrees using the
//! block's `lat_offset` / `lon_offset` and `granularity` fields.
//!
//! [`iter_nodes`]: PrimitiveGroupView::iter_nodes
//! [`iter_dense_nodes`]: PrimitiveGroupView::iter_dense_nodes
//! [`iter_all_nodes`]: PrimitiveGroupView::iter_all_nodes
//! [`iter_ways`]: PrimitiveGroupView::iter_ways
//! [`iter_relations`]: PrimitiveGroupView::iter_relations
//! [`iter_changesets`]: PrimitiveGroupView::iter_changesets
//! [`iter_elements`]: PrimitiveGroupView::iter_elements

use std::str;

use protobuf::{ProtoBytes, RepeatedView};

use crate::protos::{
    ChangeSetView, DenseNodesView, NodeView, PrimitiveGroupView, RelationView, StringTableView,
    WayView,
};

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

/// Map-like tag access for a single decoded [`DenseNode`].
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

/// Iterator over tag key/value string pairs of a single [`DenseNode`].
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

// ── DenseNode ─────────────────────────────────────────────────────────────────

/// A single node decoded from a [`DenseNodes`] group.
///
/// The `lat` and `lon` fields hold the DELTA-decoded raw values — the same
/// format as [`NodeView::lat`] / [`NodeView::lon`]. Use [`decode_coordinate`]
/// to convert them to degrees.
///
/// ### DenseNodes encoding (per OSM PBF spec)
/// Coordinates and IDs are delta-encoded across the sequence: each stored value
/// is the *difference* from the previous node's value. `DenseNodesIter`
/// accumulates these deltas to produce the absolute values stored here.
/// Tags are stored in the flat `keys_vals` array as interleaved `(key, val)`
/// index pairs separated by `0` sentinels — one per node, even for tagless
/// nodes (unless the *entire* group is tagless, in which case `keys_vals` is
/// omitted).
///
/// [`DenseNodes`]: crate::protos::DenseNodes
pub struct DenseNode<'msg> {
    /// Node ID (DELTA-decoded).
    pub id: i64,
    /// Raw latitude value (DELTA-decoded; same encoding as [`NodeView::lat`]).
    pub lat: i64,
    /// Raw longitude value (DELTA-decoded; same encoding as [`NodeView::lon`]).
    pub lon: i64,
    keys_vals: RepeatedView<'msg, i32>,
    kv_start: usize,
    kv_end: usize,
    stringtable: RepeatedView<'msg, ProtoBytes>,
}

impl<'msg> DenseNode<'msg> {
    /// Returns map-like access to this node's tags.
    #[inline]
    pub fn tags(&self) -> DenseNodeTags<'msg> {
        DenseNodeTags {
            keys_vals: self.keys_vals,
            kv_start: self.kv_start,
            kv_end: self.kv_end,
            stringtable: self.stringtable,
        }
    }
}

// ── DenseNodesIter ────────────────────────────────────────────────────────────

/// Iterator that delta-decodes a [`DenseNodesView`] and yields individual
/// [`DenseNode`] values.
///
/// Obtain via [`DenseNodesView::iter_nodes`].
pub struct DenseNodesIter<'msg> {
    dense: DenseNodesView<'msg>,
    stringtable: RepeatedView<'msg, ProtoBytes>,
    /// Index of the next node to decode.
    pos: usize,
    /// Current position in `keys_vals` (advances past each node's tag slice).
    kv_pos: usize,
    acc_id: i64,
    acc_lat: i64,
    acc_lon: i64,
}

impl<'msg> DenseNodesIter<'msg> {
    #[inline]
    fn new(dense: DenseNodesView<'msg>, stringtable: RepeatedView<'msg, ProtoBytes>) -> Self {
        Self {
            dense,
            stringtable,
            pos: 0,
            kv_pos: 0,
            acc_id: 0,
            acc_lat: 0,
            acc_lon: 0,
        }
    }
}

impl<'msg> Iterator for DenseNodesIter<'msg> {
    type Item = DenseNode<'msg>;

    fn next(&mut self) -> Option<DenseNode<'msg>> {
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

        Some(DenseNode {
            id: self.acc_id,
            lat: self.acc_lat,
            lon: self.acc_lon,
            keys_vals,
            kv_start,
            kv_end,
            stringtable: self.stringtable,
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.dense.id().len().saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl std::iter::ExactSizeIterator for DenseNodesIter<'_> {}

// ── NodeRef ───────────────────────────────────────────────────────────────────

/// A reference to a single OSM node, regardless of whether it came from the
/// `nodes` field (a regular [`NodeView`]) or from a [`DenseNodes`] group (a
/// decoded [`DenseNode`]).
///
/// Both variants expose `id()`, `lat()`, and `lon()`. For tag access, match
/// on the variant and call `.tags(stringtable)` or `.tags()` accordingly.
pub enum NodeRef<'msg> {
    /// A regular (non-dense) node.
    Regular(NodeView<'msg>),
    /// A node decoded from a `DenseNodes` group.
    Dense(DenseNode<'msg>),
}

impl<'msg> NodeRef<'msg> {
    /// Returns the node ID.
    #[inline]
    pub fn id(&self) -> i64 {
        match self {
            Self::Regular(n) => n.id(),
            Self::Dense(n) => n.id,
        }
    }

    /// Returns the raw latitude value (same encoding as [`NodeView::lat`]).
    ///
    /// Use [`decode_coordinate`] to convert to degrees.
    #[inline]
    pub fn lat(&self) -> i64 {
        match self {
            Self::Regular(n) => n.lat(),
            Self::Dense(n) => n.lat,
        }
    }

    /// Returns the raw longitude value (same encoding as [`NodeView::lon`]).
    ///
    /// Use [`decode_coordinate`] to convert to degrees.
    #[inline]
    pub fn lon(&self) -> i64 {
        match self {
            Self::Regular(n) => n.lon(),
            Self::Dense(n) => n.lon,
        }
    }
}

// ── Element ───────────────────────────────────────────────────────────────────

/// An OSM element decoded from a [`PrimitiveGroup`].
///
/// [`PrimitiveGroup`]: crate::protos::PrimitiveGroup
pub enum Element<'msg> {
    /// A regular (non-dense) node.
    Node(NodeView<'msg>),
    /// A node decoded from a `DenseNodes` group.
    DenseNode(DenseNode<'msg>),
    /// A way.
    Way(WayView<'msg>),
    /// A relation.
    Relation(RelationView<'msg>),
    /// A changeset. Note: changesets carry no tags in the PBF format.
    Changeset(ChangeSetView<'msg>),
}

// ── Inherent impl extensions on generated view types ─────────────────────────

impl<'msg> NodeView<'msg> {
    /// Returns map-like access to this node's tags.
    #[inline]
    pub fn tags(self, stringtable: StringTableView<'msg>) -> Tags<'msg> {
        Tags::new(self.keys(), self.vals(), stringtable.s())
    }
}

impl<'msg> WayView<'msg> {
    /// Returns map-like access to this way's tags.
    #[inline]
    pub fn tags(self, stringtable: StringTableView<'msg>) -> Tags<'msg> {
        Tags::new(self.keys(), self.vals(), stringtable.s())
    }
}

impl<'msg> RelationView<'msg> {
    /// Returns map-like access to this relation's tags.
    #[inline]
    pub fn tags(self, stringtable: StringTableView<'msg>) -> Tags<'msg> {
        Tags::new(self.keys(), self.vals(), stringtable.s())
    }
}

impl<'msg> DenseNodesView<'msg> {
    /// Returns an iterator that delta-decodes this group and yields individual
    /// [`DenseNode`] values.
    #[inline]
    pub fn iter_nodes(self, stringtable: StringTableView<'msg>) -> DenseNodesIter<'msg> {
        DenseNodesIter::new(self, stringtable.s())
    }
}

impl<'msg> PrimitiveGroupView<'msg> {
    /// Returns an iterator over regular (non-dense) nodes in this group.
    #[inline]
    pub fn iter_nodes(self) -> impl Iterator<Item = NodeView<'msg>> {
        self.nodes().into_iter()
    }

    /// Returns an iterator that delta-decodes and yields individual
    /// [`DenseNode`] values from the dense node group.
    ///
    /// Returns an empty iterator if this group has no dense nodes.
    #[inline]
    pub fn iter_dense_nodes(self, stringtable: StringTableView<'msg>) -> DenseNodesIter<'msg> {
        let st = stringtable.s();
        let dense = self.dense();
        DenseNodesIter::new(dense, st)
    }

    /// Returns an iterator over **all** nodes in this group as uniform
    /// [`NodeRef`] values — regular nodes first, then dense nodes.
    #[inline]
    pub fn iter_all_nodes(
        self,
        stringtable: StringTableView<'msg>,
    ) -> impl Iterator<Item = NodeRef<'msg>> {
        self.nodes()
            .into_iter()
            .map(NodeRef::Regular)
            .chain(self.iter_dense_nodes(stringtable).map(NodeRef::Dense))
    }

    /// Returns an iterator over all ways in this group.
    #[inline]
    pub fn iter_ways(self) -> impl Iterator<Item = WayView<'msg>> {
        self.ways().into_iter()
    }

    /// Returns an iterator over all relations in this group.
    #[inline]
    pub fn iter_relations(self) -> impl Iterator<Item = RelationView<'msg>> {
        self.relations().into_iter()
    }

    /// Returns an iterator over all changesets in this group.
    #[inline]
    pub fn iter_changesets(self) -> impl Iterator<Item = ChangeSetView<'msg>> {
        self.changesets().into_iter()
    }

    /// Returns an iterator over **all** elements in this group as [`Element`]
    /// values, in the order: nodes, dense nodes, ways, relations, changesets.
    #[inline]
    pub fn iter_elements(
        self,
        stringtable: StringTableView<'msg>,
    ) -> impl Iterator<Item = Element<'msg>> {
        self.nodes()
            .into_iter()
            .map(Element::Node)
            .chain(self.iter_dense_nodes(stringtable).map(Element::DenseNode))
            .chain(self.ways().into_iter().map(Element::Way))
            .chain(self.relations().into_iter().map(Element::Relation))
            .chain(self.changesets().into_iter().map(Element::Changeset))
    }
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
