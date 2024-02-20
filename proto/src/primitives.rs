use std::ops::Deref;

use bitflags::bitflags;
use bytes::Bytes;
use protobuf::SpecialFields;

use crate::osmformat::{
    ChangeSet, DenseInfo, Info, Node, PrimitiveBlock, PrimitiveGroup, Relation, Way,
};

bitflags! {
    pub struct PrimitiveType: u32 {
        const NODE = 1;
        const WAY = 2;
        const RELATION = 4;
        const CHANGE_SET = 8;

        const DEFAULT = Self::NODE.bits() | Self::WAY.bits() | Self::RELATION.bits();
    }
}

#[derive(Copy, Clone)]
pub struct PrimitiveRef<'l, T: ?Sized> {
    value: &'l T,
    block: &'l PrimitiveBlock,
}

impl<T: ?Sized> Deref for PrimitiveRef<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
    }
}

pub type PrimitiveGroupRef<'l> = PrimitiveRef<'l, PrimitiveGroup>;
pub type WayRef<'l> = PrimitiveRef<'l, Way>;
pub type RelationRef<'l> = PrimitiveRef<'l, Relation>;
pub type ChangeSetRef<'l> = PrimitiveRef<'l, ChangeSet>;

#[derive(PartialEq, Clone, Debug)]
pub struct NodeRef<'l> {
    pub id: i64,
    pub nano_lat: i64,
    pub nano_lon: i64,
    pub index: usize,
    data: NodeData<'l>,
    block: &'l PrimitiveBlock,
}

impl<'l> NodeRef<'l> {
    #[inline]
    fn from_node(index: usize, node: &'l Node, block: &'l PrimitiveBlock) -> Self {
        Self {
            id: node.id(),
            nano_lat: block.lat_offset() + node.lat() * block.granularity() as i64,
            nano_lon: block.lon_offset() + node.lon() * block.granularity() as i64,
            index,
            data: NodeData::Node {
                keys: &node.keys,
                vals: &node.vals,
                info: &node.info,
            },
            block,
        }
    }

    #[inline]
    fn from_dense_node(
        index: usize,
        dense_state: &DenseState,
        kv_pairs: &'l [i32],
        info: &'l DenseInfo,
        block: &'l PrimitiveBlock,
    ) -> Self {
        Self {
            id: dense_state.id,
            nano_lat: block.lat_offset() + dense_state.lat * block.granularity() as i64,
            nano_lon: block.lon_offset() + dense_state.lon * block.granularity() as i64,
            index,
            data: NodeData::DenseNode { kv_pairs, info },
            block,
        }
    }

    #[inline]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Latitude in degrees.
    #[inline]
    pub fn lat(&self) -> f64 {
        self.nano_lat as f64 * 1e-9
    }

    /// Longitude in degrees.
    #[inline]
    pub fn lon(&self) -> f64 {
        self.nano_lon as f64 * 1e-9
    }

    pub fn info(&self) -> Info {
        match self.data {
            NodeData::Node { info, .. } => info.clone(),
            NodeData::DenseNode { info, .. } => Info {
                version: info.version.get(self.index).copied(),
                timestamp: info.timestamp.get(self.index).copied(),
                changeset: info.changeset.get(self.index).copied(),
                uid: info.uid.get(self.index).copied(),
                user_sid: info.user_sid.get(self.index).copied().map(|v| v as u32),
                visible: info.visible.get(self.index).copied(),
                special_fields: SpecialFields::new(),
            },
        }
    }

    pub fn tags(&self) -> Tags<'_> {
        match self.data {
            NodeData::Node { keys, vals, .. } => Tags {
                kv: TagsData::Normal(keys.iter(), vals.iter()),
                s: &self.block.stringtable.s,
            },
            NodeData::DenseNode { kv_pairs, .. } => Tags {
                kv: TagsData::Dense(kv_pairs.iter()),
                s: &self.block.stringtable.s,
            },
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
enum NodeData<'l> {
    Node {
        keys: &'l [u32],
        vals: &'l [u32],
        info: &'l Info,
    },
    DenseNode {
        kv_pairs: &'l [i32],
        info: &'l DenseInfo,
    },
}

#[derive(Default)]
struct DenseState {
    id: i64,
    lat: i64,
    lon: i64,
    kv_pos: usize,
}

impl WayRef<'_> {
    #[inline]
    pub fn tags(&self) -> Tags<'_> {
        Tags {
            kv: TagsData::Normal(self.value.keys.iter(), self.value.vals.iter()),
            s: &self.block.stringtable.s,
        }
    }
}

impl RelationRef<'_> {
    #[inline]
    pub fn tags(&self) -> Tags<'_> {
        Tags {
            kv: TagsData::Normal(self.value.keys.iter(), self.value.vals.iter()),
            s: &self.block.stringtable.s,
        }
    }
}

#[derive(Clone, Debug)]
enum TagsData<'l> {
    Normal(std::slice::Iter<'l, u32>, std::slice::Iter<'l, u32>),
    Dense(std::slice::Iter<'l, i32>),
}

#[derive(Clone, Debug)]
pub struct Tags<'l> {
    kv: TagsData<'l>,
    s: &'l [Bytes],
}

impl<'l> Tags<'l> {
    pub fn get(&self, key: &str) -> Option<&'l str> {
        let key = key.as_bytes();
        let s = self.s;
        match self.kv.clone() {
            TagsData::Normal(mut keys, mut vals) => loop {
                let key_index = keys.next().copied()? as usize;
                if let Some(k) = s.get(key_index) {
                    if k == key {
                        let value_index = vals.next().copied()? as usize;
                        return s.get(value_index).and_then(|b| std::str::from_utf8(b).ok());
                    }
                }
            },
            TagsData::Dense(mut kv_pairs) => loop {
                let key_index = kv_pairs.next().copied()? as usize;
                let value_index = kv_pairs.next().copied()? as usize;
                if let Some(k) = s.get(key_index) {
                    if k == key {
                        return s.get(value_index).and_then(|b| std::str::from_utf8(b).ok());
                    }
                }
            },
        }
    }
}

impl<'l> Iterator for Tags<'l> {
    type Item = (&'l str, &'l str);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let s = self.s;
        loop {
            let key_index;
            let value_index;
            match &mut self.kv {
                TagsData::Normal(keys, vals) => {
                    key_index = keys.next().copied()? as usize;
                    value_index = vals.next().copied()? as usize;
                }
                TagsData::Dense(kv_pairs) => {
                    key_index = kv_pairs.next().copied()? as usize;
                    value_index = kv_pairs.next().copied()? as usize;
                }
            }
            // get string as utf8 (ognore invalid)
            let Some(key) = s.get(key_index).and_then(|b| std::str::from_utf8(b).ok()) else {
                continue;
            };
            let Some(value) = s.get(value_index).and_then(|b| std::str::from_utf8(b).ok()) else {
                continue;
            };
            return Some((key, value));
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.kv {
            TagsData::Normal(keys, vals) => {
                let (keys_lower, keys_upper) = keys.size_hint();
                let (values_lower, values_upper) = vals.size_hint();
                (
                    keys_lower.min(values_lower),
                    keys_upper.zip(values_upper).map(|(k, v)| k.min(v)),
                )
            }
            TagsData::Dense(kv_pairs) => {
                let (lower, upper) = kv_pairs.size_hint();
                (lower / 2, upper.map(|l| l / 2))
            }
        }
    }
}

impl<'l> std::iter::FusedIterator for Tags<'l> {}

#[non_exhaustive]
pub enum Primitive<'l> {
    Node(NodeRef<'l>),
    Way(WayRef<'l>),
    Relation(RelationRef<'l>),
    ChangeSet(ChangeSetRef<'l>),
}

pub struct PrimitivesIter<'l> {
    block: &'l PrimitiveBlock,
    groups: &'l [PrimitiveGroup],
    filter: PrimitiveType,
    group_pos: usize,
    prim_pos: usize,
    dense_state: DenseState,
}

impl PrimitiveBlock {
    #[inline]
    pub fn primitives(&self) -> PrimitivesIter<'_> {
        PrimitivesIter {
            block: self,
            groups: &self.primitivegroup,
            filter: PrimitiveType::DEFAULT,
            group_pos: 0,
            prim_pos: 0,
            dense_state: DenseState::default(),
        }
    }
    #[inline]
    pub fn primitivegroup(&self, i: usize) -> Option<PrimitiveGroupRef<'_>> {
        Some(PrimitiveGroupRef {
            value: self.primitivegroup.get(i)?,
            block: self,
        })
    }
}

impl<'l> PrimitiveGroupRef<'l> {
    #[inline]
    pub fn primitives(self) -> PrimitivesIter<'l> {
        PrimitivesIter {
            block: self.block,
            groups: std::slice::from_ref(self.value),
            filter: PrimitiveType::DEFAULT,
            group_pos: 0,
            prim_pos: 0,
            dense_state: DenseState::default(),
        }
    }
}

impl<'l> PrimitivesIter<'l> {
    #[inline]
    pub fn filter_types(mut self, types: PrimitiveType) -> Self {
        self.filter = types;
        self
    }
}

impl<'l> IntoIterator for &'l PrimitiveBlock {
    type Item = Primitive<'l>;
    type IntoIter = PrimitivesIter<'l>;
    #[inline]
    fn into_iter(self) -> PrimitivesIter<'l> {
        self.primitives()
    }
}

impl<'l> IntoIterator for PrimitiveGroupRef<'l> {
    type Item = Primitive<'l>;
    type IntoIter = PrimitivesIter<'l>;
    #[inline]
    fn into_iter(self) -> PrimitivesIter<'l> {
        self.primitives()
    }
}

impl<'l> Iterator for PrimitivesIter<'l> {
    type Item = Primitive<'l>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let group = self.groups.get(self.group_pos)?;
            if self.filter.contains(PrimitiveType::NODE) && !group.nodes.is_empty() {
                let index = self.prim_pos;
                if let Some(n) = group.nodes.get(index) {
                    self.prim_pos = index + 1;
                    return Some(Primitive::Node(NodeRef::from_node(index, n, self.block)));
                }
            } else if self.filter.contains(PrimitiveType::NODE) && group.dense.is_some() {
                let dense = &group.dense;
                let index = self.prim_pos;
                if let (Some(id), Some(lat), Some(lon)) = (
                    dense.id.get(index).copied(),
                    dense.lat.get(index).copied(),
                    dense.lon.get(index).copied(),
                ) {
                    self.prim_pos = index + 1;
                    self.dense_state.id += id;
                    self.dense_state.lat += lat;
                    self.dense_state.lon += lon;

                    // find range for key-value pairs
                    let kv_from = self.dense_state.kv_pos;
                    while let Some(k) = dense.keys_vals.get(self.dense_state.kv_pos).copied() {
                        if k == 0 {
                            self.dense_state.kv_pos += 1;
                        } else {
                            self.dense_state.kv_pos += 2;
                        }
                    }
                    let key_values = &dense.keys_vals[kv_from..self.dense_state.kv_pos];

                    let n = NodeRef::from_dense_node(
                        index,
                        &self.dense_state,
                        key_values,
                        &dense.denseinfo,
                        self.block,
                    );
                    return Some(Primitive::Node(n));
                }
                // reset dense state for next group
                self.dense_state = DenseState::default();
            } else if self.filter.contains(PrimitiveType::WAY) && !group.ways.is_empty() {
                if let Some(w) = group.ways.get(self.prim_pos) {
                    self.prim_pos += 1;
                    return Some(Primitive::Way(PrimitiveRef {
                        value: w,
                        block: self.block,
                    }));
                }
            } else if self.filter.contains(PrimitiveType::RELATION) && !group.relations.is_empty() {
                if let Some(r) = group.relations.get(self.prim_pos) {
                    self.prim_pos += 1;
                    return Some(Primitive::Relation(PrimitiveRef {
                        value: r,
                        block: self.block,
                    }));
                }
            } else if self.filter.contains(PrimitiveType::CHANGE_SET)
                && !group.changesets.is_empty()
            {
                if let Some(c) = group.changesets.get(self.prim_pos) {
                    self.prim_pos += 1;
                    return Some(Primitive::ChangeSet(PrimitiveRef {
                        value: c,
                        block: self.block,
                    }));
                }
            }
            self.group_pos += 1;
        }
    }
}
