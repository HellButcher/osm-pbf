use bitflags::bitflags;
use osm_pbf_proto::osmformat::PrimitiveGroup as PbfPrimitiveGroup;

use super::{
    changeset::ChangeSet, node::Node, primitive_group::PrimitiveGroup, relation::Relation,
    way::Way, DenseState, Offset, PrimitiveBlock,
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

#[non_exhaustive]
pub enum Primitive<'l> {
    Node(super::node::Node<'l>),
    Way(super::way::Way<'l>),
    Relation(super::relation::Relation<'l>),
    ChangeSet(super::changeset::ChangeSet),
}

pub struct Primitives<'l> {
    strings: &'l [String],
    groups: &'l [PbfPrimitiveGroup],
    filter: PrimitiveType,
    group_pos: usize,
    prim_pos: usize,
    offset: Offset,
    dense_state: DenseState,
}

impl PrimitiveBlock {
    pub fn primitives(&self) -> Primitives<'_> {
        Primitives {
            strings: &self.strings,
            groups: &self.primitive_groups,
            filter: PrimitiveType::DEFAULT,
            group_pos: 0,
            prim_pos: 0,
            offset: self.offset,
            dense_state: DenseState::default(),
        }
    }
}

impl PrimitiveGroup<'_> {
    pub fn primitives(&self) -> Primitives<'_> {
        Primitives {
            strings: &self.block.strings,
            groups: std::slice::from_ref(self.group),
            filter: PrimitiveType::DEFAULT,
            group_pos: 0,
            prim_pos: 0,
            offset: self.block.offset,
            dense_state: DenseState::default(),
        }
    }
}

impl<'l> Primitives<'l> {
    #[inline]
    pub fn types(mut self, types: PrimitiveType) -> Self {
        self.filter = types;
        self
    }
}

impl<'l> Iterator for Primitives<'l> {
    type Item = Primitive<'l>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let group = self.groups.get(self.group_pos)?;
            if self.filter.contains(PrimitiveType::NODE) && !group.nodes.is_empty() {
                if let Some(n) = group.nodes.get(self.prim_pos) {
                    self.prim_pos += 1;
                    let n = Node::from_pbf(n, &self.offset, self.strings);
                    return Some(Primitive::Node(n));
                }
            } else if self.filter.contains(PrimitiveType::NODE) && group.dense.is_some() {
                let dense = &group.dense;
                let prim_pos = self.prim_pos;
                if let (Some(id), Some(lat), Some(lon)) = (
                    dense.id.get(prim_pos).copied(),
                    dense.lat.get(prim_pos).copied(),
                    dense.lon.get(prim_pos).copied(),
                ) {
                    self.prim_pos = prim_pos + 1;
                    self.dense_state.id += id;
                    self.dense_state.lat += lat;
                    self.dense_state.lon += lon;

                    let version;
                    let visible;
                    if let Some(info) = dense.denseinfo.as_ref() {
                        version = info.version.get(prim_pos).copied().unwrap_or(0) as u32;
                        visible = info.visible.get(prim_pos).copied().unwrap_or(true);
                    } else {
                        version = 0;
                        visible = true;
                    }

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

                    let n = Node::from_pbf_dense(
                        self.dense_state,
                        version,
                        visible,
                        &self.offset,
                        key_values,
                        self.strings,
                    );
                    return Some(Primitive::Node(n));
                }
                // reset dense state for next group
                self.dense_state = DenseState::default();
            } else if self.filter.contains(PrimitiveType::WAY) && !group.ways.is_empty() {
                if let Some(w) = group.ways.get(self.prim_pos) {
                    self.prim_pos += 1;
                    let w = Way::from_pbf(w, self.strings);
                    return Some(Primitive::Way(w));
                }
            } else if self.filter.contains(PrimitiveType::RELATION) && !group.relations.is_empty() {
                if let Some(r) = group.relations.get(self.prim_pos) {
                    self.prim_pos += 1;
                    let r = Relation::from_pbf(r, self.strings);
                    return Some(Primitive::Relation(r));
                }
            } else if self.filter.contains(PrimitiveType::CHANGE_SET)
                && !group.changesets.is_empty()
            {
                if let Some(c) = group.changesets.get(self.prim_pos) {
                    self.prim_pos += 1;
                    let c = ChangeSet::from_pbf(c);
                    return Some(Primitive::ChangeSet(c));
                }
            }
            self.group_pos += 1;
        }
    }
}
