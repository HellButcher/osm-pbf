use std::ops::Deref;

use osm_pbf_proto::osmformat::Node as PbfNode;

use super::{
    tags::{NodeTagFields, Tags},
    DenseState, Meta,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub i64);

pub struct Node<'l> {
    pub id: NodeId,
    /// Latitude in nanodegrees
    pub nano_lat: i64,
    /// Longitude in nanodegrees
    pub nano_lon: i64,

    strings: &'l [String],
    tags: NodeTagFields<'l>,
    meta: Meta,
}

impl Deref for Node<'_> {
    type Target = Meta;
    #[inline]
    fn deref(&self) -> &Meta {
        &self.meta
    }
}

impl<'l> Node<'l> {
    #[inline]
    pub(super) fn from_pbf(n: &'l PbfNode, offset: &super::Offset, strings: &'l [String]) -> Self {
        Self {
            id: NodeId(n.id()),
            strings,
            nano_lat: offset.lat + n.lat() * offset.granularity as i64,
            nano_lon: offset.lon + n.lon() * offset.granularity as i64,
            tags: NodeTagFields::Normal(&n.keys, &n.vals),
            meta: Meta::from_info(&n.info),
        }
    }

    #[inline]
    pub(super) fn from_pbf_dense(
        d: DenseState,
        version: u32,
        visible: bool,
        offset: &super::Offset,
        key_values: &'l [i32],
        strings: &'l [String],
    ) -> Self {
        Self {
            id: NodeId(d.id),
            nano_lat: offset.lat + d.lat * offset.granularity as i64,
            nano_lon: offset.lon + d.lon * offset.granularity as i64,
            strings,
            tags: NodeTagFields::Dense(key_values),
            meta: Meta { version, visible },
        }
    }

    /// Latitude in degrees.
    #[inline(always)]
    pub fn lat(&self) -> f64 {
        self.nano_lat as f64 * 1e-9
    }
    /// Longitude in degrees.
    #[inline(always)]
    pub fn lon(&self) -> f64 {
        self.nano_lon as f64 * 1e-9
    }

    pub fn tags(&self) -> Tags<'l> {
        self.tags.iter_with_strings(self.strings)
    }
}
