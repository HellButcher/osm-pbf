use std::{iter::FusedIterator, ops::Deref};

use osm_pbf_proto::osmformat::Way as PbfWay;

use super::{
    node::NodeId,
    tags::{TagFields, Tags},
    Meta,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WayId(pub i64);

pub struct Way<'l> {
    pub id: WayId,

    strings: &'l [String],

    refs: &'l [i64],

    tags: TagFields<'l>,
    meta: Meta,
}

impl Deref for Way<'_> {
    type Target = Meta;
    #[inline]
    fn deref(&self) -> &Meta {
        &self.meta
    }
}

impl<'l> Way<'l> {
    #[inline]
    pub(crate) fn from_pbf(w: &'l PbfWay, strings: &'l [String]) -> Self {
        Self {
            id: WayId(w.id()),
            strings,
            refs: &w.refs,
            tags: TagFields(&w.keys, &w.vals),
            meta: Meta::from_info(&w.info),
        }
    }

    #[inline(always)]
    pub fn refs(&self) -> Refs<'l> {
        Refs {
            iter: self.refs.iter(),
            current: 0,
        }
    }

    #[inline]
    pub fn tags(&self) -> Tags<'l> {
        self.tags.iter_with_strings(self.strings)
    }
}

pub struct Refs<'l> {
    iter: std::slice::Iter<'l, i64>,
    current: i64,
}

impl<'l> IntoIterator for Way<'l> {
    type Item = NodeId;
    type IntoIter = Refs<'l>;
    #[inline]
    fn into_iter(self) -> Refs<'l> {
        self.refs()
    }
}

impl<'l> IntoIterator for &Way<'l> {
    type Item = NodeId;
    type IntoIter = Refs<'l>;
    #[inline]
    fn into_iter(self) -> Refs<'l> {
        self.refs()
    }
}

impl Iterator for Refs<'_> {
    type Item = NodeId;
    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        self.current += self.iter.next()?;
        Some(NodeId(self.current))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.iter.count()
    }
}

impl FusedIterator for Refs<'_> {}
