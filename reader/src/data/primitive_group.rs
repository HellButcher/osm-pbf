use std::iter::FusedIterator;

use osm_pbf_proto::osmformat::PrimitiveGroup as PbfPrimitiveGroup;

use super::PrimitiveBlock;

#[non_exhaustive]
pub struct PrimitiveGroup<'l> {
    pub(super) block: &'l PrimitiveBlock,
    pub(super) group: &'l PbfPrimitiveGroup,
}

pub struct PrimitiveGroups<'l> {
    block: &'l PrimitiveBlock,
    pos: usize,
}

impl PrimitiveBlock {
    #[inline(always)]
    pub fn iter(&self) -> PrimitiveGroups<'_> {
        self.primitive_groups()
    }
    pub fn primitive_groups(&self) -> PrimitiveGroups<'_> {
        PrimitiveGroups {
            block: self,
            pos: 0,
        }
    }
    pub fn get_primitive_group(&self, index: usize) -> Option<PrimitiveGroup<'_>> {
        let group = self.primitive_groups.get(index)?;
        Some(PrimitiveGroup { block: self, group })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.primitive_groups.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.primitive_groups.is_empty()
    }
}

impl<'l> Iterator for PrimitiveGroups<'l> {
    type Item = PrimitiveGroup<'l>;
    fn next(&mut self) -> Option<Self::Item> {
        let group = self.block.get_primitive_group(self.pos)?;
        self.pos += 1;
        Some(group)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.block.len() - self.pos;
        (len, Some(len))
    }

    #[inline]
    fn count(self) -> usize {
        self.block.len() - self.pos
    }

    #[inline]
    fn last(self) -> Option<Self::Item> {
        let len = self.block.primitive_groups.len();
        if len > 0 {
            self.block.get_primitive_group(len - 1)
        } else {
            None
        }
    }
    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.pos += n;
        self.next()
    }
}

impl FusedIterator for PrimitiveGroups<'_> {}

impl<'l> IntoIterator for &'l PrimitiveBlock {
    type Item = PrimitiveGroup<'l>;
    type IntoIter = PrimitiveGroups<'l>;

    #[inline(always)]
    fn into_iter(self) -> PrimitiveGroups<'l> {
        self.primitive_groups()
    }
}
