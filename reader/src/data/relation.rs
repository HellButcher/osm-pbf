use std::ops::Deref;

use osm_pbf_proto::{
    osmformat::{relation::MemberType as PbfMemberType, Relation as PbfRelation},
    protobuf::EnumOrUnknown,
};

use super::{
    node::NodeId,
    tags::{TagFields, Tags},
    way::WayId,
    Meta,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationId(pub i64);

pub struct Relation<'l> {
    pub id: RelationId,

    strings: &'l [String],

    roles_sid: &'l [i32],
    memids: &'l [i64],
    types: &'l [EnumOrUnknown<PbfMemberType>],

    tags: TagFields<'l>,
    meta: Meta,
}

impl Deref for Relation<'_> {
    type Target = Meta;
    #[inline]
    fn deref(&self) -> &Meta {
        &self.meta
    }
}

impl<'l> Relation<'l> {
    #[inline]
    pub(crate) fn from_pbf(r: &'l PbfRelation, strings: &'l [String]) -> Self {
        Self {
            id: RelationId(r.id()),
            strings,
            roles_sid: &r.roles_sid,
            memids: &r.memids,
            types: &r.types,
            tags: TagFields(&r.keys, &r.vals),
            meta: Meta::from_info(&r.info),
        }
    }

    pub fn members(&self) -> Members<'l> {
        Members {
            strings: self.strings,
            i: 0,
            roles: self.roles_sid,
            member_ids: self.memids,
            member_types: self.types,
        }
    }

    #[inline]
    pub fn tags(&self) -> Tags<'l> {
        self.tags.iter_with_strings(self.strings)
    }
}

pub enum Member<'l> {
    Node(NodeId, &'l str),
    Way(WayId, &'l str),
    Relation(RelationId, &'l str),
}

#[derive(Clone)]
pub struct Members<'l> {
    strings: &'l [String],
    i: usize,
    roles: &'l [i32],
    member_ids: &'l [i64],
    member_types: &'l [EnumOrUnknown<PbfMemberType>],
}

impl<'l> IntoIterator for Relation<'l> {
    type Item = Member<'l>;
    type IntoIter = Members<'l>;
    #[inline(always)]
    fn into_iter(self) -> Members<'l> {
        self.members()
    }
}

impl<'l> IntoIterator for &Relation<'l> {
    type Item = Member<'l>;
    type IntoIter = Members<'l>;
    #[inline(always)]
    fn into_iter(self) -> Members<'l> {
        self.members()
    }
}

impl<'l> Iterator for Members<'l> {
    type Item = Member<'l>;
    #[inline]
    fn next(&mut self) -> Option<Member<'l>> {
        loop {
            let pos = self.i;
            self.i += 1;
            let role_str_id = *self.roles.get(pos)? as usize;
            let member_id = *self.member_ids.get(pos)?;
            let member_type = match self.member_types.get(pos)?.enum_value() {
                Ok(member_type) => member_type,
                Err(_) => continue,
            };
            let role_str = self
                .strings
                .get(role_str_id)
                .map(Deref::deref)
                .unwrap_or("");
            return Some(match member_type {
                PbfMemberType::NODE => Member::Node(NodeId(member_id), role_str),
                PbfMemberType::WAY => Member::Way(WayId(member_id), role_str),
                PbfMemberType::RELATION => Member::Relation(RelationId(member_id), role_str),
            });
        }
    }
}
