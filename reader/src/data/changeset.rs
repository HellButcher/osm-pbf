use osm_pbf_proto::osmformat::ChangeSet as PbfChangeSet;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChangeSetId(pub i64);

pub struct ChangeSet {
    pub id: ChangeSetId,
}
impl ChangeSet {
    #[inline]
    pub(crate) fn from_pbf(n: &PbfChangeSet) -> Self {
        Self {
            id: ChangeSetId(n.id()),
        }
    }
}
