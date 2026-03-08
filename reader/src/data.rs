pub use osm_pbf_proto::osmformat::{
    ChangeSet, Info, Node, PrimitiveBlock, PrimitiveGroup, Relation, Way,
};
pub mod primitives {
    pub use osm_pbf_proto::elements::*;
    pub use osm_pbf_proto::osmformat::{ChangeSet, Info, Node, Relation, Way};
}

pub type OSMDataBlob = crate::blob::Blob<PrimitiveBlock>;
