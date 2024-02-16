use std::ops::Deref;

pub use osm_pbf_proto::osmformat::HeaderBlock as PbfHeaderBlock;

use crate::{blob::Block, error::Result};

// REQUIRED FEATURES
pub const DENSE_NODES: &str = "DenseNodes";
pub const HISTORICAL_INFORMATION: &str = "HistoricalInformation";

// OPTIONAL FEATURES
pub const HAS_METADATA: &str = "Has_Metadata";
pub const SORT_TYPE_THEN_ID: &str = "Sort.Type_then_ID";
pub const SORT_GEOGRAPHIC: &str = "Sort.Geographic";
pub const LOCATIONS_ON_WAYS: &str = "LocationsOnWays";

pub struct HeaderBlock {
    pbf: PbfHeaderBlock,
}

impl Deref for HeaderBlock {
    type Target = PbfHeaderBlock;
    #[inline]
    fn deref(&self) -> &PbfHeaderBlock {
        &self.pbf
    }
}

impl Block for HeaderBlock {
    type Message = PbfHeaderBlock;

    #[inline]
    fn from_message(pbf: PbfHeaderBlock) -> Result<Self> {
        Ok(Self { pbf })
    }
}

pub type OSMHeaderBlob = crate::blob::Blob<HeaderBlock>;
