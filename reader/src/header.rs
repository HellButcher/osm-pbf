pub use osm_pbf_proto::osmformat::HeaderBlock;

// REQUIRED FEATURES
pub const DENSE_NODES: &str = "DenseNodes";
pub const HISTORICAL_INFORMATION: &str = "HistoricalInformation";

// OPTIONAL FEATURES
pub const HAS_METADATA: &str = "Has_Metadata";
pub const SORT_TYPE_THEN_ID: &str = "Sort.Type_then_ID";
pub const SORT_GEOGRAPHIC: &str = "Sort.Geographic";
pub const LOCATIONS_ON_WAYS: &str = "LocationsOnWays";

pub type OSMHeaderBlob = crate::blob::Blob<HeaderBlock>;
