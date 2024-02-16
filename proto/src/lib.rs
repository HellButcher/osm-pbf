// re-export of protobuf
pub use protobuf;

include!(concat!(env!("OUT_DIR"), "/protos-gen/mod.rs"));
