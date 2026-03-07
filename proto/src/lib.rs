// re-export of protobuf
pub use protobuf;

#[allow(clippy::all)]
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}

pub mod elements;
