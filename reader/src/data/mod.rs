use std::string::FromUtf8Error;

use crate::{blob::Block, error::Result};

use osm_pbf_proto::osmformat::{
    Info as PbfInfo, PrimitiveBlock as PbfPrimitiveBlock, PrimitiveGroup as PbfPrimitiveGroup,
};

pub mod changeset;
pub mod node;
pub mod primitive;
pub mod primitive_group;
pub mod relation;
pub mod tags;
pub mod way;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Meta {
    pub version: u32,
    pub visible: bool,
}

impl Meta {
    fn from_info(info: &PbfInfo) -> Self {
        Self {
            version: if info.has_version() {
                info.version() as u32
            } else {
                0
            },
            visible: if info.has_visible() {
                info.visible()
            } else {
                true
            },
        }
    }
}

impl Default for Meta {
    #[inline(always)]
    fn default() -> Self {
        Self {
            version: 0,
            visible: true,
        }
    }
}

#[derive(Copy, Clone)]
struct Offset {
    lat: i64,
    lon: i64,
    granularity: i32,
}

#[derive(Copy, Clone, Default)]
struct DenseState {
    id: i64,
    lat: i64,
    lon: i64,
    kv_pos: usize,
}

#[derive(Clone)]
pub struct PrimitiveBlock {
    strings: Vec<String>,
    primitive_groups: Vec<PbfPrimitiveGroup>,
    offset: Offset,
}

impl Block for PrimitiveBlock {
    type Message = PbfPrimitiveBlock;

    #[inline]
    fn from_message(mut pbf: PbfPrimitiveBlock) -> Result<Self> {
        let strings = if let Some(st) = pbf.stringtable.take() {
            st.s.into_iter()
                .map(|b| String::from_utf8(b.into()))
                .collect::<Result<Vec<String>, FromUtf8Error>>()?
        } else {
            Vec::new()
        };
        Ok(Self {
            strings,
            offset: Offset {
                lat: pbf.lat_offset(),
                lon: pbf.lon_offset(),
                granularity: pbf.granularity(),
            },
            primitive_groups: pbf.primitivegroup,
        })
    }
}

pub type OSMDataBlob = crate::blob::Blob<PrimitiveBlock>;
