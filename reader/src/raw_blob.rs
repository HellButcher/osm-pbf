//! Zero-copy streaming parser for the OSM PBF `Blob` message.
//!
//! The protobuf-generated `Blob` type uses the UPB arena under the hood, which
//! means parsing it requires first reading all bytes into a scratch buffer and
//! then copying the contained payload (raw/zlib_data/…) into the arena — two
//! copies of potentially 32 MiB of compressed data.
//!
//! [`RawBlob::from_reader`] bypasses the protobuf runtime entirely and reads
//! the wire format field-by-field, placing the bytes payload directly into a
//! buffer acquired from a [`BufPool`].  Only one allocation (at most) and one
//! copy are needed, and the buffer is returned to the pool once the blob is
//! decoded and dropped.

use std::io::{self, Read};
use std::sync::Arc;

use crate::buf_pool::{BufPool, OwnedPoolBuf};
use crate::error::{Error, Result};

/// The data payload carried by a [`RawBlob`].
#[derive(Clone, Debug)]
pub enum RawBlobData {
    NotSet,
    Raw(OwnedPoolBuf),
    ZlibData(OwnedPoolBuf),
    LzmaData(OwnedPoolBuf),
    ObsoleteBzip2Data(OwnedPoolBuf),
    Lz4Data(OwnedPoolBuf),
    ZstdData(OwnedPoolBuf),
}

/// A parsed OSM PBF `Blob` message, decoded without the protobuf runtime.
///
/// Created by [`RawBlob::from_reader`].  Use [`RawBlobData`] to access the
/// payload bytes, then decompress and parse the inner `PrimitiveBlock` /
/// `HeaderBlock` as usual.
#[derive(Clone, Debug)]
pub struct RawBlob {
    /// Uncompressed size hint (field 2 in the proto), when the data is
    /// compressed.  Used to pre-size the decompression buffer.
    pub raw_size: Option<i32>,
    /// The data payload (one of the `oneof data` variants).
    pub data: RawBlobData,
}

impl RawBlob {
    /// Parses a `RawBlob` by reading exactly `size` bytes from `reader`.
    ///
    /// Reads protobuf fields one-by-one directly from the stream.  When a
    /// bytes payload field is encountered, a buffer is acquired from `pool`
    /// and the contents are read in a single `read_exact` call — no
    /// intermediate copy.  Unknown fields are skipped.  Any bytes left over
    /// after parsing (which should not occur for well-formed input) are
    /// drained to leave the stream correctly positioned.
    pub fn from_reader<R: Read>(reader: &mut R, size: usize, pool: &Arc<BufPool>) -> Result<Self> {
        let mut limited = reader.take(size as u64);
        let mut raw_size: Option<i32> = None;
        let mut data = RawBlobData::NotSet;

        loop {
            let tag = match read_varint(&mut limited) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::IoError(e)),
            };

            let field_number = tag >> 3;
            let wire_type = tag & 0x7;

            match (field_number, wire_type) {
                // raw_size: optional int32 (field 2, varint)
                (2, 0) => {
                    let v = read_varint(&mut limited).map_err(Error::IoError)?;
                    raw_size = Some(v as i32);
                }
                // data payload: bytes fields (fields 1,3–7, length-delimited)
                (1 | 3 | 4 | 5 | 6 | 7, 2) => {
                    let len = read_varint(&mut limited).map_err(Error::IoError)? as usize;
                    let bytes = read_bytes_pooled(&mut limited, len, pool)?;
                    data = match field_number {
                        1 => RawBlobData::Raw(bytes),
                        3 => RawBlobData::ZlibData(bytes),
                        4 => RawBlobData::LzmaData(bytes),
                        5 => RawBlobData::ObsoleteBzip2Data(bytes),
                        6 => RawBlobData::Lz4Data(bytes),
                        7 => RawBlobData::ZstdData(bytes),
                        _ => unreachable!(),
                    };
                }
                // Unknown varint field — skip the value.
                (_, 0) => {
                    read_varint(&mut limited).map_err(Error::IoError)?;
                }
                // Unknown length-delimited field — skip the payload.
                (_, 2) => {
                    let len = read_varint(&mut limited).map_err(Error::IoError)? as usize;
                    skip_bytes(&mut limited, len)?;
                }
                // Fixed 64-bit field.
                (_, 1) => skip_bytes(&mut limited, 8)?,
                // Fixed 32-bit field.
                (_, 5) => skip_bytes(&mut limited, 4)?,
                _ => return Err(Error::InvalidBlobFormat),
            }
        }

        // Drain any remaining bytes to keep the stream correctly positioned.
        io::copy(&mut limited, &mut io::sink()).map_err(Error::IoError)?;

        Ok(Self { raw_size, data })
    }
}

/// Reads a protobuf varint from `reader`.
///
/// Returns `Err(UnexpectedEof)` when the reader is empty at the start of the
/// first byte (signals end-of-message to the caller).
fn read_varint<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        let b = byte[0];
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}

fn read_bytes_pooled<R: Read>(
    reader: &mut R,
    len: usize,
    pool: &Arc<BufPool>,
) -> Result<OwnedPoolBuf> {
    let mut buf = pool.acquire_owned();
    buf.resize(len, 0);
    reader.read_exact(&mut buf).map_err(Error::IoError)?;
    Ok(buf)
}

fn skip_bytes<R: Read>(reader: &mut R, len: usize) -> Result<()> {
    io::copy(&mut reader.take(len as u64), &mut io::sink()).map_err(Error::IoError)?;
    Ok(())
}
