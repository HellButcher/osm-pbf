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

use std::fmt::Debug;
use std::io::{self, Read};
use std::ops::Deref;
use std::sync::Arc;

use osm_pbf_proto::protobuf::Message;

use crate::buf_pool::{BufPool, PoolBuf};
use crate::error::{Error, Result};

pub const MAX_UNCOMPRESSED_DATA_SIZE: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
pub enum BlobBuf<'a> {
    PoolBuf(PoolBuf),
    Slice(&'a [u8], Arc<BufPool>),
}

impl BlobBuf<'_> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            BlobBuf::PoolBuf(pool_buf) => pool_buf.as_slice(),
            BlobBuf::Slice(slice, _) => slice,
        }
    }
}

impl AsRef<[u8]> for BlobBuf<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for BlobBuf<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

/// The data payload carried by a [`RawBlob`].
#[derive(Clone, Debug)]
pub enum BlobData<'a> {
    NotSet,
    Raw(BlobBuf<'a>),
    ZlibData(BlobBuf<'a>),
    LzmaData(BlobBuf<'a>),
    ObsoleteBzip2Data(BlobBuf<'a>),
    Lz4Data(BlobBuf<'a>),
    ZstdData(BlobBuf<'a>),
}

/// A parsed OSM PBF `Blob` message, decoded without the protobuf runtime.
///
/// Created by [`RawBlob::from_reader`].  Use [`RawBlobData`] to access the
/// payload bytes, then decompress and parse the inner `PrimitiveBlock` /
/// `HeaderBlock` as usual.
#[derive(Clone, Debug)]
pub struct RawBlob<'a> {
    /// Uncompressed size hint (field 2 in the proto), when the data is
    /// compressed.  Used to pre-size the decompression buffer.
    pub raw_size: Option<i32>,
    /// The data payload (one of the `oneof data` variants).
    pub data: BlobData<'a>,
}

impl<'a> RawBlob<'a> {
    pub const NOT_SET: Self = Self {
        raw_size: None,
        data: BlobData::NotSet,
    };

    /// Parses a `RawBlob` by reading exactly `size` bytes from `reader`.
    ///
    /// Reads protobuf fields one-by-one directly from the stream.  When a
    /// bytes payload field is encountered, a buffer is acquired from `pool`
    /// and the contents are read in a single `read_exact` call — no
    /// intermediate copy.  Unknown fields are skipped.  Any bytes left over
    /// after parsing (which should not occur for well-formed input) are
    /// drained to leave the stream correctly positioned.
    pub(crate) fn from_reader<R: Read>(
        reader: &mut R,
        size: usize,
        pool: &Arc<BufPool>,
    ) -> Result<Self> {
        let mut limited = reader.take(size as u64);
        let mut raw_size: Option<i32> = None;
        let mut data = BlobData::NotSet;

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
                        1 => BlobData::Raw(bytes),
                        3 => BlobData::ZlibData(bytes),
                        4 => BlobData::LzmaData(bytes),
                        5 => BlobData::ObsoleteBzip2Data(bytes),
                        6 => BlobData::Lz4Data(bytes),
                        7 => BlobData::ZstdData(bytes),
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

    pub(crate) fn from_bytes(mut bytes: &'a [u8], pool: &Arc<BufPool>) -> Result<Self> {
        let mut raw_size: Option<i32> = None;
        let mut data = BlobData::NotSet;
        loop {
            let tag = match read_varint(&mut bytes) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::IoError(e)),
            };

            let field_number = tag >> 3;
            let wire_type = tag & 0x7;

            match (field_number, wire_type) {
                // raw_size: optional int32 (field 2, varint)
                (2, 0) => {
                    let v = read_varint_slice(&mut bytes).map_err(Error::IoError)?;
                    raw_size = Some(v as i32);
                }
                // data payload: bytes fields (fields 1,3–7, length-delimited)
                (1 | 3 | 4 | 5 | 6 | 7, 2) => {
                    let len = read_varint_slice(&mut bytes).map_err(Error::IoError)? as usize;
                    if len > bytes.len() {
                        return Err(Error::InvalidBlobFormat);
                    }
                    let bytes_buf = BlobBuf::Slice(&bytes[..len], pool.clone());
                    bytes = &bytes[len..];
                    data = match field_number {
                        1 => BlobData::Raw(bytes_buf),
                        3 => BlobData::ZlibData(bytes_buf),
                        4 => BlobData::LzmaData(bytes_buf),
                        5 => BlobData::ObsoleteBzip2Data(bytes_buf),
                        6 => BlobData::Lz4Data(bytes_buf),
                        7 => BlobData::ZstdData(bytes_buf),
                        _ => unreachable!(),
                    };
                }
                // Unknown varint field — skip the value.
                (_, 0) => {
                    read_varint(&mut bytes).map_err(Error::IoError)?;
                }
                // Unknown length-delimited field — skip the payload.
                (_, 2) => {
                    let len = read_varint_slice(&mut bytes).map_err(Error::IoError)? as usize;
                    if len > bytes.len() {
                        return Err(Error::InvalidBlobFormat);
                    }
                    skip_bytes_slice(&mut bytes, len)?;
                }
                // Fixed 64-bit field.
                (_, 1) => skip_bytes_slice(&mut bytes, 8)?,
                // Fixed 32-bit field.
                (_, 5) => skip_bytes_slice(&mut bytes, 4)?,
                _ => return Err(Error::InvalidBlobFormat),
            }
        }
        Ok(Self { raw_size, data })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self.data {
            BlobData::NotSet => true,
            BlobData::Raw(ref bytes)
            | BlobData::ZlibData(ref bytes)
            | BlobData::LzmaData(ref bytes)
            | BlobData::ObsoleteBzip2Data(ref bytes)
            | BlobData::Lz4Data(ref bytes)
            | BlobData::ZstdData(ref bytes) => bytes.is_empty(),
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self.data {
            BlobData::NotSet => &[],
            BlobData::Raw(ref bytes)
            | BlobData::ZlibData(ref bytes)
            | BlobData::LzmaData(ref bytes)
            | BlobData::ObsoleteBzip2Data(ref bytes)
            | BlobData::Lz4Data(ref bytes)
            | BlobData::ZstdData(ref bytes) => bytes.as_slice(),
        }
    }

    pub fn decompress(&mut self) -> Result<&[u8]> {
        if let Some(decompressed) = self.decompressed()? {
            self.data = BlobData::Raw(decompressed);
            if let BlobData::Raw(ref bytes) = self.data {
                return Ok(bytes.as_slice());
            } else {
                unreachable!();
            }
        }
        match self.data {
            BlobData::NotSet => Ok(&[]),
            BlobData::Raw(ref buf) => Ok(buf.as_slice()),
            _ => Err(Error::UnsupportedEncoding),
        }
    }

    pub fn into_decompressed(mut self) -> Result<BlobBuf<'a>> {
        if let Some(decompressed) = self.decompressed()? {
            return Ok(decompressed);
        }
        match std::mem::replace(&mut self.data, BlobData::NotSet) {
            BlobData::Raw(buf) => Ok(buf),
            _ => Err(Error::UnsupportedEncoding),
        }
    }

    pub fn decompressed(&self) -> Result<Option<BlobBuf<'static>>> {
        fn get_buf_and_reserve(src_buf: &BlobBuf<'_>, raw_size: Option<i32>) -> PoolBuf {
            let mut dest_buf = match src_buf {
                BlobBuf::PoolBuf(b) => b.acquire(),
                BlobBuf::Slice(_, pool) => pool.acquire(),
            };
            if let Some(raw_size) = raw_size {
                let hint = (raw_size as usize).min(MAX_UNCOMPRESSED_DATA_SIZE);
                dest_buf.reserve(hint);
            }
            dest_buf
        }
        match &self.data {
            BlobData::NotSet | BlobData::Raw(_) => Ok(None),
            #[cfg(feature = "zlib")]
            BlobData::ZlibData(src_buf) => {
                let mut dest_buf = get_buf_and_reserve(src_buf, self.raw_size);
                flate2::bufread::ZlibDecoder::new(src_buf.as_slice()).read_to_end(&mut dest_buf)?;
                Ok(Some(BlobBuf::PoolBuf(dest_buf)))
            }
            #[cfg(feature = "lzma")]
            BlobData::LzmaData(src_buf) => {
                let mut dest_buf = get_buf_and_reserve(src_buf, self.raw_size);
                xz2::bufread::XzDecoder::new(src_buf.as_slice()).read_to_end(&mut dest_buf)?;
                Ok(Some(BlobBuf::PoolBuf(dest_buf)))
            }
            _ => Err(Error::UnsupportedEncoding),
        }
    }

    pub fn decode<M: Message>(&mut self) -> Result<M> {
        let bytes = self.decompress()?;
        let msg = M::parse(bytes)?;
        Ok(msg)
    }

    /// Decompresses and parses the inner message `M` from a [`RawBlob`] into `out`.
    ///
    /// `out` is cleared before use. When `raw_size` is present in the blob it is
    /// used to pre-size the buffer, avoiding repeated reallocations during
    /// decompression. The buffer is reused across calls when the caller provides
    /// the same `Vec`.
    pub fn into_decoded<M: Message>(mut self) -> Result<M> {
        let bytes = self.decompress()?;
        let msg = M::parse(bytes)?;
        Ok(msg)
    }

    pub fn decoded<M: Message>(&self) -> Result<M> {
        if let Some(decompressed) = self.decompressed()? {
            let msg = M::parse(decompressed.as_slice())?;
            return Ok(msg);
        }
        let bytes = match self.data {
            BlobData::NotSet => &[],
            BlobData::Raw(ref buf) => buf.as_slice(),
            _ => return Err(Error::UnsupportedEncoding),
        };
        let msg = M::parse(bytes)?;
        Ok(msg)
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

#[inline]
fn read_varint_slice(bytes: &mut &[u8]) -> io::Result<u64> {
    read_varint(bytes)
}

fn read_bytes_pooled<R: Read>(
    reader: &mut R,
    len: usize,
    pool: &Arc<BufPool>,
) -> Result<BlobBuf<'static>> {
    let mut buf = pool.acquire();
    buf.resize(len, 0);
    reader.read_exact(&mut buf).map_err(Error::IoError)?;
    Ok(BlobBuf::PoolBuf(buf))
}

fn skip_bytes<R: Read>(reader: &mut R, len: usize) -> Result<()> {
    io::copy(&mut reader.take(len as u64), &mut io::sink()).map_err(Error::IoError)?;
    Ok(())
}

fn skip_bytes_slice(reader: &mut &[u8], len: usize) -> Result<()> {
    if len > reader.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    *reader = &reader[len..];
    Ok(())
}

#[derive(Clone, Debug)]
pub enum Blob<'a, M> {
    Encoded(RawBlob<'a>),
    Decoded(M),
}

impl<M: Message> Blob<'_, M> {
    #[inline]
    pub fn into_decoded(self) -> Result<M> {
        match self {
            Self::Encoded(r) => r.into_decoded(),
            Self::Decoded(m) => Ok(m),
        }
    }

    pub fn decode(&mut self) -> Result<&mut M> {
        match self {
            Self::Decoded(m) => Ok(m),
            Self::Encoded(blob) => {
                let m = blob.decoded()?;
                *self = Self::Decoded(m);
                if let Self::Decoded(ref mut m) = self {
                    Ok(m)
                } else {
                    unreachable!()
                }
            }
        }
    }
}
