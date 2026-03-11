use byteorder::{BigEndian, ReadBytesExt};
pub use osm_pbf_proto::fileformat::{Blob as PbfBlob, BlobHeader as PbfBlobHeader};
use osm_pbf_proto::osmformat::{HeaderBlock, PrimitiveBlock as PbfPrimitiveBlock};
use osm_pbf_proto::protobuf::{self as pb, CodedInputStream, Message};
use std::fs::File;
use std::io::{self, Read};
use std::iter;
use std::path::Path;
use std::sync::Arc;

use crate::buf_pool::{BufPool, PoolBuf};
use crate::data::OSMDataBlob;
use crate::error::{Error, Result};

const MAX_HEADER_SIZE: u32 = 64 * 1024;
const MAX_UNCOMPRESSED_DATA_SIZE: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub struct RawBlob {
    raw_size: u32,
    data: RawBlobData,
}

#[derive(Clone, Debug, Default)]
enum RawBlobData {
    #[default]
    NotSet,
    Raw(PoolBuf),
    ZlibData(PoolBuf),
    LzmaData(PoolBuf),
    //#[deprecated]
    //Zip2Data(PoolBuf),
    //Lz4Data(PoolBuf),
    //ZstdData(PoolBuf),
}

impl RawBlob {
    fn parse(is: &mut CodedInputStream<'_>, pool: &Arc<BufPool>) -> Result<Self> {
        let mut raw_size = 0;
        let mut data = RawBlobData::NotSet;

        fn read_bytes_into(is: &mut CodedInputStream<'_>, pool: &Arc<BufPool>) -> Result<PoolBuf> {
            let len = is.read_raw_varint64()?;
            let old_limit = is.push_limit(len)?;
            let mut buf = pool.acquire();
            buf.reserve(len as usize);
            is.read_raw_bytes_into(len as u32, &mut buf)?;
            is.pop_limit(old_limit);
            Ok(buf)
        }

        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                16 => {
                    raw_size = is.read_int32()? as u32;
                }
                10 => {
                    // Raw (1)
                    let buf = read_bytes_into(is, pool)?;
                    data = RawBlobData::Raw(buf);
                }
                26 => {
                    // ZlibData (3)
                    let buf = read_bytes_into(is, pool)?;
                    data = RawBlobData::ZlibData(buf);
                }
                34 => {
                    // LzmaData (4)
                    let buf = read_bytes_into(is, pool)?;
                    data = RawBlobData::LzmaData(buf);
                }
                //42 => {
                //    // OBSOLETEzip2Data (5)
                //    let buf = read_bytes_into(is, pool)?;
                //    data = RawBlobData::Zip2Data(buf);
                //}
                //50 => {
                //    // Lz4Data (6)
                //    let buf = read_bytes_into(is, pool)?;
                //    data = RawBlobData::Lz4Data(buf);
                //}
                //58 => {
                //    // ZstdData (7)
                //    let buf = read_bytes_into(is, pool)?;
                //    data = RawBlobData::ZstdData(buf);
                //}
                _ => {
                    pb::rt::skip_field_for_tag(tag, is)?;
                }
            };
        }

        Ok(Self { raw_size, data })
    }

    pub fn decompress(&mut self) -> Result<&[u8]> {
        if let Some(decompressed) = self.decompressed()? {
            self.data = RawBlobData::Raw(decompressed.clone());
            let RawBlobData::Raw(decompressed) = &self.data else {
                unreachable!();
            };
            return Ok(decompressed.as_slice());
        }
        match &self.data {
            RawBlobData::NotSet => Ok(&[]),
            RawBlobData::Raw(buf) => Ok(buf.as_slice()),
            _ => Err(Error::UnsupportedEncoding),
        }
    }

    pub fn into_decompressed(self) -> Result<PoolBuf> {
        if let Some(decompressed) = self.decompressed()? {
            return Ok(decompressed);
        }
        match self.data {
            RawBlobData::Raw(buf) => Ok(buf),
            _ => Err(Error::UnsupportedEncoding),
        }
    }

    pub fn decompressed(&self) -> Result<Option<PoolBuf>> {
        match &self.data {
            RawBlobData::ZlibData(z) => {
                let mut decompressed = z.acquire();
                if self.raw_size > 0 {
                    decompressed.reserve(self.raw_size as usize);
                }
                let src = &mut z.as_slice();
                let mut decoder = flate2::bufread::ZlibDecoder::new(src);
                decoder.read_to_end(&mut decompressed)?;
                Ok(Some(decompressed))
            }
            RawBlobData::LzmaData(z) => {
                let mut decompressed = z.acquire();
                if self.raw_size > 0 {
                    decompressed.reserve(self.raw_size as usize);
                }
                let src = &mut z.as_slice();
                let mut decoder = xz2::bufread::XzDecoder::new(src);
                decoder.read_to_end(&mut decompressed)?;
                Ok(Some(decompressed))
            }
            _ => Ok(None),
        }
    }

    pub fn decoded<M: Message>(&self) -> Result<M> {
        if let Some(decompressed) = self.decompressed()? {
            let mut input = CodedInputStream::from_bytes(decompressed.as_slice());
            let msg = M::parse_from_reader(&mut input)?;
            input.check_eof()?;
            return Ok(msg);
        }
        let bytes = match &self.data {
            RawBlobData::NotSet => &[],
            RawBlobData::Raw(buf) => buf.as_slice(),
            _ => return Err(Error::UnsupportedEncoding),
        };

        let mut input = CodedInputStream::from_bytes(bytes);
        let msg = M::parse_from_reader(&mut input)?;
        input.check_eof()?;
        Ok(msg)
    }
}

#[derive(Clone, Debug)]
pub enum Blob<M> {
    Encoded(RawBlob),
    Decoded(M),
}

impl<M> Default for Blob<M> {
    fn default() -> Self {
        Self::Encoded(RawBlob::default())
    }
}

impl<M: Message> Blob<M> {
    pub fn into_decoded(self) -> Result<M> {
        match self {
            Self::Encoded(d) => d.decoded(),
            Self::Decoded(m) => Ok(m),
        }
    }

    pub fn decode(&mut self) -> Result<&mut M> {
        match self {
            Self::Encoded(d) => {
                let msg = d.decoded()?;
                *self = Self::Decoded(msg);
                let Self::Decoded(m) = self else {
                    unreachable!();
                };
                Ok(m)
            }
            Self::Decoded(m) => Ok(m),
        }
    }
}

#[derive(Debug)]
pub struct Blobs<R> {
    header: HeaderBlock,
    pool: Arc<BufPool>,
    reader: R,
}

impl<R> Blobs<R> {
    #[inline]
    pub fn into_reader(self) -> R {
        self.reader
    }

    #[inline]
    pub fn header(&self) -> &HeaderBlock {
        &self.header
    }
}

impl<R: AsRef<[u8]>> Blobs<io::Cursor<R>> {
    #[inline]
    pub fn from_bytes(bytes: R) -> Result<Self> {
        Self::from_buf_read(io::Cursor::new(bytes))
    }
}

impl<R: Read> Blobs<io::BufReader<R>> {
    #[inline]
    pub fn from_read(read: R) -> Result<Self> {
        Self::from_buf_read(io::BufReader::new(read))
    }
}

impl Blobs<io::BufReader<File>> {
    #[inline]
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        Self::from_read(file)
    }
}

impl<R: io::Seek> Blobs<R> {
    #[inline]
    pub fn rewind(&mut self) -> Result<()> {
        self.reader.rewind()?;
        Ok(())
    }
}

impl<R: io::BufRead> Blobs<R> {
    #[inline]
    pub fn from_buf_read(reader: R) -> Result<Self> {
        let mut r = Self {
            header: HeaderBlock::new(),
            pool: Arc::new(BufPool::new()),
            reader,
        };
        r._read_header_block()?;
        Ok(r)
    }

    fn _read_blob_header(&mut self) -> Result<Option<PbfBlobHeader>> {
        let header_size = match self.reader.read_u32::<BigEndian>() {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None); // Expected EOF
            }
            Err(e) => return Err(Error::IoError(e)),
            Ok(header_size) if header_size > MAX_HEADER_SIZE => {
                return Err(Error::BlobHeaderToLarge);
            }
            Ok(header_size) => header_size as usize,
        };

        let header: PbfBlobHeader = self.read_msg_exact(header_size)?;
        let data_size = header.datasize() as usize;
        if data_size > MAX_UNCOMPRESSED_DATA_SIZE {
            return Err(Error::BlobDataToLarge);
        }
        Ok(Some(header))
    }

    fn read_msg_exact<M: Message>(&mut self, exact_size: usize) -> Result<M> {
        let mut input = self.reader.by_ref().take(exact_size as u64);
        let mut input = CodedInputStream::from_buf_read(&mut input);
        let msg = M::parse_from_reader(&mut input)?;
        input.check_eof()?;
        Ok(msg)
    }

    pub fn next_blob(&mut self) -> Result<Option<(PbfBlobHeader, RawBlob)>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        let mut input = self.reader.by_ref().take(header.datasize() as u64);
        let mut input = CodedInputStream::from_buf_read(&mut input);
        let blob = RawBlob::parse(&mut input, &self.pool)?;
        input.check_eof()?;
        Ok(Some((header, blob)))
    }

    pub fn next_blob_expect(
        &mut self,
        expected_header_type: &str,
    ) -> Result<Option<(PbfBlobHeader, RawBlob)>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        if header.type_() != expected_header_type {
            return Err(Error::UnexpectedBlobType(header.type_().to_string()));
        }
        let mut input = self.reader.by_ref().take(header.datasize() as u64);
        let mut input = CodedInputStream::from_buf_read(&mut input);
        let blob = RawBlob::parse(&mut input, &self.pool)?;
        input.check_eof()?;
        Ok(Some((header, blob)))
    }
    fn _read_header_block(&mut self) -> Result<()> {
        let Some((_, blob)) = self.next_blob_expect("OSMHeader")? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        self.header = blob.decoded()?;
        Ok(())
    }

    pub fn next_primitive_block(&mut self) -> Result<Option<OSMDataBlob>> {
        let Some((_, blob)) = self.next_blob_expect("OSMData")? else {
            return Ok(None);
        };
        Ok(Some(Blob::Encoded(blob)))
    }

    pub fn next_primitive_block_decoded(&mut self) -> Result<Option<PbfPrimitiveBlock>> {
        let Some(blob) = self.next_primitive_block()? else {
            return Ok(None);
        };
        let block = blob.into_decoded()?;
        Ok(Some(block))
    }
}

impl<R: io::BufRead> Iterator for Blobs<R> {
    type Item = Result<OSMDataBlob>;

    #[inline]
    fn next(&mut self) -> Option<Result<OSMDataBlob>> {
        self.next_primitive_block().transpose()
    }
}

impl<R: io::BufRead> iter::FusedIterator for Blobs<R> {}
