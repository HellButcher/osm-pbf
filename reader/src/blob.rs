use byteorder::{BigEndian, ReadBytesExt};
use bytes::Buf;
use osm_pbf_proto::fileformat::blob::Data;
pub use osm_pbf_proto::fileformat::{Blob as PbfBlob, BlobHeader as PbfBlobHeader};
use osm_pbf_proto::osmformat::{HeaderBlock, PrimitiveBlock as PbfPrimitiveBlock};
use osm_pbf_proto::protobuf::{self as pb, CodedInputStream, Message};
use std::fs::File;
use std::io::{self, Read};
use std::iter;
use std::path::Path;

use crate::data::OSMDataBlob;
use crate::error::{Error, Result};

const MAX_HEADER_SIZE: u32 = 64 * 1024;
const MAX_UNCOMPRESSED_DATA_SIZE: usize = 32 * 1024 * 1024;

#[derive(PartialEq, Clone, Debug)]
pub enum Blob<M> {
    Encoded(PbfBlob),
    Decoded(M),
}

impl<M> Blob<M> {
    const INST: Self = Self::Encoded(PbfBlob {
        raw_size: None,
        data: None,
        special_fields: pb::SpecialFields::new(),
    });

    #[inline]
    const fn new(blob: PbfBlob) -> Self {
        Self::Encoded(blob)
    }
}

impl<M> Default for Blob<M> {
    fn default() -> Self {
        Self::Encoded(PbfBlob::new())
    }
}

impl<M: Message> Blob<M> {
    pub fn decode_into(mut self) -> Result<M> {
        self.decode()?;
        let Self::Decoded(d) = self else {
            unreachable!();
        };
        Ok(d)
    }

    pub fn decode(&mut self) -> Result<&mut M> {
        if let Self::Encoded(d) = self {
            let r = match &d.data {
                Some(Data::Raw(r)) => M::parse_from_tokio_bytes(r)?,
                Some(Data::ZlibData(z)) => {
                    let mut decoder = flate2::bufread::ZlibDecoder::new(io::Cursor::new(z));
                    M::parse_from_reader(&mut decoder)?
                }
                Some(Data::LzmaData(z)) => {
                    let mut decoder = xz2::bufread::XzDecoder::new(io::Cursor::new(z));
                    M::parse_from_reader(&mut decoder)?
                }
                None => M::new(),
                _ => {
                    return Err(Error::UnsupportedEncoding);
                }
            };
            *self = Self::Decoded(r);
        }
        let Self::Decoded(d) = self else {
            unreachable!();
        };
        Ok(d)
    }

    pub fn parse_and_decode(is: &mut CodedInputStream<'_>) -> pb::Result<M> {
        let mut data = M::new();
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                10 => {
                    // Raw (1)
                    let len = is.read_raw_varint64()?;
                    let old_limit = is.push_limit(len)?;
                    data.merge_from(is)?;
                    is.pop_limit(old_limit);
                }
                #[cfg(feature = "zlib")]
                26 => {
                    // ZlibData (3)
                    let len = is.read_raw_varint64()?;
                    let old_limit = is.push_limit(len)?;
                    let read: &mut dyn io::BufRead = is;
                    {
                        let mut decoder = flate2::bufread::ZlibDecoder::new(read);
                        let mut is = CodedInputStream::new(&mut decoder);
                        data.merge_from(&mut is)?;
                    }
                    is.pop_limit(old_limit);
                }
                #[cfg(feature = "lzma")]
                34 => {
                    // LzmaData (4)
                    let len = is.read_raw_varint64()?;
                    let old_limit = is.push_limit(len)?;
                    let read: &mut dyn io::BufRead = is;
                    {
                        let mut decoder = xz2::bufread::XzDecoder::new(read);
                        let mut is = CodedInputStream::new(&mut decoder);
                        data.merge_from(&mut is)?;
                    }
                    is.pop_limit(old_limit);
                }
                /*
                42 => { // OBSOLETEzip2Data (5)
                    todo!()
                },
                50 => { // Lz4Data (6)
                    todo!()
                },
                58 => { // ZstdData (
                        // 7)
                    todo!()
                },
                */
                tag => {
                    pb::rt::skip_field_for_tag(tag, is)?;
                }
            };
        }
        data.check_initialized()?;
        Ok(data)
    }
}

#[derive(Debug)]
pub struct Blobs<R> {
    header: HeaderBlock,
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

    pub fn next_blob(&mut self) -> Result<Option<(PbfBlobHeader, PbfBlob)>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        let blob: PbfBlob = self.read_msg_exact(header.datasize() as usize)?;
        Ok(Some((header, blob)))
    }

    fn _read_header_block(&mut self) -> Result<()> {
        let Some(header) = self._read_blob_header()? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        if header.type_() != "OSMHeader" {
            return Err(Error::UnexpectedBlobType(header.type_().to_string()));
        }
        let mut input = self.reader.by_ref().take(header.datasize() as u64);
        let mut input = CodedInputStream::from_buf_read(&mut input);
        self.header = Blob::parse_and_decode(&mut input)?;
        input.check_eof()?;
        Ok(())
    }

    pub fn next_primitive_block(&mut self) -> Result<Option<OSMDataBlob>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        if header.type_() != "OSMData" {
            return Err(Error::UnexpectedBlobType(header.type_().to_string()));
        }
        let blob: PbfBlob = self.read_msg_exact(header.datasize() as usize)?;
        Ok(Some(Blob::Encoded(blob)))
    }

    pub fn next_primitive_block_decoded(&mut self) -> Result<Option<PbfPrimitiveBlock>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        if header.type_() != "OSMData" {
            return Err(Error::UnexpectedBlobType(header.type_().to_string()));
        }
        let mut input = self.reader.by_ref().take(header.datasize() as u64);
        let mut input = CodedInputStream::from_buf_read(&mut input);
        let decoded = Blob::parse_and_decode(&mut input)?;
        input.check_eof()?;
        Ok(Some(decoded))
    }
}

impl<R: io::BufRead + io::Seek> Blobs<R> {
    fn next_blob_with(
        &mut self,
        cond: impl Fn(&PbfBlobHeader) -> bool,
    ) -> Result<Option<(PbfBlobHeader, PbfBlob)>> {
        loop {
            let Some(header) = self._read_blob_header()? else {
                return Ok(None);
            };
            if cond(&header) {
                let blob: PbfBlob = self.read_msg_exact((header.datasize() as u32) as usize)?;
                return Ok(Some((header, blob)));
            }
            self.reader
                .seek(io::SeekFrom::Current((header.datasize() as u32) as i64))?;
        }
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
