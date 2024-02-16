use byteorder::{BigEndian, ReadBytesExt};
pub use osm_pbf_proto::fileformat::{Blob as PbfBlob, BlobHeader as PbfBlobHeader};
use osm_pbf_proto::protobuf::{CodedInputStream, Message};
use std::io::{self, BufRead, Read};
use std::iter;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::data::OSMDataBlob;
use crate::error::{Error, Result};
use crate::header::OSMHeaderBlob;

const MAX_HEADER_SIZE: u32 = 64 * 1024;
const MAX_UNCOMPRESSED_DATA_SIZE: usize = 32 * 1024 * 1024;

pub struct Blob<M> {
    header: PbfBlobHeader,
    blob: PbfBlob,
    phantom: PhantomData<M>,
}

impl<M> Blob<M> {
    #[inline]
    const fn new(header: PbfBlobHeader, blob: PbfBlob) -> Self {
        Self {
            header,
            blob,
            phantom: PhantomData,
        }
    }
}

pub trait Block: Sized {
    type Message: Message;

    fn from_message(pbf: Self::Message) -> Result<Self>;

    #[inline]
    fn parse_from_reader(reader: &mut dyn Read) -> Result<Self> {
        let mut is = CodedInputStream::new(reader);
        let msg = Self::Message::parse_from(&mut is)?;
        is.check_eof()?;
        let block = Self::from_message(msg)?;
        Ok(block)
    }

    #[inline]
    fn parse_from_buffered_reader(reader: &mut dyn BufRead) -> Result<Self> {
        let mut is = CodedInputStream::from_buf_read(reader);
        let msg = Self::Message::parse_from(&mut is)?;
        is.check_eof()?;
        let block = Self::from_message(msg)?;
        Ok(block)
    }

    #[inline]
    fn parse_from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut is = CodedInputStream::from_bytes(bytes);
        let msg = Self::Message::parse_from(&mut is)?;
        is.check_eof()?;
        let block = Self::from_message(msg)?;
        Ok(block)
    }

    #[inline]
    fn parse_from(is: &mut CodedInputStream<'_>) -> Result<Self> {
        let msg = Self::Message::parse_from(is)?;
        let block = Self::from_message(msg)?;
        Ok(block)
    }
}

impl<M: Block> Blob<M> {
    pub fn decode(&self) -> Result<M> {
        if self.blob.has_raw() {
            Ok(M::parse_from_bytes(self.blob.raw())?)
        } else if cfg!(feature = "zlib") && self.blob.has_zlib_data() {
            let cursor = io::Cursor::new(self.blob.zlib_data());
            let mut decoder = flate2::bufread::ZlibDecoder::new(cursor);
            Ok(M::parse_from_reader(&mut decoder)?)
        } else if cfg!(feature = "lzma") && self.blob.has_lzma_data() {
            let cursor = io::Cursor::new(self.blob.lzma_data());
            let mut decoder = xz2::bufread::XzDecoder::new(cursor);
            Ok(M::parse_from_reader(&mut decoder)?)
        } else {
            Err(Error::UnsupportedEncoding)
        }
    }
}

impl<M> Deref for Blob<M> {
    type Target = PbfBlobHeader;
    #[inline]
    fn deref(&self) -> &PbfBlobHeader {
        &self.header
    }
}

#[derive(Debug)]
pub struct Blobs<R>(R);

impl<R> Blobs<R> {
    #[inline]
    pub fn into_inner(self) -> R {
        self.0
    }
}

impl<R: AsRef<[u8]>> Blobs<io::Cursor<R>> {
    #[inline]
    pub fn from_bytes(bytes: R) -> Self {
        Self(io::Cursor::new(bytes))
    }
}

impl<R: Read> Blobs<io::BufReader<R>> {
    #[inline]
    pub fn from_read(read: R) -> Self {
        Self(io::BufReader::new(read))
    }
}

impl<R: io::Seek> Blobs<R> {
    #[inline]
    pub fn rewind(&mut self) -> io::Result<()> {
        self.0.rewind()?;
        Ok(())
    }
}

impl<R: BufRead> Blobs<R> {
    #[inline]
    pub fn from_buf_read(read: R) -> Self {
        Self(read)
    }

    pub fn header(&mut self) -> Result<OSMHeaderBlob> {
        match self.next_blob()? {
            Some((header, blob)) if header.type_() == "OSMHeader" => {
                Ok(OSMHeaderBlob::new(header, blob))
            }
            Some((header, _)) => Err(Error::UnexpectedBlobType(header.type_().to_string())),
            None => Err(std::io::ErrorKind::UnexpectedEof.into()),
        }
    }

    fn read_msg_exact<M: Message>(&mut self, exact_size: usize) -> Result<M> {
        let mut input = self.0.by_ref().take(exact_size as u64);
        let mut input = CodedInputStream::from_buf_read(&mut input);
        let msg = M::parse_from_reader(&mut input)?;
        input.check_eof()?;
        Ok(msg)
    }

    fn next_blob(&mut self) -> Result<Option<(PbfBlobHeader, PbfBlob)>> {
        let header_size = match self.0.read_u32::<BigEndian>() {
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

        let blob: PbfBlob = self.read_msg_exact(data_size)?;
        Ok(Some((header, blob)))
    }
}

impl<R: BufRead> Iterator for Blobs<R> {
    type Item = Result<OSMDataBlob>;

    fn next(&mut self) -> Option<Result<OSMDataBlob>> {
        loop {
            match self.next_blob() {
                Err(e) => {
                    return Some(Err(e));
                }
                Ok(None) => {
                    return None;
                }
                Ok(Some((header, blob))) if header.type_() == "OSMData" => {
                    return Some(Ok(OSMDataBlob::new(header, blob)));
                }
                // skip unsupported blobs and header-blobs
                _ => {}
            }
        }
    }
}

impl<R: BufRead> iter::FusedIterator for Blobs<R> {}
