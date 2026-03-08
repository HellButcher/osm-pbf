use byteorder::{BigEndian, ReadBytesExt};
use osm_pbf_proto::protos::{
    blob::DataOneof, Blob as PbfBlob, BlobHeader as PbfBlobHeader, HeaderBlock,
    PrimitiveBlock as PbfPrimitiveBlock,
};
use osm_pbf_proto::protobuf::Message;
use std::fs::File;
use std::io::{self, Read};
use std::iter;
use std::path::Path;

use crate::data::OSMDataBlob;
use crate::error::{Error, Result};

const MAX_HEADER_SIZE: u32 = 64 * 1024;
const MAX_UNCOMPRESSED_DATA_SIZE: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
pub enum Blob<M> {
    Encoded(PbfBlob),
    Decoded(M),
}

impl<M> Blob<M> {}

impl<M> Default for Blob<M> {
    fn default() -> Self {
        Self::Encoded(PbfBlob::default())
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
        if let Self::Encoded(blob) = self {
            let m = decode_blob(blob)?;
            *self = Self::Decoded(m);
        }
        let Self::Decoded(d) = self else {
            unreachable!();
        };
        Ok(d)
    }
}

/// Decompresses and parses the inner message `M` from a `PbfBlob`.
fn decode_blob<M: Message>(blob: &PbfBlob) -> Result<M> {
    match blob.as_view().data() {
        DataOneof::Raw(bytes) => Ok(M::parse(bytes)?),
        #[cfg(feature = "zlib")]
        DataOneof::ZlibData(bytes) => {
            let mut out = Vec::new();
            flate2::bufread::ZlibDecoder::new(bytes).read_to_end(&mut out)?;
            Ok(M::parse(&out)?)
        }
        #[cfg(feature = "lzma")]
        DataOneof::LzmaData(bytes) => {
            let mut out = Vec::new();
            xz2::bufread::XzDecoder::new(bytes).read_to_end(&mut out)?;
            Ok(M::parse(&out)?)
        }
        DataOneof::not_set(_) => Ok(M::default()),
        _ => Err(Error::UnsupportedEncoding),
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
            header: HeaderBlock::default(),
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
        let data_size = header.as_view().datasize() as usize;
        if data_size > MAX_UNCOMPRESSED_DATA_SIZE {
            return Err(Error::BlobDataToLarge);
        }
        Ok(Some(header))
    }

    fn read_msg_exact<M: Message>(&mut self, exact_size: usize) -> Result<M> {
        let mut buf = vec![0u8; exact_size];
        self.reader.read_exact(&mut buf)?;
        Ok(M::parse(&buf)?)
    }

    pub fn next_blob(&mut self) -> Result<Option<(PbfBlobHeader, PbfBlob)>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        let blob: PbfBlob = self.read_msg_exact(header.as_view().datasize() as usize)?;
        Ok(Some((header, blob)))
    }

    fn _read_header_block(&mut self) -> Result<()> {
        let Some(header) = self._read_blob_header()? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        if header.as_view().r#type().as_bytes() != b"OSMHeader" {
            return Err(Error::UnexpectedBlobType(
                header.as_view().r#type().to_string(),
            ));
        }
        let blob: PbfBlob = self.read_msg_exact(header.as_view().datasize() as usize)?;
        self.header = decode_blob(&blob)?;
        Ok(())
    }

    pub fn next_primitive_block(&mut self) -> Result<Option<OSMDataBlob>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        if header.as_view().r#type().as_bytes() != b"OSMData" {
            return Err(Error::UnexpectedBlobType(
                header.as_view().r#type().to_string(),
            ));
        }
        let blob: PbfBlob = self.read_msg_exact(header.as_view().datasize() as usize)?;
        Ok(Some(Blob::Encoded(blob)))
    }

    pub fn next_primitive_block_decoded(&mut self) -> Result<Option<PbfPrimitiveBlock>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        if header.as_view().r#type().as_bytes() != b"OSMData" {
            return Err(Error::UnexpectedBlobType(
                header.as_view().r#type().to_string(),
            ));
        }
        let blob: PbfBlob = self.read_msg_exact(header.as_view().datasize() as usize)?;
        Ok(Some(decode_blob(&blob)?))
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
