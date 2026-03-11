use byteorder::{BigEndian, ReadBytesExt};
use osm_pbf_proto::protobuf::Message;
use osm_pbf_proto::protos::{
    BlobHeader as PbfBlobHeader, HeaderBlock, PrimitiveBlock as PbfPrimitiveBlock,
};
use std::fs::File;
use std::io::{self, Read};
use std::iter;
use std::path::Path;
use std::sync::Arc;

use crate::blob::{Blob, RawBlob, MAX_UNCOMPRESSED_DATA_SIZE};
use crate::buf_pool::BufPool;
use crate::data::OSMDataBlob;
use crate::error::{Error, Result};

const MAX_HEADER_SIZE: u32 = 64 * 1024;

#[derive(Debug)]
pub struct Blobs<R> {
    header: HeaderBlock,
    reader: R,
    /// Reusable scratch buffer for reading raw blob bytes off the wire.
    buf: Vec<u8>,
    /// Shared pool of decompression buffers.
    pool: Arc<BufPool>,
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
        Self::from_buf_read_with_pool(reader, Arc::new(BufPool::new()))
    }

    /// Like [`Self::from_buf_read`] but uses the provided pool for
    /// decompression buffers, allowing callers to share a pool with
    /// their parallel workers.
    #[inline]
    pub(crate) fn from_buf_read_with_pool(reader: R, pool: Arc<BufPool>) -> Result<Self> {
        let mut r = Self {
            header: HeaderBlock::default(),
            reader,
            buf: Vec::new(),
            pool,
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
        self.buf.clear();
        self.buf.resize(exact_size, 0);
        self.reader.read_exact(&mut self.buf)?;
        Ok(M::parse(&self.buf)?)
    }

    pub fn next_blob(&mut self) -> Result<Option<(PbfBlobHeader, RawBlob)>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        let blob = RawBlob::from_reader(
            &mut self.reader,
            header.as_view().datasize() as usize,
            &self.pool,
        )?;
        Ok(Some((header, blob)))
    }

    fn _read_header_block(&mut self) -> Result<()> {
        let Some(header) = self._read_blob_header()? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        let blob_type = header.as_view().r#type();
        if blob_type.as_bytes() != b"OSMHeader" {
            return Err(Error::UnexpectedBlobType(blob_type.to_string()));
        }
        let blob = RawBlob::from_reader(
            &mut self.reader,
            header.as_view().datasize() as usize,
            &self.pool,
        )?;
        self.header = blob.into_decoded()?;
        Ok(())
    }

    pub fn next_primitive_block(&mut self) -> Result<Option<OSMDataBlob>> {
        let Some(header) = self._read_blob_header()? else {
            return Ok(None);
        };
        let blob_type = header.as_view().r#type();
        if blob_type.as_bytes() != b"OSMData" {
            return Err(Error::UnexpectedBlobType(blob_type.to_string()));
        }
        let blob = RawBlob::from_reader(
            &mut self.reader,
            header.as_view().datasize() as usize,
            &self.pool,
        )?;
        Ok(Some(Blob::Encoded(blob)))
    }

    pub fn next_primitive_block_decoded(&mut self) -> Result<Option<PbfPrimitiveBlock>> {
        let Some(blob) = self.next_primitive_block()? else {
            return Ok(None);
        };
        let decoded = blob.into_decoded()?;
        Ok(Some(decoded))
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
