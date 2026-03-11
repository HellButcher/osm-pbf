use byteorder::{BigEndian, ReadBytesExt};
use osm_pbf_proto::protobuf::ClearAndParse;
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
    curent_blob_header: PbfBlobHeader,
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

impl<R: io::Seek + io::BufRead> Blobs<R> {
    #[inline]
    pub fn rewind(&mut self) -> Result<()> {
        self.reader.rewind()?;
        self._read_header_block()?;
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
            curent_blob_header: PbfBlobHeader::default(),
            buf: Vec::new(),
            pool,
        };
        r._read_header_block()?;
        Ok(r)
    }

    fn _read_blob_header(&mut self) -> Result<bool> {
        let header_size = match self.reader.read_u32::<BigEndian>() {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(false); // Expected EOF
            }
            Err(e) => return Err(Error::IoError(e)),
            Ok(header_size) if header_size > MAX_HEADER_SIZE => {
                return Err(Error::BlobHeaderToLarge);
            }
            Ok(header_size) => header_size as usize,
        };
        self.buf_read_exact(header_size)?;
        self.curent_blob_header.clear_and_parse(&self.buf)?;
        let data_size = self.curent_blob_header.as_view().datasize() as usize;
        if data_size > MAX_UNCOMPRESSED_DATA_SIZE {
            return Err(Error::BlobDataToLarge);
        }
        Ok(true)
    }

    fn buf_read_exact(&mut self, exact_size: usize) -> Result<()> {
        self.buf.clear();
        self.buf.resize(exact_size, 0);
        self.reader.read_exact(&mut self.buf)?;
        Ok(())
    }

    pub fn next_blob(&mut self) -> Result<Option<(&PbfBlobHeader, RawBlob<'static>)>> {
        if !self._read_blob_header()? {
            return Ok(None);
        };
        let blob = RawBlob::from_reader(
            &mut self.reader,
            self.curent_blob_header.as_view().datasize() as usize,
            &self.pool,
        )?;
        Ok(Some((&self.curent_blob_header, blob)))
    }

    pub fn next_blob_expected(
        &mut self,
        expected_blob_type: &str,
    ) -> Result<Option<(&PbfBlobHeader, RawBlob<'static>)>> {
        if !self._read_blob_header()? {
            return Ok(None);
        };
        let blob_type = self.curent_blob_header.as_view().r#type();
        if blob_type.as_bytes() != expected_blob_type.as_bytes() {
            return Err(Error::UnexpectedBlobType(blob_type.to_string()));
        }
        let blob = RawBlob::from_reader(
            &mut self.reader,
            self.curent_blob_header.as_view().datasize() as usize,
            &self.pool,
        )?;
        Ok(Some((&self.curent_blob_header, blob)))
    }

    fn _read_header_block(&mut self) -> Result<()> {
        let Some((_, blob)) = self.next_blob_expected("OSMHeader")? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        self.header = blob.into_decoded()?;
        Ok(())
    }

    pub fn next_primitive_block(&mut self) -> Result<Option<OSMDataBlob<'static>>> {
        let Some((_, blob)) = self.next_blob_expected("OSMData")? else {
            return Ok(None);
        };
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
    type Item = Result<OSMDataBlob<'static>>;

    #[inline]
    fn next(&mut self) -> Option<Result<OSMDataBlob<'static>>> {
        self.next_primitive_block().transpose()
    }
}

impl<R: io::BufRead> iter::FusedIterator for Blobs<R> {}

#[derive(Debug)]
pub struct BlobsBytes<'a> {
    bytes: &'a [u8],
    offset: usize,
    header: HeaderBlock,
    curent_blob_header: PbfBlobHeader,
    /// Shared pool of decompression buffers.
    pool: Arc<BufPool>,
}

impl<'a> BlobsBytes<'a> {
    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    #[inline]
    pub fn header(&self) -> &HeaderBlock {
        &self.header
    }
    #[inline]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let mut r = Self {
            bytes,
            offset: 0,
            header: HeaderBlock::default(),
            curent_blob_header: PbfBlobHeader::default(),
            pool: Arc::new(BufPool::new()),
        };
        r._read_header_block()?;
        Ok(r)
    }

    #[inline]
    pub fn rewind(&mut self) -> Result<()> {
        self.offset = 0;
        self._read_header_block()?;
        Ok(())
    }

    fn _read_blob_header(&mut self) -> Result<bool> {
        if self.offset >= self.bytes.len() {
            return Ok(false); // Expected EOF
        }
        let mut bytes = &self.bytes[self.offset..];
        let header_size = (&mut bytes).read_u32::<BigEndian>()? as usize;
        self.curent_blob_header
            .clear_and_parse(&bytes[..header_size])?;
        let new_offset = self.offset + 4 + header_size;
        let data_size = self.curent_blob_header.as_view().datasize() as usize;
        if data_size > self.bytes.len() - new_offset {
            return Err(Error::BlobDataToLarge);
        }
        self.offset = new_offset;
        Ok(true)
    }

    pub fn next_blob(&mut self) -> Result<Option<(&PbfBlobHeader, RawBlob<'a>)>> {
        if !self._read_blob_header()? {
            return Ok(None);
        };
        let size = self.curent_blob_header.as_view().datasize() as usize;
        let end = self.offset + size;
        let blob = RawBlob::from_bytes(&self.bytes[self.offset..end], &self.pool)?;
        self.offset = end;
        Ok(Some((&self.curent_blob_header, blob)))
    }

    pub fn next_blob_expected(
        &mut self,
        expected_blob_type: &str,
    ) -> Result<Option<(&PbfBlobHeader, RawBlob<'a>)>> {
        if !self._read_blob_header()? {
            return Ok(None);
        };
        let blob_type = self.curent_blob_header.as_view().r#type();
        if blob_type.as_bytes() != expected_blob_type.as_bytes() {
            return Err(Error::UnexpectedBlobType(blob_type.to_string()));
        }
        let size = self.curent_blob_header.as_view().datasize() as usize;
        let end = self.offset + size;
        let blob = RawBlob::from_bytes(&self.bytes[self.offset..end], &self.pool)?;
        self.offset = end;
        Ok(Some((&self.curent_blob_header, blob)))
    }

    fn _read_header_block(&mut self) -> Result<()> {
        let Some((_, blob)) = self.next_blob_expected("OSMHeader")? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        self.header = blob.into_decoded()?;
        Ok(())
    }

    pub fn next_primitive_block(&mut self) -> Result<Option<OSMDataBlob<'a>>> {
        let Some((_, blob)) = self.next_blob_expected("OSMData")? else {
            return Ok(None);
        };
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
impl<'a> Iterator for BlobsBytes<'a> {
    type Item = Result<OSMDataBlob<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Result<OSMDataBlob<'a>>> {
        self.next_primitive_block().transpose()
    }
}

impl<'a> iter::FusedIterator for BlobsBytes<'a> {}
