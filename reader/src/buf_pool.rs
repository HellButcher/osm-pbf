//! A thread-safe pool of reusable decompression buffers.

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

use crossbeam_queue::SegQueue;

/// A thread-safe, lock-free pool of reusable byte buffers.
///
/// Backed by a [`SegQueue`], so `acquire` and `release` are wait-free under
/// typical conditions and never block other threads.
///
/// Useful when decoding OSM PBF blobs in parallel: each worker thread
/// checks out a buffer, decompresses a blob into it, then returns it when
/// done — avoiding per-blob heap allocations while keeping the buffer
/// lifetime explicit and bounded.
///
/// # Example
///
/// ```ignore
/// use rayon::prelude::*;
/// use osm_pbf_reader::buf_pool::BufPool;
///
/// let pool = BufPool::new();
/// blobs.par_bridge().for_each(|blob| {
///     let block = blob.unwrap().decode_with_pool(&pool).unwrap();
///     // process block …
/// });
/// ```
#[derive(Debug, Default)]
pub struct BufPool {
    queue: SegQueue<Vec<u8>>,
}

impl BufPool {
    /// Creates an empty pool. Buffers are allocated on demand.
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks out a buffer from the pool (or allocates a fresh one if the
    /// pool is empty). The buffer is returned to the pool when the
    /// [`PoolBuf`] guard is dropped.
    pub fn acquire(&self) -> PoolBuf<'_> {
        let buf = self.queue.pop().unwrap_or_default();
        PoolBuf {
            buf: ManuallyDrop::new(buf),
            pool: self,
        }
    }

    fn release(&self, mut buf: Vec<u8>) {
        buf.clear();
        self.queue.push(buf);
    }
}

/// A byte buffer checked out from a [`BufPool`].
///
/// Derefs to `Vec<u8>`. The buffer is cleared and returned to the pool
/// automatically when this guard is dropped.
pub struct PoolBuf<'pool> {
    buf: ManuallyDrop<Vec<u8>>,
    pool: &'pool BufPool,
}

impl Deref for PoolBuf<'_> {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Vec<u8> {
        &self.buf
    }
}

impl DerefMut for PoolBuf<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

impl Drop for PoolBuf<'_> {
    fn drop(&mut self) {
        // SAFETY: `buf` is only taken here, in `drop`, which runs exactly once.
        let buf = unsafe { ManuallyDrop::take(&mut self.buf) };
        self.pool.release(buf);
    }
}
