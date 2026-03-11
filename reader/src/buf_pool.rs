//! A thread-safe pool of reusable decompression buffers.

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

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

    /// Like [`Self::acquire`] but returns an [`OwnedPoolBuf`] that keeps an
    /// [`Arc`] clone of the pool, so the buffer can outlive the borrow of
    /// `self` (e.g. when stored inside a [`RawBlob`](crate::raw_blob::RawBlob)
    /// for deferred parallel decoding).
    pub fn acquire(self: &Arc<Self>) -> PoolBuf {
        let buf = self.queue.pop().unwrap_or_default();
        PoolBuf {
            buf: ManuallyDrop::new(buf),
            pool: Arc::clone(self),
        }
    }

    fn release(&self, mut buf: Vec<u8>) {
        buf.clear();
        self.queue.push(buf);
    }
}

/// An owned byte buffer checked out from a [`BufPool`].
///
/// Like [`PoolBuf`] but holds an [`Arc`] reference to the pool instead of a
/// borrow, allowing it to outlive the original `&BufPool`.  The buffer is
/// cleared and returned to the pool when this value is dropped.
///
/// Obtain one via [`BufPool::acquire_owned`].
pub struct PoolBuf {
    buf: ManuallyDrop<Vec<u8>>,
    pool: Arc<BufPool>,
}

impl PoolBuf {
    #[inline]
    pub fn acquire(&self) -> Self {
        self.pool.acquire()
    }
}

impl Deref for PoolBuf {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Vec<u8> {
        &self.buf
    }
}

impl DerefMut for PoolBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

impl Drop for PoolBuf {
    fn drop(&mut self) {
        // SAFETY: `buf` is only taken here, in `drop`, which runs exactly once.
        let buf = unsafe { ManuallyDrop::take(&mut self.buf) };
        self.pool.release(buf);
    }
}

impl Clone for PoolBuf {
    fn clone(&self) -> Self {
        let mut new_buf = self.pool.acquire();
        new_buf.extend_from_slice(&self.buf);
        new_buf
    }
}

impl std::fmt::Debug for PoolBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedPoolBuf")
            .field("len", &self.buf.len())
            .finish()
    }
}
