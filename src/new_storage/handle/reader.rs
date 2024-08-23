//! The reader implementation.

use std::sync::Arc;

use clippy_utilities::Cast;
use parking_lot::{Mutex, RwLock};

use super::super::policy::LruPolicy;
use super::super::{
    format_path, Backend, Block, BlockSlice, CacheKey, MemoryCache, StorageError, StorageResult,
};

/// Reader is a struct responsible for reading blocks of data from a backend
/// storage system, optionally caching these blocks using a `MemoryCache` with
/// LRU eviction policy.
#[derive(Debug)]
pub struct Reader {
    /// The inode number associated with the file being read.
    ino: u64,
    /// The block size
    block_size: usize,
    /// The `MemoryCache`
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
}

impl Reader {
    /// Creates a new `Reader` instance.
    pub fn new(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        Reader {
            ino,
            block_size,
            cache,
            backend,
        }
    }

    /// Try fetch the block from `MemoryCache`.
    fn fetch_block_from_cache(&self, block_id: u64, version: u64) -> Option<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
            // Check current block is invalid or not.
            version,
        };
        let cache = self.cache.lock();
        cache.fetch(&key)
    }

    /// Fetch the block from the backend storage system.
    async fn fetch_block_from_backend(
        &self,
        block_id: u64,
        version: u64,
    ) -> StorageResult<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
            version,
        };
        let content = {
            let mut buf = vec![0; self.block_size];
            self.backend
                .read(&format_path(self.ino, block_id), &mut buf, version)
                .await?;
            buf
        };
        match self.cache.lock().new_block(&key, &content) {
            Some(block) => Ok(block),
            None => Err(StorageError::OutOfMemory),
        }
    }

    /// Reads data from the file starting at the given offset and up to the
    /// given length.
    pub async fn read(
        &self,
        buf: &mut Vec<u8>,
        slices: &[BlockSlice],
        version: u64,
    ) -> StorageResult<usize> {
        for slice in slices {
            let block_id = slice.block_id;
            // Block's pin count is increased by 1.
            let block = match self.fetch_block_from_cache(block_id, version) {
                Some(block) => block,
                None => self.fetch_block_from_backend(block_id, version).await?,
            };
            {
                // Copy the data from the block to the buffer.
                let block = block.read();
                assert!(block.pin_count() >= 1);
                let offset = slice.offset.cast();
                let size: usize = slice.size.cast();
                let end = offset + size;
                let block_size = block.len();
                assert!(
                    block_size >= end,
                    "The size of block should be greater than {end}, but {block_size} found."
                );
                let slice = block
                    .get(offset..end)
                    .unwrap_or_else(|| unreachable!("The block is checked to be big enough."));
                buf.extend_from_slice(slice);
            }
            self.cache.lock().unpin(&CacheKey {
                ino: self.ino,
                block_id,
                version,
            });
        }
        Ok(buf.len())
    }

    /// Close the reader and remove the accessed cache blocks.
    #[allow(clippy::unused_self)]
    pub fn close(&self) {
        // For continuous read/write, we do not need to clean these cache blocks.
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;

    use super::super::writer::Writer;
    use super::*;
    use crate::new_storage::backend::backend_impl::memory_backend;
    use crate::new_storage::block::BLOCK_SIZE;

    #[tokio::test]
    async fn test_reader() {
        let backend = Arc::new(memory_backend().unwrap());
        let manger = Arc::new(Mutex::new(MemoryCache::new(10, BLOCK_SIZE)));
        let content = Bytes::from(vec![b'1'; BLOCK_SIZE]);
        let slice = BlockSlice::new(0, 0, content.len().cast());

        let b = Arc::clone(&backend);
        let writer = Writer::new(1, BLOCK_SIZE, Arc::clone(&manger), b);
        let version = 0;
        writer.write(&content, &[slice], version).await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let reader = Reader::new(1, BLOCK_SIZE, Arc::clone(&manger), backend);
        let slice = BlockSlice::new(0, 0, BLOCK_SIZE.cast());
        let mut buf = Vec::with_capacity(BLOCK_SIZE);
        let version = 0;
        let size = reader.read(&mut buf, &[slice], version).await.unwrap();
        assert_eq!(size, BLOCK_SIZE);
        assert_eq!(content, buf);
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 1);
        reader.close();
        let memory_size = manger.lock().len();
        // assert_eq!(memory_size, 0);
        // Current cache is not cleaned after close for continuous read/write.
        assert_eq!(memory_size, 1);
    }
}
