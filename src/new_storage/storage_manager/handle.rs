//! The file handle implementation

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::new_storage::{format_path, Block, BlockSlice, StorageError};
use bytes::Bytes;
use clippy_utilities::Cast;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use super::super::backend::Backend;
use super::super::block_slice::offset_to_slice;
use super::super::error::StorageResult;
use super::super::policy::LruPolicy;
use super::super::{CacheKey, MemoryCache};
use crate::async_fuse::memfs::{FileAttr, MetaData};

/// The `DirtyFileAttr` struct represents the dirty file attributes.
#[derive(Copy, Clone, Debug)]
struct DirtyFileAttr {
    /// The file attributes, sync with the write back work operation file size in the memory.
    pub attr: FileAttr,
    /// The dirty file size, sync with the write operation file size in the memory.
    pub dirty_filesize: Option<u64>,
}

/// The `FileHandleInner` struct represents the inner state of a file handle.
/// It contains the file handle, reader, and writer.
pub struct FileHandleInner {
    /// The inode number associated with the file being read.
    ino: u64,
    /// Integrate openfiles in current strutcture, representing an open file with its attributes and open count.
    /// The `attr` field contains the file attributes, while `open_cnt` keeps track
    /// of the number of times this file is currently opened.
    /// The number of times this file is currently opened.
    open_cnt: AtomicU32,
    /// Block count of unflushed blocks, used to determine current file handle is clean.
    dirty_count: AtomicU32,
    /// Current file attributes with dirty file size.
    attr: Arc<tokio::sync::RwLock<DirtyFileAttr>>,
    /// The block size
    block_size: usize,
    /// The `MemoryCache`
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// The access keys.
    access_keys: Mutex<Vec<CacheKey>>,
    /// The sender to send tasks to the write back worker.
    write_back_sender: Sender<Task>,
    /// The sender to send close signal to the write back worker.
    close_sender: Sender<()>,
}

impl std::fmt::Debug for FileHandleInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHandleInner")
            .field("ino", &self.ino)
            .field("block_size", &self.block_size)
            .field("cache", &self.cache)
            .field("backend", &self.backend)
            .field("access_keys", &self.access_keys)
            .finish_non_exhaustive()
    }
}

/// The `MetaTask` enum represents the different types of meta tasks that the
/// write back worker can perform.
#[derive(Debug)]
enum MetaTask {
    /// A pending write task.
    Pending(Arc<MetaCommitTask>),
    /// A flush task, which means we need to flush the meta to the meta data server.
    Flush(oneshot::Sender<Option<StorageError>>),
}

/// The `MetaCommitTask` struct represents a meta commit task.
#[derive(Debug)]
struct MetaCommitTask {
    /// The inode number associated with the file being written.
    ino: u64,
}

/// The `Task` enum represents the different types of tasks that the write back
/// worker can perform.
#[derive(Debug)]
pub enum Task {
    /// A pending write task.
    Pending(Arc<WriteTask>),
    /// A flush task.
    Flush(oneshot::Sender<Option<StorageError>>),
    /// A finish task.
    Finish(oneshot::Sender<Option<StorageError>>),
}

/// The `WriteTask` struct represents a write task
#[derive(Debug)]
pub struct WriteTask {
    /// The cache manager.
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// The block id.
    block_id: u64,
    /// The block to be written.
    block: Arc<RwLock<Block>>,
    /// The file size in this operation.
    file_size: u64,
}

impl FileHandleInner {
    /// Creates a new `FileHandleInner` instance.
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        write_back_sender: Sender<Task>,
        close_sender: Sender<()>,
    ) -> Self {
        FileHandleInner {
            ino,
            block_size,
            cache,
            backend,
            // The open count is initialized to 0, open() method will increase this flag.
            open_cnt: AtomicU32::new(1),
            dirty_count: AtomicU32::new(0),
            // init the file attributes
            attr: Arc::new(tokio::sync::RwLock::new(DirtyFileAttr {
                attr: FileAttr::default(),
                dirty_filesize: None,
            })),
            access_keys: Mutex::new(Vec::new()),
            write_back_sender,
            close_sender,
        }
    }

    /// Get the open file attributes.
    #[inline]
    pub async fn getattr(&self) -> FileAttr {
        let attr = self.attr.read().await;
        debug!("Get attr for ino: {} attr: {:?}", self.ino, *attr);
        let mut dirty_attr = attr.attr;
        if let Some(dirty_filesize) = attr.dirty_filesize {
            dirty_attr.size = dirty_filesize;
        }

        dirty_attr
    }

    /// Set the open file attributes.
    #[inline]
    pub async fn setattr(&self, attr: FileAttr) {
        debug!("Set attr for ino: {} attr: {:?}", self.ino, attr);
        let mut old_attr = self.attr.write().await;

        // If the size of the file is changed, set the dirty file size.
        if old_attr.attr.size != attr.size {
            old_attr.dirty_filesize = Some(attr.size);
        }

        // Do not change old attr size, it will be changed by the write operation.
        let old_attr_size = old_attr.attr.size;
        old_attr.attr = attr;
        old_attr.attr.size = old_attr_size;
    }

    /// Set dirty file size.
    #[inline]
    pub async fn set_dirty_filesize(&self, size: u64) {
        let mut attr = self.attr.write().await;
        attr.dirty_filesize = Some(size);
    }

    /// Fetch the block from the cache manager.
    #[inline]
    pub async fn fetch_block(&self, block_id: u64) -> StorageResult<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };

        // Fetch the block from the cache manager.
        {
            let cache = self.cache.lock();
            if let Some(block) = cache.fetch(&key) {
                return Ok(block);
            }
        }

        // Fetch the block from the backend storage system.
        let path = format_path(self.ino, block_id);
        // There is a gap between the block is created and the content is read from the
        // backend. But according to the current design, concurrency
        // read/write is not supported.
        let mut buf = vec![0; self.block_size];
        self.backend.read(&path, &mut buf).await?;
        let block = {
            let mut cache = self.cache.lock();
            cache
                .new_block(&key, &buf)
                .ok_or(StorageError::OutOfMemory)?
        };

        Ok(block)
    }

    /// Reads data from the file starting at the given offset and up to the
    /// given length.
    pub async fn read(&self, buf: &mut Vec<u8>, slices: &[BlockSlice]) -> StorageResult<usize> {
        for slice in slices {
            let block_id = slice.block_id;
            // Block's pin count is increased by 1.
            let block = self.fetch_block(block_id).await?;
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
            });
        }
        Ok(buf.len())
    }

    /// Writes data to the file starting at the given offset.
    #[inline]
    pub async fn write(&self, buf: &[u8], slices: &[BlockSlice], size: u64) -> StorageResult<()> {
        let mut consume_index = 0;
        for slice in slices {
            let block_id = slice.block_id;
            let end = consume_index + slice.size.cast::<usize>();
            let len = buf.len();
            assert!(
                len >= end,
                "The `buf` should be longer than {end} bytes, but {len} found."
            );
            let write_content = buf
                .get(consume_index..end)
                .unwrap_or_else(|| unreachable!("The `buf` is checked to be long enough."));
            let block = self.fetch_block(block_id).await?;
            {
                let mut block = block.write();
                block.set_dirty(true);
                let start = slice.offset.cast();
                let end = start + slice.size.cast::<usize>();
                let block_size = block.len();
                assert!(
                    block_size >= end,
                    "The size of block should be greater than {end}, but {block_size} found."
                );
                block
                    .get_mut(start..end)
                    .unwrap_or_else(|| unreachable!("The block is checked to be big enough."))
                    .copy_from_slice(write_content);
                consume_index += slice.size.cast::<usize>();
                block.inc_version();
            }
            let task = Arc::new(WriteTask {
                cache: Arc::clone(&self.cache),
                backend: Arc::clone(&self.backend),
                block_id,
                block,
                file_size: size,
            });
            self.write_back_sender
                .send(Task::Pending(task))
                .await
                .unwrap_or_else(|_| {
                    panic!("Should not send command to write back task when the task quits.");
                });
            self.dirty_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.set_dirty_filesize(size);
        }

        Ok(())
    }

    /// Flushes any pending writes to the file.
    #[inline]
    pub async fn flush(&self) -> StorageResult<()> {
        let (tx, rx) = oneshot::channel();

        self.write_back_sender
            .send(Task::Flush(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to write back task when the task quits.");
            });

        rx.await
            .unwrap_or_else(|_| panic!("The sender should not be closed."))
            .map_or(Ok(()), Err)
    }

    /// Extends the file from the old size to the new size.
    /// It is only called by the truncate method in the storage system.
    #[inline]
    pub async fn extend(&self, old_size: u64, new_size: u64) -> StorageResult<()> {
        let slices = offset_to_slice(self.block_size.cast(), old_size, new_size - old_size);
        for slice in slices {
            let buf = vec![0_u8; slice.size.cast()];
            self.write(&buf, &[slice], new_size).await?;
        }

        Ok(())
    }

    /// Increase the open count of the file handle.
    pub fn open(&self) {
        self.open_cnt
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get the open count of the file handle.
    #[must_use]
    pub fn get_open_count(&self) -> u32 {
        self.open_cnt.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Closes the writer associated with the file handle.
    /// If the open count is greater than 0, it will return false and do not remove this handle.
    /// Otherwise, it will return true and remove this handle.
    pub async fn close(&self) -> StorageResult<bool> {
        // Decrease the open count of the file handle, fetch sub will return the previous value.
        if self
            .open_cnt
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            > 1
        {
            debug!("The file handle is still open by other processes.");
            return Ok(false);
        }

        debug!("The file handle is closed, remove the file handle.");

        // Check dirty count to determine whether the file handle is clean.
        self.flush().await?;

        // Will be closed by the send method, or the sender is dropped.
        self.close_sender.send(()).await.unwrap_or_else(|_| {
            panic!("Should not send command to write back task when the task quits.");
        });

        Ok(true)
    }

    /// Write a block back to the backend, return current file size by write handle
    async fn write_back_block(self: Arc<Self>, task: Arc<WriteTask>) -> StorageResult<u64> {
        let path = format_path(self.ino, task.block_id);
        loop {
            let (content, version) = {
                let block = task.block.read();
                if !block.dirty() {
                    // The block has been flushed previously, skip
                    return Ok(task.file_size);
                }
                let content = Bytes::copy_from_slice(block.as_ref());
                let version = block.version();
                (content, version)
            };

            task.backend.write(&path, &content).await?;
            {
                let mut block = task.block.write();
                // Check version
                if block.version() != version {
                    warn!(
                        "Version mismatch previous: {}, current: {}",
                        version,
                        block.version()
                    );
                    continue;
                }
                block.set_dirty(false);
            }
            {
                task.cache.lock().unpin(&CacheKey {
                    ino: self.ino,
                    block_id: task.block_id,
                });
                break;
            }
        }

        Ok(task.file_size)
    }

    /// Write the blocks to the backend storage system concurrently.
    async fn write_blocks<M: MetaData + Send + Sync + 'static>(
        self: Arc<Self>,
        tasks: &Vec<Arc<WriteTask>>,
        metadata_client: Arc<M>,
    ) -> Option<StorageError> {
        let mut handles = Vec::new();
        let mut result = None;
        let mut done_file_size = None;
        for task in tasks {
            let arc_self = Arc::clone(&self);
            let handle = tokio::spawn(arc_self.write_back_block(Arc::clone(task)));
            handles.push(handle);
        }
        // Make sure current blocks is finished.
        for handle in handles {
            match handle.await {
                Err(e) => {
                    result = Some(StorageError::Internal(e.into()));
                }
                Ok(Err(e)) => {
                    result = Some(e);
                }
                Ok(Ok(file_size)) => {
                    self.dirty_count
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    done_file_size = Some(file_size);
                }
            }
        }

        // Write back the file size to the meta data server.
        if let Some(file_size) = done_file_size {
            // Commit current metadata to the meta data server.
            let ino = self.ino;
            info!(
                "Commit meta data for ino: {} with attr size: {:?}",
                ino, file_size
            );
            if let Err(e) = metadata_client
                .write_remote_size_helper(ino, file_size)
                .await
            {
                error!("Failed to commit meta data, the error is {e}.");
            }

            let mut attr_write = self.attr.write().await;
            if let Some(dirty_filesize) = attr_write.dirty_filesize {
                if dirty_filesize == file_size {
                    // If the dirty file size is equal to the file size, set the dirty file size to None.
                    attr_write.dirty_filesize = None;
                }
            }

            return result;
        }

        let mut attr_write = self.attr.write().await;
        if let Some(dirty_filesize) = attr_write.dirty_filesize {
            // Commit current metadata to the meta data server.
            let ino = self.ino;
            info!(
                "Commit meta data for ino: {} with attr size: {:?}",
                ino, dirty_filesize
            );
            if let Err(e) = metadata_client
                .write_remote_size_helper(ino, dirty_filesize)
                .await
            {
                error!("Failed to commit meta data, the error is {e}.");
            }

            // Clean up size
            attr_write.dirty_filesize = None;
        }

        result
    }

    /// The `write_back_work` function represents the write back worker.
    #[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select!`
    async fn write_back_work<M: MetaData + Send + Sync + 'static>(
        self: Arc<Self>,
        mut write_back_receiver: Receiver<Task>,
        mut close_receiver: Receiver<()>,
        metadata_client: Arc<M>,
    ) {
        //  Create a timer to flush the cache every 200ms.
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
        let mut tasks = Vec::new();
        let mut result = None;
        loop {
            tokio::select! {
                Some(()) = close_receiver.recv() => {
                    info!("The {:?} write back task is closed.", self.ino);
                    return;
                }
                Some(task) = write_back_receiver.recv() => {
                    match task {
                        Task::Pending(task) => {
                            info!("Get pending Write back the block. ino: {:?}", self.ino);
                            tasks.push(task);
                            if tasks.len() >= 10 {
                                let slf_clone = Arc::clone(&self);
                                let res = slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                                if let Some(e) = res {
                                    result.get_or_insert(e);
                                }
                                tasks.clear();
                            }
                        }
                        Task::Flush(tx) => {
                            info!("Flush the cache. ino: {:?}", self.ino);
                            let slf_clone = Arc::clone(&self);
                            let res = slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                            tasks.clear();
                            let res = result.take().or(res);
                            if let Err(Some(e)) = tx.send(res) {
                                error!("Failed to send storage error back to `Writer`, the error is {e}.");
                            }
                        }
                        Task::Finish(tx) => {
                            info!("Finish the write back task. ino: {:?}", self.ino);
                            let slf_clone = Arc::clone(&self);
                            let res = slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                            tasks.clear();
                            let res = result.take().or(res);
                            if let Err(Some(e)) = tx.send(res) {
                                error!("Failed to send storage error back to `Writer`, the error is {e}.");
                            }

                            // Check last write task size with mem attr
                            return;
                        }
                    }
                }
                _ = interval.tick() => {
                    let slf_clone = Arc::clone(&self);
                    slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                    tasks.clear();
                }
            }
        }
    }
}

/// The `FileHandle` struct represents a handle to an open file.
/// It contains an `Arc` of `RwLock<FileHandleInner>`.
/// Outer `Handles` have a lock to protect the file handle.
#[derive(Debug, Clone)]
pub struct FileHandle {
    /// The file handle(inode).
    fh: u64,
    /// The block size in bytes
    block_size: usize,
    /// The inner file handle
    inner: Arc<FileHandleInner>,
}

impl FileHandle {
    /// Creates a new `FileHandle` instance.
    pub fn new<M: MetaData + Send + Sync + 'static>(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        metadata_client: Arc<M>,
    ) -> Self {
        let (write_back_tx, write_back_rx) = tokio::sync::mpsc::channel(100);
        // Maybe oneshot is better than mpsc, because we only need to send a signal, but it needs to be mutable.
        let (close_tx, close_rx) = tokio::sync::mpsc::channel::<()>(1);
        let inner = Arc::new(FileHandleInner::new(
            ino,
            block_size,
            cache,
            backend,
            write_back_tx,
            close_tx,
        ));
        let inner_clone = Arc::clone(&inner);
        // TODO: Move handle to task manager
        tokio::spawn(inner_clone.write_back_work(write_back_rx, close_rx, metadata_client));

        FileHandle {
            fh: ino,
            block_size,
            inner,
        }
    }

    /// Returns the file handle.
    #[must_use]
    pub fn fh(&self) -> u64 {
        self.fh
    }

    /// Reads data from the file starting at the given offset and up to the
    /// given length.
    pub async fn read(&self, offset: u64, len: u64) -> StorageResult<Vec<u8>> {
        let slices = offset_to_slice(self.block_size.cast(), offset, len);
        let mut buf = Vec::with_capacity(len.cast());
        self.inner.read(&mut buf, &slices).await?;
        Ok(buf)
    }

    /// Writes data to the file starting at the given offset.
    pub async fn write(&self, offset: u64, buf: &[u8], size: u64) -> StorageResult<()> {
        let slices: smallvec::SmallVec<[BlockSlice; 2]> =
            offset_to_slice(self.block_size.cast(), offset, buf.len().cast());
        self.inner.write(buf, &slices, size).await
    }

    /// Extends the file from the old size to the new size.
    pub async fn extend(&self, old_size: u64, new_size: u64) -> StorageResult<()> {
        self.inner.extend(old_size, new_size).await
    }

    /// Flushes any pending writes to the file.
    ///
    /// Flush and fsync do not need to check how many times a file handle has been opened.
    pub async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    /// Check current file handle is dirty or not.
    ///
    /// If the file handle is dirty, return true, otherwise return false.
    pub fn is_dirty(&self) -> StorageResult<bool> {
        Ok(self
            .inner
            .dirty_count
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0)
    }

    /// Increase the open count of the file handle.
    pub fn open(&self) {
        debug!("try to get open filehandle lock ok {:?}", self.fh);
        self.inner.open();
    }

    /// Get the open count of the file handle.
    /// Be careful, do not split the `open_cnt` with open and close operation.
    #[must_use]
    pub fn get_open_count(&self) -> u32 {
        debug!("try to get open cnt filehandle lock ok {:?}", self.fh);
        self.inner.get_open_count()
    }

    /// Closes the file handle, closing both the reader and writer.
    pub async fn close(&self) -> StorageResult<bool> {
        debug!("try to close filehandle lock ok {:?}", self.fh);
        info!("Close the file handle, ino: {}", self.fh);
        match self.inner.close().await {
            Ok(true) => {
                debug!("filehandle {:?} drop close filehandle ok", self.fh);
                Ok(true)
            }
            Ok(false) => {
                debug!("filehandle {:?} drop filehandle lock ok", self.fh);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Get the open file attributes.
    #[must_use]
    pub async fn getattr(&self) -> FileAttr {
        self.inner.getattr().await
    }

    /// Set the open file attributes.
    pub async fn setattr(&self, attr: FileAttr) {
        self.inner.setattr(attr).await;
    }
}

/// Number of handle shards.
const HANDLE_SHARD_NUM: usize = 100;

/// The `Handles` struct represents a collection of file handles.
/// It uses sharding to avoid lock contention.
#[derive(Debug)]
pub struct Handles {
    /// Use shard to avoid lock contention
    /// Update vec to hashmap to avoid duplicate file handle.
    handles: [Arc<tokio::sync::RwLock<HashMap<u64, FileHandle>>>; HANDLE_SHARD_NUM],
}

impl Default for Handles {
    fn default() -> Self {
        Self::new()
    }
}

impl Handles {
    /// Creates a new `Handles` instance.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let mut handles: Vec<_> = Vec::with_capacity(HANDLE_SHARD_NUM);
        for _ in 0..HANDLE_SHARD_NUM {
            handles.push(Arc::new(tokio::sync::RwLock::new(HashMap::new())));
        }
        let handles: [_; HANDLE_SHARD_NUM] = handles.try_into().unwrap_or_else(|_| {
            unreachable!("The length should match.");
        });
        Handles { handles }
    }

    /// Returns the shard index for the given file handle.
    fn hash(fh: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        fh.hash(&mut hasher);
        (hasher.finish().cast::<usize>()) % HANDLE_SHARD_NUM
    }

    /// Gets a shard of the fh.
    fn get_shard(&self, fh: u64) -> &Arc<tokio::sync::RwLock<HashMap<u64, FileHandle>>> {
        let idx = Self::hash(fh);
        self.handles
            .get(idx)
            .unwrap_or_else(|| unreachable!("The array is ensured to be long enough."))
    }

    /// Returns a file handle from the collection.
    #[must_use]
    pub async fn get_handle(&self, fh: u64) -> Option<FileHandle> {
        let shard = self.get_shard(fh);
        let shard_lock = shard.read().await;
        let fh = shard_lock.get(&fh)?;
        Some(fh.clone())
    }

    /// Create or open a new filehandle, if current filehandle is new, return true
    /// otherwise return false.
    /// If true, we need to update the file handle attr later.
    pub async fn create_or_open_handle<M: MetaData + Send + Sync + 'static>(
        &self,
        fh: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        metadata_client: Arc<M>,
        attr: FileAttr,
    ) -> bool {
        let shard = self.get_shard(fh);
        let mut shard_lock = shard.write().await;
        // If the file handle is already open, reopen it and return false
        if let Some(file_handle) = shard_lock.get(&fh) {
            let open_cnt = file_handle.get_open_count();
            info!("Reopen file handle for ino: {fh} with opencnt: {open_cnt}");
            file_handle.open();

            false
        } else {
            info!("Create a new file handle for ino: {}", fh);
            // If the file handle is not open, create a new file handle and return true
            let file_handle = FileHandle::new(fh, block_size, cache, backend, metadata_client);
            file_handle.setattr(attr).await;
            shard_lock.insert(fh, file_handle);

            true
        }
    }

    /// Close and remove current filehandle.
    pub async fn close_handle(&self, fh: u64) -> StorageResult<Option<FileHandle>> {
        let shard = self.get_shard(fh);
        let mut shard_lock = shard.write().await;
        let filehandle = shard_lock.get(&fh).ok_or_else(|| {
            StorageError::Internal(anyhow::anyhow!("Cannot close a file that is not open."))
        })?;

        let need_remove_flag = filehandle.close().await?;
        if need_remove_flag {
            // Remove the file handle from the handles map
            match shard_lock.remove(&fh) {
                Some(filehandle) => return Ok(Some(filehandle)),
                None => {
                    panic!("Cannot close a file that is not open.");
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
    use crate::async_fuse::memfs::{self, S3MetaData};
    use crate::new_storage::backend::backend_impl::tmp_fs_backend;
    use crate::new_storage::block::BLOCK_SIZE;

    const TEST_NODE_ID: &str = "test_node";
    const TEST_ETCD_ENDPOINT: &str = "127.0.0.1:2379";
    const IO_SIZE: usize = 128 * 1024;

    #[tokio::test]
    async fn test_file_handle() {
        let kv_engine: Arc<memfs::kv_engine::etcd_impl::EtcdKVEngine> = Arc::new(
            KVEngineType::new(vec![TEST_ETCD_ENDPOINT.to_owned()])
                .await
                .unwrap(),
        );
        let metadata_client = S3MetaData::new(kv_engine, TEST_NODE_ID).await.unwrap();

        let cache = Arc::new(Mutex::new(MemoryCache::new(100, BLOCK_SIZE)));
        let backend = Arc::new(tmp_fs_backend().unwrap());
        let handles = Arc::new(Handles::new());
        let ino = 1;

        let creat_res = handles
            .create_or_open_handle(
                ino,
                BLOCK_SIZE,
                cache,
                backend,
                metadata_client,
                FileAttr::default(),
            )
            .await;
        assert!(creat_res);

        let file_handle = handles.get_handle(ino).await.unwrap();
        // handles.add_handle(file_handle.clone()).await;
        let buf = vec![b'1', b'2', b'3', b'4'];
        file_handle.write(0, &buf, 4).await.unwrap();
        let read_buf = file_handle.read(0, 4).await.unwrap();
        assert_eq!(read_buf, buf);
        file_handle.flush().await.unwrap();
    }
}
