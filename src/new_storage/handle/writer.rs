//! The writer implementation.

use std::sync::Arc;

use bytes::Bytes;
use clippy_utilities::Cast;
use hashbrown::HashSet;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use super::super::policy::LruPolicy;
use super::super::{
    format_path, offset_to_slice, Backend, Block, BlockSlice, CacheKey, MemoryCache,
};
use crate::async_fuse::memfs::MetaData;
use crate::new_storage::{StorageError, StorageResult};

/// The `Writer` struct represents a struct responsible for writing blocks of
/// data to a backend storage system
pub struct Writer {
    /// The inode number associated with the writer
    ino: u64,
    /// The block size
    block_size: usize,
    /// The cache manager.
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// The sender to send tasks to the write back worker.
    write_back_sender: Sender<Task>,
    /// The handle to the write back worker.
    write_back_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    /// The sender to send meta task to the meta task worker.
    meta_task_sender: Sender<MetaTask>,
    /// The handle to the meta task worker.
    meta_task_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    /// The access keys.
    access_keys: Mutex<Vec<CacheKey>>,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
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
    /// A finish task.
    Finish(oneshot::Sender<Option<StorageError>>),
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
enum Task {
    /// A pending write task.
    Pending(Arc<WriteTask>),
    /// A flush task.
    Flush(oneshot::Sender<Option<StorageError>>),
    /// A finish task.
    Finish(oneshot::Sender<Option<StorageError>>),
}

/// The `WriteTask` struct represents a write task
#[derive(Debug)]
struct WriteTask {
    /// The cache manager.
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// The inode number associated with the file being written.
    ino: u64,
    /// The block id.
    block_id: u64,
    /// The block to be written.
    block: Arc<RwLock<Block>>,
    /// Meta commit task sender, when current block is written back, we need to send a meta commit task.
    meta_task_sender: Sender<MetaTask>,
}

/// Write a block back to the backend.
async fn write_back_block(task: Arc<WriteTask>) -> StorageResult<()> {
    let path = format_path(task.ino, task.block_id);
    loop {
        let (content, version) = {
            let block = task.block.read();
            if !block.dirty() {
                // The block has been flushed previously, skip
                return Ok(());
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
                ino: task.ino,
                block_id: task.block_id,
            });
            break;
        }
    }

    // Send the meta commit task.
    let meta_task = Arc::new(MetaCommitTask { ino: task.ino });
    match task
        .meta_task_sender
        .send(MetaTask::Pending(meta_task))
        .await
    {
        Ok(()) => {
            debug!(
                "Send meta task successfully, current meta task is {:?}",
                task
            );
        }
        Err(e) => {
            error!("Failed to send meta task, the error is {e}.");
        }
    }

    Ok(())
}

/// Write the blocks to the backend storage system concurrently.
async fn write_blocks(tasks: &Vec<Arc<WriteTask>>) -> Option<StorageError> {
    let mut handles = Vec::new();
    let mut result = None;
    for task in tasks {
        let handle = tokio::spawn(write_back_block(Arc::clone(task)));
        handles.push(handle);
    }
    for handle in handles {
        match handle.await {
            Err(e) => {
                result = Some(StorageError::Internal(e.into()));
            }
            Ok(Err(e)) => {
                result = Some(e);
            }
            _ => {}
        }
    }
    result
}

/// The `commit_meta_data` function represents the meta data commit operation.
async fn commit_meta_data<M: MetaData + Send + Sync + 'static>(
    metadata_client: Arc<M>,
    to_be_committed_inos: &HashSet<u64>,
) {
    for ino in to_be_committed_inos {
        if let Err(e) = metadata_client.write_remote_helper(ino.to_owned()).await {
            error!("Failed to commit meta data, the error is {e}.");
        }
    }
}

/// The `meta_commit_work` function represents the meta commit worker.
#[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select!`
async fn meta_commit_work<M: MetaData + Send + Sync + 'static>(
    metadata_client: Arc<M>,
    mut meta_task_receiver: Receiver<MetaTask>,
) {
    // We will receive the meta write back message here and flush open_files status to meta data server,
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut to_be_committed_inos = HashSet::new();
    loop {
        tokio::select! {
            Some(task) = meta_task_receiver.recv() => {
                match task {
                    MetaTask::Pending(meta_task) => {
                        let ino = meta_task.ino;
                        to_be_committed_inos.insert(ino);
                    }
                    MetaTask::Flush(tx) => {
                        // Commit immediately.
                        commit_meta_data(Arc::clone(&metadata_client), &to_be_committed_inos).await;
                        to_be_committed_inos.clear();

                        // Flush the open_files to the meta data server.
                        if let Err(Some(e)) = tx.send(None) {
                            error!("Failed to send storage error back to `Writer`, the error is {e}.");
                        }
                    }
                    MetaTask::Finish(tx) => {
                        // Commit immediately.
                        commit_meta_data(Arc::clone(&metadata_client), &to_be_committed_inos).await;
                        to_be_committed_inos.clear();

                        // Flush the open_files to the meta data server.
                        if let Err(Some(e)) = tx.send(None) {
                            error!("Failed to send storage error back to `Writer`, the error is {e}.");
                        }
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                commit_meta_data(Arc::clone(&metadata_client), &to_be_committed_inos).await;
                to_be_committed_inos.clear();
            }
        }
    }
}

/// The `write_back_work` function represents the write back worker.
#[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select!`
async fn write_back_work(mut write_back_receiver: Receiver<Task>) {
    //  Create a timer to flush the cache every 200ms.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
    let mut tasks = Vec::new();
    let mut result = None;
    loop {
        tokio::select! {
            Some(task) = write_back_receiver.recv() => {
                match task {
                    Task::Pending(task) => {
                        tasks.push(task);
                        if tasks.len() >= 10 {
                            let res = write_blocks(&tasks).await;
                            if let Some(e) = res {
                                result.get_or_insert(e);
                            }
                            tasks.clear();
                        }
                    }
                    Task::Flush(tx) => {
                        let res = write_blocks(&tasks).await;
                        tasks.clear();
                        let res = result.take().or(res);
                        if let Err(Some(e)) = tx.send(res) {
                            error!("Failed to send storage error back to `Writer`, the error is {e}.");
                        }
                    }
                    Task::Finish(tx) => {
                        let res = write_blocks(&tasks).await;
                        tasks.clear();
                        let res = result.take().or(res);
                        if let Err(Some(e)) = tx.send(res) {
                            error!("Failed to send storage error back to `Writer`, the error is {e}.");
                        }
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                write_blocks(&tasks).await;
                tasks.clear();
            }
        }
    }
}

impl Writer {
    /// Create a new `Writer`.
    #[inline]
    #[must_use]
    pub fn new<M: MetaData + Send + Sync + 'static>(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        metadata_client: Arc<M>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let (meta_task_tx, meta_task_rx) = tokio::sync::mpsc::channel(100);
        let mut writer = Writer {
            ino,
            block_size,
            cache,
            backend,
            write_back_sender: tx,
            write_back_handle: tokio::sync::Mutex::new(None),
            meta_task_sender: meta_task_tx,
            meta_task_handle: tokio::sync::Mutex::new(None),
            access_keys: Mutex::new(Vec::new()),
        };
        let handle = tokio::spawn(write_back_work(rx));
        writer.write_back_handle = tokio::sync::Mutex::new(Some(handle));

        let meta_commit_handle = tokio::spawn(meta_commit_work(metadata_client, meta_task_rx));
        writer.meta_task_handle = tokio::sync::Mutex::new(Some(meta_commit_handle));
        writer
    }

    /// Record the block access.
    fn access(&self, block_id: u64) {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let mut access_keys = self.access_keys.lock();
        access_keys.push(key);
    }

    /// Fetch the block from the cache manager.
    #[inline]
    pub async fn fetch_block(&self, block_id: u64) -> StorageResult<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };

        {
            let cache = self.cache.lock();
            if let Some(block) = cache.fetch(&key) {
                return Ok(block);
            }
        }

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

    /// Writes data to the file starting at the given offset.
    #[inline]
    pub async fn write(&self, buf: &[u8], slices: &[BlockSlice]) -> StorageResult<()> {
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
            self.access(block_id);
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
                ino: self.ino,
                block_id,
                block,
                meta_task_sender: self.meta_task_sender.clone(),
            });
            self.write_back_sender
                .send(Task::Pending(task))
                .await
                .unwrap_or_else(|_| {
                    panic!("Should not send command to write back task when the task quits.");
                });
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
            .map_or(Ok(()), Err)?;

        let (tx, rx) = oneshot::channel();
        self.meta_task_sender
            .send(MetaTask::Flush(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to meta task when the task quits.");
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
            self.write(&buf, &[slice]).await?;
        }

        Ok(())
    }

    /// Closes the writer associated with the file handle.
    pub async fn close(&self) -> StorageResult<()> {
        let (tx, rx) = oneshot::channel();
        self.write_back_sender
            .send(Task::Finish(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to write back task when the task quits.");
            });
        // TODO: handle it by `TaskManager`
        self.write_back_handle
            .lock()
            .await
            .take()
            .unwrap_or_else(|| {
                unreachable!("The `JoinHandle` should not be None.");
            })
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to join the write back task: {e}");
            });

        {
            let keys = self.access_keys.lock();
            for key in keys.iter() {
                self.cache.lock().remove(key);
            }
        }

        rx.await
            .unwrap_or_else(|_| panic!("The sender should not be closed."))
            .map_or(Ok(()), Err)?;

        let (tx, rx) = oneshot::channel();
        // TODO: handle it by `TaskManager`
        self.meta_task_sender
            .send(MetaTask::Finish(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to meta task when the task quits.");
            });

        self.meta_task_handle
            .lock()
            .await
            .take()
            .unwrap_or_else(|| {
                unreachable!("The `JoinHandle` should not be None.");
            })
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to join the meta task: {e}");
            });

        rx.await
            .unwrap_or_else(|_| panic!("The sender should not be closed."))
            .map_or(Ok(()), Err)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
    use crate::async_fuse::memfs::{self, S3MetaData};
    use crate::new_storage::backend::backend_impl::memory_backend;
    use crate::new_storage::block::BLOCK_SIZE;

    const IO_SIZE: usize = 128 * 1024;
    const TEST_NODE_ID: &str = "test_node";
    const TEST_ETCD_ENDPOINT: &str = "127.0.0.1:2379";

    #[tokio::test]
    async fn test_writer() {
        let backend = Arc::new(memory_backend().unwrap());
        let manger = Arc::new(Mutex::new(MemoryCache::new(10, BLOCK_SIZE)));

        let kv_engine: Arc<memfs::kv_engine::etcd_impl::EtcdKVEngine> = Arc::new(
            KVEngineType::new(vec![TEST_ETCD_ENDPOINT.to_owned()])
                .await
                .unwrap(),
        );
        let metadata_client = S3MetaData::new(kv_engine, TEST_NODE_ID).await.unwrap();

        let writer = Writer::new(1, BLOCK_SIZE, Arc::clone(&manger), backend, metadata_client);
        let content = Bytes::from_static(&[b'1'; IO_SIZE]);
        let slice = BlockSlice::new(0, 0, content.len().cast());
        writer.write(&content, &[slice]).await.unwrap();
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 1);
        writer.close().await.unwrap();
        let memory_size = manger.lock().len();
        assert_eq!(memory_size, 0);
    }
}
