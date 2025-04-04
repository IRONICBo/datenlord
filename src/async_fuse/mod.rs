//! FUSE async implementation

use std::net::IpAddr;
use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;
use fuse::file_system::FuseFileSystem;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::async_fuse::fuse::session;
use crate::config::StorageConfig;
use crate::fs::datenlordfs;
use crate::fs::kv_engine::KVEngineType;
use crate::new_storage::{BackendBuilder, MemoryCache, StorageManager};

pub mod fuse;
pub mod proactor;
pub mod util;

/// Async fuse args type
#[derive(Debug)]
pub struct AsyncFuseArgs {
    /// Node id
    pub node_id: String,
    /// Node ip
    pub ip_address: IpAddr,
    /// Server port
    pub server_port: u16,
    /// Mount dir
    pub mount_dir: String,
    /// Storage config
    pub storage_config: StorageConfig,
}

/// Start async-fuse
#[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select`
pub async fn start_async_fuse(
    kv_engine: Arc<KVEngineType>,
    args: AsyncFuseArgs,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let storage_config = &args.storage_config;

    let mount_point = std::path::Path::new(&args.mount_dir);
    let global_cache_capacity = args.storage_config.memory_cache_config.capacity;
    let metadata_client = datenlordfs::MetaData::new(Arc::clone(&kv_engine), &args.node_id).await?;
    let storage = {
        let storage_param = &storage_config.params;
        let memory_cache_config = &storage_config.memory_cache_config;

        let block_size = storage_config.block_size;
        let capacity_in_blocks = memory_cache_config.capacity.overflow_div(block_size);

        let cache = Arc::new(Mutex::new(MemoryCache::new(capacity_in_blocks, block_size)));
        let backend = Arc::new(BackendBuilder::new(storage_param.clone()).build().await?);

        StorageManager::new(cache, backend, block_size, Arc::clone(&metadata_client))
    };

    let fs: FuseFileSystem<datenlordfs::S3MetaData> = FuseFileSystem::new_datenlord_fs(
        &args.mount_dir,
        global_cache_capacity,
        storage_config,
        storage,
        &args.node_id,
        Arc::clone(&metadata_client),
    );

    let ss = session::new_fuse_session(mount_point, fs).await?;
    ss.run(token).await?;

    Ok(())
}

#[cfg(test)]
mod test {
    mod integration_tests;
    mod test_util;

    use std::{fs, io};

    use futures::StreamExt;
    use tracing::debug;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_iter() -> io::Result<()> {
        let dir = tokio::task::spawn_blocking(|| fs::read_dir(".")).await??;
        let mut dir = futures::stream::iter(dir);
        while let Some(entry) = dir.next().await {
            let path = entry?.path();
            if path.is_file() {
                debug!("read file: {:?}", path);
                let buf = tokio::fs::read(path).await?;
                let output_length = 16;
                if buf.len() > output_length {
                    debug!(
                        "first {} bytes: {:?}",
                        output_length,
                        &buf.get(..output_length)
                    );
                } else {
                    debug!("total bytes: {:?}", buf);
                }
            } else {
                debug!("skip directory: {:?}", path);
            }
        }
        Ok(())
    }
}
