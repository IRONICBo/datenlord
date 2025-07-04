#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
#[allow(unused_imports)]
#[allow(clippy::unreadable_literal)]
#[allow(clippy::decimal_literal_representation)]
#[allow(clippy::unseparated_literal_suffix)]
#[allow(clippy::default_numeric_fallback)]
#[allow(clippy::assertions_on_result_states)]
mod tests {
    use std::sync::Arc;

    use crate::{
        distribute_kv_cache::{
            cluster::{
                cluster_manager::ClusterManager,
                node::{Node, NodeStatus},
            },
            kvclient::DistributeKVCacheClient,
            manager::KVCacheHandler,
            rpc::{common::ServerTimeoutOptions, server::RpcServer, workerpool::WorkerPool},
            server_cache::manager::{IndexManager, KVBlockManager},
        },
        fs::kv_engine::{etcd_impl::EtcdKVEngine, DeleteOption, KVEngine, KVEngineType, KeyType},
    };
    use std::sync::Once;

    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{
        filter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt, Layer,
    };

    const ETCD_ADDRESS: &str = "127.0.0.1:2379";
    /// Use for unit test to setup tracing
    static INIT: Once = Once::new();

    /// Set up once for tracing
    fn setup() {
        // init tracing once
        INIT.call_once(|| {
            // Set the tracing log level to debug
            let filter = filter::Targets::new().with_target(
                "datenlord::storage::distribute_kv_cache",
                LevelFilter::DEBUG,
            );
            tracing_subscriber::registry()
                .with(layer().with_filter(filter))
                .init();
        });
    }

    async fn clean_up_etcd() {
        // Clean up all `CacheNode` prefix keys in etcd
        KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheNode(String::new()),
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await
            .unwrap();

        // Clean up all `CacheMasterNode` keys in etcd
        KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheMasterNode,
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_distribute_kv_cache() {
        setup();
        clean_up_etcd().await;

        // Setup the kv cache server
        let ip = "127.0.0.1";
        let port = 2789;
        let addr = format!("{ip}:{port}");
        let etcd_endpoint = "localhost:2379";
        let client = EtcdKVEngine::new(vec![etcd_endpoint.to_owned()])
            .await
            .unwrap();
        let client = Arc::new(client);

        let node = Node::new(ip.to_owned(), port, 1, NodeStatus::Initializing);
        let cluster_manager = Arc::new(ClusterManager::new(client, node));
        let cluster_manager_clone = Arc::clone(&cluster_manager);
        tokio::spawn(async move {
            cluster_manager_clone.run().await.unwrap();
        });

        // Wait for the cluster manager login as master
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        let is_master_online = cluster_manager.get_master_node().await;
        assert!(is_master_online.is_ok());

        let cache_manager = Arc::new(KVBlockManager::default());
        let index_manager = Arc::new(IndexManager::<u32>::new());

        let pool = Arc::new(WorkerPool::new(5, 5));
        let handler = KVCacheHandler::new(Arc::clone(&pool), cache_manager, index_manager);
        let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 5, 5, handler);
        server.listen(&addr).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;

        // Setup the kv cache client
        let block_size = 16777216;
        let kvcacheclient = DistributeKVCacheClient::new(cluster_manager, block_size);
        kvcacheclient.start_watch().await.unwrap();

        // Test insert 1 to the kv cache client
        let prefix = [1, 2, 3, 1];
        let data = vec![1u8; 16777216];
        kvcacheclient.insert(prefix.to_vec(), data).await.unwrap();

        // Test insert 2 to the kv cache client
        let prefix = vec![1, 2, 3, 2];
        let data = vec![2u8; 16777216];
        kvcacheclient.insert(prefix, data).await.unwrap();

        // Test insert 3 to the kv cache client, will evict and insert to remote node
        let prefix = vec![1, 2, 3, 3];
        let data = vec![3u8; 16777216];
        kvcacheclient.insert(prefix.clone(), data).await.unwrap();

        // Test get 1 from the kv cache client
        let prefix = vec![1, 2, 3, 1];
        let (matched_prefix, buf) = kvcacheclient.try_load(prefix.clone()).await.unwrap();
        assert_eq!(matched_prefix, vec![1, 2, 3, 1]);
        assert_eq!(buf.len(), 16777216);
        assert!(buf.iter().all(|&x| x == 1));

        let start = tokio::time::Instant::now();
        for i in 4_u32..=10 {
            // Test get 2 from the kv cache client
            let prefix = vec![0_u32, i];
            let data = vec![1u8; 16777216];
            kvcacheclient.insert(prefix.clone(), data).await.unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "Speed: {:?}",
            16777216.0 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
        );

        // Test get 4 from the kv cache client, should return error
        let prefix = vec![1, 2, 3, 4];
        let (_, buf) = kvcacheclient.try_load(prefix.clone()).await.unwrap();
        assert_eq!(buf.len(), 0);

        // Test get 111 from the kv cache client, should return partial result
        let prefix = vec![1, 2, 3, 1, 1, 1];
        let (matched_prefix, buf) = kvcacheclient.try_load(prefix.clone()).await.unwrap();
        assert_eq!(matched_prefix, vec![1, 2, 3, 1]);
        assert_eq!(buf.len(), 16777216);
        assert!(buf.iter().all(|&x| x == 1));
    }
}
