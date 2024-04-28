use std::{fmt::Debug, sync::Arc};

use parking_lot::RwLock;

use crate::async_fuse::memfs::kv_engine::KVEngineType;
use crate::common::error::DatenLordResult;

use super::{cluster_manager::ClusterManager, node::Node, ring::Ring};

/// DistributeCacheCluster
///
/// This struct is used to manage the inner topology cache.
#[allow(dead_code)]
pub struct DistributeCacheCluster {
    /// The cache proxy topology
    node: Arc<RwLock<Node>>,
    /// Proxy topology for hashring
    hashring: Arc<RwLock<Ring<Node>>>,
    /// Node list
    node_list: Arc<RwLock<Vec<Node>>>,
    /// Cluster informer
    cluster_manager: Arc<ClusterManager>,
}

impl DistributeCacheCluster {
    /// Create a new proxy topology
    pub fn new(node: Arc<RwLock<Node>>, kv_engine: Arc<KVEngineType>) -> Self {
        let node_list = Arc::new(RwLock::new(Vec::new()));
        let hashring = Arc::new(RwLock::new(Ring::default()));
        let cluster_manager = Arc::new(ClusterManager::new(kv_engine));

        Self {
            node,
            hashring,
            node_list,
            cluster_manager,
        }
    }

    /// Register to cluster
    pub async fn register(&self) -> DatenLordResult<()> {
        // 1. join to the cluster and fetch the cluster info
        self.cluster_manager.register(self.node.clone()).await?;
        {
            // TODO: set a register lock to fetch the cluster info
            if let Some(ring) = self.cluster_manager.load_ring().await? {
                self.hashring.write().clone_from(&ring);
            }
            let nodes = self.cluster_manager.get_nodes().await?;
            self.node_list.write().clone_from(&nodes);
        }

        // 2. Watch the cluster master
        let cluster_manager_clone = self.cluster_manager.clone(); // Assume ClusterManager is cloneable or wrapped in Arc
        let node_clone = self.node.clone();
        let node_list_clone = self.node_list.clone();
        let hashring_clone = self.hashring.clone();
        tokio::task::spawn(async move {
            let _ = cluster_manager_clone
                .watch_master(node_clone, node_list_clone, hashring_clone)
                .await;
        });

        // 3. Watch the cluster nodes
        let cluster_manager_clone = self.cluster_manager.clone(); // Assume ClusterManager is cloneable or wrapped in Arc
        let node_list_clone = self.node_list.clone();
        let hashring_clone = self.hashring.clone();
        tokio::task::spawn(async move {
            let _ = cluster_manager_clone
                .watch_nodes(node_list_clone, hashring_clone)
                .await;
        });

        Ok(())
    }
}

impl Debug for DistributeCacheCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributeCacheCluster")
            .field("node", &self.node)
            .field("hashring", &self.hashring)
            .field("node_list", &self.node_list)
            .field("cluster_manager", &self.cluster_manager)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tracing::info;
    use tracing_subscriber;

    use crate::{
        async_fuse::memfs::kv_engine::{KVEngine, KVEngineType},
        storage::cache_proxy::{cluster::DistributeCacheCluster, node::Node},
    };

    const ETCD_ADDRESS: &str = "127.0.0.1:2379";

    /// Helper function to create a new node with a given IP address
    fn create_node(ip: &str) -> Arc<RwLock<Node>> {
        let mut node = Node::default();
        node.set_ip(ip.to_string());

        let node = Arc::new(RwLock::new(node));
        node
    }

    #[tokio::test]
    async fn test_single_master_election() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
                .await
                .unwrap(),
        );

        let master_node_info = create_node("192.168.1.2");
        let master_node = DistributeCacheCluster::new(master_node_info.clone(), client.clone());
        let slave_node_1_info = create_node("192.168.1.3");
        let slave_node_1 = DistributeCacheCluster::new(slave_node_1_info.clone(), client.clone());
        let slave_node_2_info = create_node("192.168.1.4");
        let slave_node_2 = DistributeCacheCluster::new(slave_node_2_info.clone(), client.clone());

        info!("test_single_master_election: start to test single master election");

        let (master_res, slave_1_res, slave_2_res) = tokio::join!(
            async { master_node.register().await },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                slave_node_1.register().await
            },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                slave_node_2.register().await
            }
        );
        // Wait for the election to finish
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        info!("test_single_master_election: finish to test single master election");

        // Check the result
        assert!(master_res.is_ok());
        assert!(slave_1_res.is_ok());
        assert!(slave_2_res.is_ok());

        // Check node role
        assert_eq!(master_node_info.read().role().to_string(), "master");
        assert_eq!(slave_node_1_info.read().role().to_string(), "slave");
        assert_eq!(slave_node_2_info.read().role().to_string(), "slave");
    }

    #[tokio::test]
    async fn test_add_new_node() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
                .await
                .unwrap(),
        );

        let master_node_info = create_node("192.168.1.2");
        let master_node = DistributeCacheCluster::new(master_node_info.clone(), client.clone());
        let slave_node_1_info = create_node("192.168.1.3");
        let slave_node_1 = DistributeCacheCluster::new(slave_node_1_info.clone(), client.clone());
        let slave_node_2_info = create_node("192.168.1.4");
        let slave_node_2 = DistributeCacheCluster::new(slave_node_2_info.clone(), client.clone());

        let (master_res, slave_1_res) =
            tokio::join!(async { master_node.register().await }, async {
                slave_node_1.register().await
            });
        // Wait for the election to finish
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Check the result
        assert!(master_res.is_ok());
        assert!(slave_1_res.is_ok());

        // Test add new node
        slave_node_2.register().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Check hashring and node list
        let hashring = master_node.hashring.read();
        let node_list = master_node.node_list.read();
        assert_eq!(hashring.len_slots(), 3);
        assert_eq!(node_list.len(), 3);

        drop(hashring);
        drop(node_list);
    }

    #[tokio::test]
    async fn test_remove_node() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
                .await
                .unwrap(),
        );

        let master_node_info = create_node("192.168.1.2");
        let master_node = DistributeCacheCluster::new(master_node_info.clone(), client.clone());
        let slave_node_1_info = create_node("192.168.1.3");
        let slave_node_1 = DistributeCacheCluster::new(slave_node_1_info.clone(), client.clone());

        let (master_res, slave_1_res) =
            tokio::join!(async { master_node.register().await }, async {
                slave_node_1.register().await
            });

        // Wait for the election to finish
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Check the result
        assert!(master_res.is_ok());
        assert!(slave_1_res.is_ok());

        // Abort slave node 1 and check the result
        drop(slave_node_1);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        // Check hashring and node list
        let hashring = master_node.hashring.read();
        let node_list = master_node.node_list.read();
        assert_eq!(hashring.len_slots(), 1);
        assert_eq!(node_list.len(), 1);
    }
}
