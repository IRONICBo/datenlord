//! The utilities of distribute cache cluster management

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::async_fuse::memfs::kv_engine::{KVEngine, LockKeyType, SetOption};
use crate::async_fuse::memfs::kv_engine::{KVEngineType, KeyType, ValueType};
use crate::common::error::{Context, DatenLordResult};

use super::node::Node;
use super::ring::Ring;

/// The timeout for the lock of updating the master node
const MASTER_LOCK_TIMEOUT_SEC: u64 = 60;

/// ETCD client
///
/// This struct is used to interact with etcd server.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterInformer {
    /// Etcd client
    kv_engine: Arc<KVEngineType>,
    /// Retry times
    retry_times: Arc<AtomicUsize>,
    // TODO: Add cancel function like broadcast
}

#[allow(dead_code)]
impl ClusterInformer {
    /// Create a new etcd client
    pub fn new(kv_engine: Arc<KVEngineType>) -> Self {
        Self {
            kv_engine,
            retry_times: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Update node info
    pub async fn update_node_info(&self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        let node = node.read();
        let key = &KeyType::CacheNode(node.ip().to_owned());
        self.kv_engine
            .set(
                key,
                &ValueType::Json(serde_json::to_value(node.dump())?),
                None,
            )
            .await?;
        Ok(())
    }

    /// Register current node to etcd and keep alive
    pub async fn register(&self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        // Get current node info
        let node_for_read = node.read();
        info!("register: {} to etcd", node_for_read.ip());

        // Try to get lease for current node
        let lease = self
            .kv_engine
            .lease_grant(60)
            .await
            .with_context(|| "Failed to get lease for current node")?;

        // Try to register current node to etcd
        self.kv_engine
            .set(
                &KeyType::CacheNode(node_for_read.ip().to_owned()),
                &ValueType::Json(serde_json::to_value(node_for_read.dump())?),
                Some(SetOption {
                    // Set lease
                    lease: Some(lease),
                    prev_kv: false,
                }),
            )
            .await
            .with_context(|| format!("Failed to register node to etcd"))?;

        info!("register: {} to etcd success", node_for_read.ip());
        drop(node_for_read);

        // Set online
        node.write().set_online();

        // Try keep alive current node to clsuter
        let kv_engine_clone = self.kv_engine.clone();
        let node_clone = node.clone();
        // let self_clone = self.clone();
        tokio::task::spawn(async move {
            loop {
                info!("Keep alive node: {}", node_clone.read().ip());
                // Sleep 30s
                tokio::time::sleep(Duration::from_secs(10)).await;

                // Keep alive
                if let Err(e) = kv_engine_clone.lease_keep_alive(lease).await {
                    warn!("Failed to keep alive node: {}", e);

                    node_clone.write().set_offline();

                    /*
                    // Retry to keep alive node
                    let retry_times = self_clone.retry_times.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if retry_times > 3 {
                        error!("Failed to keep alive node, retry times exceed 3");
                        break;
                    }
                    // Retry to register node
                    self_clone.register(node_clone.clone()).await.unwrap();
                     */
                    break;
                }
            }
        });

        Ok(())
    }

    /// Register current node to master and keep alive
    pub async fn register_master(&self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        // Try to get the master lock
        let lock_key = &LockKeyType::CacheNodeMaster("/".to_string());
        let lock = self
            .kv_engine
            .lock(lock_key, Duration::from_secs(MASTER_LOCK_TIMEOUT_SEC))
            .await;

        match lock {
            Ok(_) => {
                // Get current node info
                let dump_node_info = node.read().dump();
                info!("register_master: {} node try to register master", dump_node_info.clone().ip());

                // Try to get master lease for current node
                let lease = self
                    .kv_engine
                    .lease_grant(15)
                    .await
                    .with_context(|| "Failed to get lease for current node")?;

                let value_key = &KeyType::CacheNodeMaster("/".to_string());

                // Try to set self as current master node
                self.kv_engine
                    .set(
                        value_key,
                        &ValueType::Json(serde_json::to_value(dump_node_info.dump())?),
                        Some(SetOption {
                            // Set lease
                            lease: Some(lease),
                            prev_kv: false,
                        }),
                    )
                    .await
                    .with_context(|| format!("Failed to set master node to etcd"))?;

                info!("register_master: {} register master to etcd success", dump_node_info.ip());

                // Set master
                node.write().set_master();

                // Try to keep alive current master node to clsuter
                let kv_engine_clone = self.kv_engine.clone();
                let node_clone = node.clone();
                tokio::task::spawn(async move {
                    loop {
                        // Sleep 10s
                        tokio::time::sleep(Duration::from_secs(10)).await;

                        // Keep alive
                        if let Err(e) = kv_engine_clone.lease_keep_alive(lease).await {
                            warn!("Failed to keep alive master node: {}", e);

                            // Try to remove the master role
                            node_clone.write().set_slave();
                            break;
                        }
                    }
                });
            }
            Err(_) => {
                // Failed to get the master lock, set current node as slave node
                node.write().set_slave();
            }
        }

        Ok(())
    }

    /// Slave nodes will watch the ring update
    pub async fn watch_ring(&self, ring: Arc<RwLock<Ring<Node>>>) -> DatenLordResult<()> {
        info!("watch_ring: will watch the ring update");

        let key = &KeyType::CacheRing("".to_string());
        let mut ring_events = self.kv_engine.watch(key).await?;
        let ring_events = Arc::get_mut(&mut ring_events).unwrap();

        // Wait for ring update
        loop {
            if let Some(event) = ring_events.recv().await {
                let key = event.0;
                let value = event.1;
                match value {
                    Some(item_value) => {
                        // Update event
                        debug!("Receive update ring event with key: {:?}", key);

                        // deserialize ring to Ring<Node>
                        let updated_ring = match item_value {
                            ValueType::Json(ring_json) => {
                                let updated_ring: Ring<Node> =
                                    serde_json::from_value(ring_json.to_owned()).unwrap();
                                Some(updated_ring)
                            }
                            _ => None,
                        };

                        // Update current node ring info
                        if let Some(updated_ring) = updated_ring {
                            // Update ring
                            let mut ring = ring.write();
                            *ring = updated_ring.clone();
                        } else {
                            warn!("Failed to deserialize ring");
                        }
                    }
                    None => {
                        // Delete event
                        info!("delete ring event with key: {:?}", key);
                    }
                }
            }
        }
    }

    /// Master node will watch the node list update, and update the ring
    pub async fn watch_nodes(
        &self,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("watch_nodes: will watch the node list update");

        let key = &KeyType::CacheNode("/".to_string());
        let mut node_events = self.kv_engine.watch(key).await?;
        let node_events = Arc::get_mut(&mut node_events).unwrap();

        // Wait for node list update
        loop {
            if let Some(event) = node_events.recv().await {
                let key = event.0;
                let value = event.1;
                match value {
                    Some(item_value) => {
                        // Update event
                        debug!("Receive update node list event with key: {:?}", key);

                        // deserialize node list to Vec<Node>
                        let updated_node = match item_value {
                            ValueType::Json(nodes_json) => {
                                let updated_node: Node =
                                    serde_json::from_value(nodes_json.to_owned()).unwrap();
                                Some(updated_node)
                            }
                            _ => None,
                        };

                        // Update current node list info
                        if let Some(updated_node) = updated_node {
                            // Append new node to the node list
                            let mut nodes = nodes.write();
                            nodes.push(updated_node.clone());

                            // Update ring
                            let mut ring = ring.write();
                            ring.add(&updated_node, true);
                        } else {
                            warn!("Failed to deserialize node list");
                        }
                    }
                    None => {
                        // Delete event
                        info!("delete node list event with key: {:?}", key);

                        // Try to remove the node from the node list and updated the ring
                        if let Some(node) = nodes.read().iter().find(|node| node.ip() == key) {
                            // Try to remove the node from the node list and get the node info
                            let mut nodes = nodes.write();
                            // Remove node from the node list
                            nodes.retain(|n| n.ip() != node.ip());

                            // Update ring
                            let mut ring = ring.write();
                            ring.remove(node.to_owned(), true);
                        }
                    }
                }
            }
        }
    }

    /// Try to watch the master node
    /// If the master node is down, the slave node will try to get the master lock
    /// Then current node will become the master node
    pub async fn watch_master(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("watch_master: will watch the master node");

        let value_key = &KeyType::CacheNodeMaster("/".to_string());

        // 1. Try to check the master is exist
        // If master key is not exist, try to get the master lock
        let master_node = self.kv_engine.get(value_key).await?;
        match master_node {
            None => {
                // Master node is down, try to get the master lock
                self.register_master(node.clone()).await?;

                // Serve as master node
                // self.watch_nodes(nodes.clone(), ring.clone()).await?;
                let nodes_self = nodes.clone();
                let ring_self = ring.clone();
                let self_clone = self.clone();
                tokio::task::spawn(async move {
                    self_clone.watch_nodes(nodes_self, ring_self).await.unwrap();
                });
            }
            Some(ValueType::Json(master_node_json)) => {
                info!("Try to watch the master node: {:?}", master_node_json);

                let mut master_node_events = self.kv_engine.watch(value_key).await?;
                let master_node_events = Arc::get_mut(&mut master_node_events).unwrap();

                // Wait for master update
                loop {
                    if let Some(event) = master_node_events.recv().await {
                        let key = event.0;
                        let value = event.1;
                        match value {
                            Some(item_value) => {
                                // TODO: Try to watch master node info
                                // Update event
                                info!("Receive update master node event with key: {:?}", key);

                                // deserialize master node to Node
                                let updated_master_node = match item_value {
                                    ValueType::Json(master_node_json) => {
                                        let updated_master_node: Node =
                                            serde_json::from_value(master_node_json.to_owned())
                                                .unwrap();
                                        Some(updated_master_node)
                                    }
                                    _ => None,
                                };

                                // Update current master node info
                                if let Some(updated_master_node) = updated_master_node {
                                    // Update master node
                                    let mut node = node.write();
                                    *node = updated_master_node.clone();
                                } else {
                                    warn!("Failed to deserialize master node");
                                }
                            }
                            None => {
                                // Delete event
                                info!("delete master node event with key: {:?}", key);

                                // Master node is down, try to get the master lock
                                self.register_master(node.clone()).await?;

                                // Serve as master node
                                // self.watch_nodes(nodes.clone(), ring.clone());
                                let nodes_self = nodes.clone();
                                let ring_self = ring.clone();
                                let self_clone = self.clone();
                                tokio::task::spawn(async move {
                                    self_clone.watch_nodes(nodes_self, ring_self).await.unwrap();
                                });
                            }
                        }
                    }
                }
            }
            _ => {
                warn!("Failed to deserialize master node");
            }
        }

        Ok(())
    }

    /// Save ring to etcd
    pub async fn save_ring(&self, ring: &Ring<Node>) -> DatenLordResult<()> {
        // Only master node can save ring to etcd
        // So we do not need to lock the ring
        let key = &KeyType::CacheRing("".to_string());
        debug!("Save ring to etcd: {}", key);
        self.kv_engine
            .set(key, &ValueType::Json(serde_json::to_value(ring)?), None)
            .await?;

        Ok(())
    }

    /// Load ring from etcd
    pub async fn load_ring(&self) -> DatenLordResult<Option<Ring<Node>>> {
        let key = &KeyType::CacheRing("".to_string());
        debug!("Load ring from etcd: {}", key);

        // Get ring from etcd
        let ring = self.kv_engine.get(key).await?;
        match ring {
            Some(ValueType::Json(ring_json)) => {
                let ring: Ring<Node> = serde_json::from_value(ring_json)?;
                Ok(Some(ring))
            }
            _ => Ok(None),
        }
    }

    /// Get node listss
    pub async fn get_nodes(&self) -> DatenLordResult<Vec<Node>> {
        let key = &KeyType::CacheNode("".to_string());
        debug!("Get node list from etcd: {}", key);

        // Get node list from etcd
        let nodes = self.kv_engine.range(key).await?;
        let mut node_list = Vec::new();
        for node in nodes {
            match node {
                ValueType::Json(node_json) => {
                    let node: Node = serde_json::from_value(node_json)?;
                    node_list.push(node);
                }
                _ => {
                    warn!("Failed to deserialize node");
                }
            }
        }

        Ok(node_list)
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
        storage::cache_proxy::{cluster_informer::ClusterInformer, node::Node},
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

        let test_master_node = create_node("192.168.1.2");
        let master_cluster_informer = ClusterInformer::new(client.clone());
        let test_slave_node_1 = create_node("192.168.1.3");
        let slave_1_cluster_informer = ClusterInformer::new(client.clone());
        let test_slave_node_2 = create_node("192.168.1.4");
        let slave_2_cluster_informer = ClusterInformer::new(client.clone());

        info!("test_single_master_election: start to test single master election");

        let (master_res, slave_1_res, slave_2_res) = tokio::join!(
            async {
                // Register node
                let _ = master_cluster_informer
                    .register(test_master_node.clone())
                    .await;
                // Register master
                master_cluster_informer
                    .register_master(test_master_node.clone())
                    .await
            },
            async {
                slave_1_cluster_informer
                    .register(test_slave_node_1.clone())
                    .await
            },
            async {
                slave_2_cluster_informer
                    .register(test_slave_node_2.clone())
                    .await
            }
        );

        // Check the result
        assert!(master_res.is_ok());
        assert!(slave_1_res.is_ok());
        assert!(slave_2_res.is_ok());

        // Check node role
        assert_eq!(test_master_node.read().role().to_string(), "master");
        assert_eq!(test_slave_node_1.read().role().to_string(), "slave");
        assert_eq!(test_slave_node_2.read().role().to_string(), "slave");
    }
}
