//! The utilities of distribute cache cluster management

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::async_fuse::memfs::kv_engine::{etcd_impl, KVEngine, LockKeyType, SetOption};
use crate::async_fuse::memfs::kv_engine::{KVEngineType, KeyType, ValueType};
use crate::common::error::{Context, DatenLordResult};

use super::node::{Node, NodeStatus};
use super::ring::Ring;

/// The timeout for the lock of updating the master node
const MASTER_LOCK_TIMEOUT_SEC: i64 = 30;
/// The timeout for the node register
const NODE_REGISTER_TIMEOUT_SEC: i64 = 60;

/// ETCD client
///
/// This struct is used to interact with etcd server.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterManager {
    /// Etcd client
    kv_engine: Arc<KVEngineType>,
    /// Register session
    /// Used to store the register tasks,
    /// so we can cancel the tasks when the node is down or role changed
    register_session: Option<Arc<etcd_impl::Session>>,
    /// Master session
    /// Used to store the master tasks,
    /// Ditto
    master_session: Option<Arc<etcd_impl::Session>>,
}

#[allow(dead_code)]
impl ClusterManager {
    /// Create a new etcd client
    pub fn new(kv_engine: Arc<KVEngineType>) -> Self {
        Self {
            kv_engine,
            register_session: None,
            master_session: None,
        }
    }

    /// Run the cluster manager as state machine
    pub async fn run(
        &mut self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        loop {
            let current_state = node.read().status();
            match current_state {
                // Register node to etcd
                NodeStatus::Initializing => {
                    info!("Current node status: {:?}", current_state);
                    // Update node status to Registering
                    node.write().set_status(NodeStatus::Registering);
                    self.update_node_info(node.clone()).await?;
                }
                // Register master node to etcd
                NodeStatus::Registering => {
                    info!("Current node status: {:?}", current_state);
                    info!("Clean up the previous session");
                    // Clean up the sessions
                    self.clean_sessions().await;

                    while self.register(node.clone()).await.is_err() {
                        error!("Failed to register node, retry in 5s");
                        // Try to register node to etcd
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }

                    // Update node status to Registering
                    node.write().set_status(NodeStatus::Slave);
                    self.update_node_info(node.clone()).await?;
                }
                // Serve as slave node
                NodeStatus::Slave => {
                    self.do_slave_tasks(node.clone(), ring.clone()).await?;
                }
                // Serve as master node
                NodeStatus::Master => {
                    self.do_master_tasks(node.clone(), nodes.clone(), ring.clone())
                        .await?;
                }
            }
        }
    }

    /// Update node info
    pub async fn update_node_info(&mut self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        let node = node.read();
        let key = &KeyType::CacheNode(node.ip().to_owned());
        while self
            .kv_engine
            .set(
                key,
                &ValueType::Json(serde_json::to_value(node.dump())?),
                None,
            )
            .await
            .is_err()
        {
            error!("Failed to update node info, retry in 5s");
            // Try to update node info
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }

    /// Register current node to etcd and keep alive
    pub async fn register(&mut self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        // Check the node status
        if node.read().status() != NodeStatus::Registering {
            return Ok(());
        }

        // Get current node info
        let node_dump = node.read().dump();
        info!("register: {} to etcd", node_dump.ip());

        // Try to get lease for current node
        let lease = self
            .kv_engine
            .lease_grant(NODE_REGISTER_TIMEOUT_SEC)
            .await
            .with_context(|| "Failed to get lease for current node")?;

        // Try to register current node to etcd
        self.kv_engine
            .set(
                &KeyType::CacheNode(node_dump.ip().to_owned()),
                &ValueType::Json(serde_json::to_value(node_dump.clone())?),
                Some(SetOption {
                    // Set lease
                    lease: Some(lease),
                    prev_kv: false,
                }),
            )
            .await
            .with_context(|| format!("Failed to register node to etcd"))?;

        info!("register: {} to etcd success", node_dump.ip());

        // Set online status, default is slave
        node.write().set_status(NodeStatus::Slave);

        // Try keep alive current node to clsuter
        let register_session = self
            .kv_engine
            .create_session(lease, NODE_REGISTER_TIMEOUT_SEC)
            .await;
        self.register_session = Some(register_session.clone());

        // Check session available
        let self_arc = Arc::new(self.clone());
        tokio::task::spawn_local(self_arc.register_sessions_check_task(node.clone()));

        Ok(())
    }

    async fn register_sessions_check_task(self: Arc<Self>, node: Arc<RwLock<Node>>) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(NODE_REGISTER_TIMEOUT_SEC as u64 / 3));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let register_session = self.register_session.clone();
                    match register_session {
                        Some(session) => {
                            // Check the register session
                            if session.is_closed() {
                                // Try to register node to etcd
                                let _ = self.register(node.clone()).await;
                                return;
                            }
                            continue;
                        }
                        None => {
                            // Try to register node to etcd
                            let _ = self.register(node.clone()).await;
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn master_sessions_check_task(self: Arc<Self>, node: Arc<RwLock<Node>>) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(NODE_REGISTER_TIMEOUT_SEC as u64 / 3));

        loop {
            tokio::select! {
                let master_session = self.master_session.clone();
                match self.master_session {
                    Some(session) => {
                        // Check the register session
                        if session.is_closed() {
                            // Try to register node to etcd
                            let _ = self.register(node.clone()).await;
                            return;
                        }
                        continue;
                    }
                    None => {
                        // Try to register node to etcd
                        let _ = self.register(node.clone()).await;
                        return;
                    }
                }
            }
        }
    }

    pub async fn do_campaign(
        &self,
        node: Arc<RwLock<Node>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        let client = self.kv_engine.clone();
        let lease = self
            .kv_engine
            .lease_grant(MASTER_LOCK_TIMEOUT_SEC)
            .await
            .with_context(|| "Failed to get lease for current node")?;
        let master_key = &LockKeyType::CacheNodeMaster(node.read().ip().to_owned());
        let ring_value = &ValueType::Json(serde_json::to_value(ring.read().dump())?);
        let leader_key = client.campaign(master_key, ring_value, lease).await?;

        // Check the leader key
        if leader_key == node.read().ip() {
            // Serve as master node
            node.write().set_status(NodeStatus::Master);

            // Try keep alive current master to clsuter
            let master_session = self
                .kv_engine
                .create_session(lease, NODE_REGISTER_TIMEOUT_SEC)
                .await;
            self.master_sessions.lock().push(master_session);

            return Ok(());
        }

        // Serve as slave node
        node.write().set_status(NodeStatus::Slave);
        Ok(())
    }

    pub async fn do_slave_tasks(
        &self,
        node: Arc<RwLock<Node>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("do_slave_tasks: will watch the ring update and campaign master");

        // 1. Try to campaign master
        self.do_campaign(node.clone(), ring.clone()).await?;

        // 2. Try to watch master and hashring
        let (close_tx, close_rx) = mpsc::channel(1);
        let watch_ring_handle = tokio::task::spawn_local(self.watch_ring(
            node.clone(),
            ring.clone(),
            node.read().ip().to_owned(),
            close_rx,
        ));

        // Wait for status update
        loop {
            // Check the node status
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(MASTER_LOCK_TIMEOUT_SEC as u64 / 3)) => {
                    // Check the node status
                    if node.read().status() != NodeStatus::Slave {
                        // If the node status is not slave, clean up slave tasks and return
                        // Clean up slave tasks
                        let mut slave_sessions = self.slave_sessions.lock();
                        slave_sessions.clear();

                        // Try to close the watch ring task
                        if close_tx.try_send(()).is_err() {
                            error!("failed to send close signal to session");
                        }

                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn do_master_tasks(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("do_master_tasks: will watch the node list update, and update the ring");

        let node_key = &KeyType::CacheNode(node.read().ip().to_owned());
        let node_prefix = node_key.prefix();
        let mut node_events = self.kv_engine.watch(node_key).await?;
        let node_events = Arc::get_mut(&mut node_events).unwrap();

        // Watch for node list update

        // Wait for node list update
        loop {
            // Keep alive master key
            // Check current status
            if node.read().status() != NodeStatus::Master {
                // If the node status is not master, clean up master tasks and return
                // Clean up master tasks
                let mut master_tasks = self.master_tasks.lock();
                for master_task in master_tasks.iter() {
                    master_task.abort();
                }
                master_tasks.clear();

                return Ok(());
            }

            // Do watch node list update task
            let arc_self = Arc::new(self.clone());
            let watch_node_handle =
                tokio::task::spawn_local(arc_self.watch_nodes(nodes.clone(), ring.clone()));
            self.master_tasks.lock().push(watch_node_handle);
        }
    }

    /// Clean up the tasks
    pub async fn clean_sessions(&self) {
        // Clean up register tasks
        self.register_session = None;

        // Clean up master tasks
        self.master_session = None;
    }

    /// Slave nodes will watch the ring update
    pub async fn watch_ring(
        &self,
        node: Arc<RwLock<Node>>,
        ring: Arc<RwLock<Ring<Node>>>,
        master_key: String,
        close_rx: mpsc::Receiver<()>,
    ) -> DatenLordResult<()> {
        info!("watch_ring: will watch the ring update");

        let mut interval =
            tokio::time::interval(Duration::from_secs(MASTER_LOCK_TIMEOUT_SEC as u64 / 3));
        let key = &KeyType::CacheNodeMaster(master_key);
        let mut ring_events = self.kv_engine.watch(key).await?;
        let ring_events = Arc::get_mut(&mut ring_events).unwrap();

        // Wait for ring update
        loop {
            tokio::select! {
                _ = interval.tick() => {
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
                                // Master has down, try to campaign master
                            }
                        }
                    }
                }
                _ = close_rx.recv() => {
                    // Close the watch ring task
                    return Ok(());
                }
            }
        }
    }

    /// Master node will watch the node list update, and update the ring
    pub async fn watch_nodes(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) {
        info!("watch_nodes: will watch the node list update");

        let key = &KeyType::CacheNode("/".to_string());
        let mut node_events = self.kv_engine.watch(key).await.unwrap();
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

                            // Update to etcd
                            let master_key = &KeyType::CacheNodeMaster(node.read().ip().to_owned());
                            let current_json_value;
                            if let Ok(json_value) = serde_json::to_value(ring.dump()) {
                                current_json_value = json_value;
                            } else {
                                warn!("Failed to serialize ring");
                                continue;
                            }
                            let ring_value = &ValueType::Json(current_json_value);

                            // Try to get current lease
                            let master_sessions = self.master_session.clone();
                            match master_sessions {
                                Some(session) => {
                                    let lease = session.lease_id();
                                    self.kv_engine
                                        .set(
                                            master_key,
                                            ring_value,
                                            Some(SetOption {
                                                // Set lease
                                                lease: Some(lease),
                                                prev_kv: false,
                                            }),
                                        )
                                        .await;
                                }
                                None => {
                                    error!("Failed to get lease for master node");
                                    // Change to slave node
                                    node.write().set_status(NodeStatus::Slave);
                                    return;
                                }
                            };
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

                            // Update to etcd
                            let master_key = &KeyType::CacheNodeMaster(node.read().ip().to_owned());
                            let current_json_value;
                            if let Ok(json_value) = serde_json::to_value(ring.dump()) {
                                current_json_value = json_value;
                            } else {
                                warn!("Failed to serialize ring");
                                continue;
                            }
                            let ring_value = &ValueType::Json(current_json_value);

                            // Try to get current lease
                            let master_sessions = self.master_session.clone();
                            match master_sessions {
                                Some(session) => {
                                    let lease = session.lease_id();
                                    self.kv_engine
                                        .set(
                                            master_key,
                                            ring_value,
                                            Some(SetOption {
                                                // Set lease
                                                lease: Some(lease),
                                                prev_kv: false,
                                            }),
                                        )
                                        .await;
                                }
                                None => {
                                    error!("Failed to get lease for master node");
                                    // Change to slave node
                                    node.write().set_status(NodeStatus::Slave);
                                    return;
                                }
                            };
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
    ) {
        info!("watch_master: will watch the master node");

        let value_key = &KeyType::CacheNodeMaster;

        // 1. Try to check the master is exist
        // If master key is not exist, try to get the master lock
        loop {
            match self.kv_engine.get(value_key).await {
                Ok(master_node) => {
                    match master_node {
                        None => {
                            // Master node is down, try to get the master lock
                            self.campaign_master(node.clone()).await;

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

                            let mut master_node_events = self.kv_engine.watch(value_key).await;
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
                                            info!(
                                                "Receive update master node event with key: {:?}",
                                                key
                                            );

                                            // deserialize master node to Node
                                            let updated_master_node = match item_value {
                                                ValueType::Json(master_node_json) => {
                                                    let updated_master_node: Node =
                                                        serde_json::from_value(
                                                            master_node_json.to_owned(),
                                                        )
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
                                            self.campaign_master(node.clone()).await;

                                            // Serve as master node
                                            // self.watch_nodes(nodes.clone(), ring.clone());
                                            let nodes_self = nodes.clone();
                                            let ring_self = ring.clone();
                                            let self_clone = self.clone();
                                            tokio::task::spawn(async move {
                                                self_clone
                                                    .watch_nodes(nodes_self, ring_self)
                                                    .await
                                                    .unwrap();
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
                }
                Err(e) => {
                    error!("Failed to get master node: {}", e);
                }
            }
        }
    }

    /// Save ring to etcd
    pub async fn save_ring(&self, ring: &Ring<Node>) -> DatenLordResult<()> {
        // Only master node can save ring to etcd
        // So we do not need to lock the ring
        let key = &KeyType::CacheRing;
        debug!("Save ring to etcd: {}", key);
        self.kv_engine
            .set(key, &ValueType::Json(serde_json::to_value(ring)?), None)
            .await?;

        Ok(())
    }

    /// Load ring from etcd
    pub async fn load_ring(&self) -> DatenLordResult<Option<Ring<Node>>> {
        let key = &KeyType::CacheRing;
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
        storage::cache_proxy::{
            cluster_manager::ClusterManager,
            node::{Node, NodeStatus},
        },
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
        let master_cluster_informer = ClusterManager::new(client.clone());
        let test_slave_node_1 = create_node("192.168.1.3");
        let slave_1_cluster_informer = ClusterManager::new(client.clone());
        let test_slave_node_2 = create_node("192.168.1.4");
        let slave_2_cluster_informer = ClusterManager::new(client.clone());

        info!("test_single_master_election: start to test single master election");

        let (master_res, slave_1_res, slave_2_res) = tokio::join!(
            async {
                // Register node
                let _ = master_cluster_informer
                    .register(test_master_node.clone())
                    .await;
                // Register master
                master_cluster_informer
                    .campaign_master(test_master_node.clone())
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
        assert_eq!(test_master_node.read().status(), NodeStatus::Master);
        assert_eq!(test_slave_node_1.read().status(), NodeStatus::Slave);
        assert_eq!(test_slave_node_2.read().status(), NodeStatus::Slave);
    }
}
