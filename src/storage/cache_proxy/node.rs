use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use super::ring::NodeType;

/// Physical node struct
///
/// physical node is the node in the slot mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// We assume that ip is unique in our system
    /// TODO: Use hash set to judge the uniqueness?
    /// The ip of the node
    ip: String,
    /// The port of the node
    port: u16,
    /// The weight of the node
    weight: u32,
    /// The status of the node
    status: NodeStatus,
    /// The role of the node
    role: NodeRole,
}

impl NodeType for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port && self.weight == other.weight
    }
}

impl Eq for Node {}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.port.hash(state);
        self.weight.hash(state);
    }
}

impl Node {
    /// Default of the node
    pub fn default() -> Self {
        Self {
            ip: String::new(),
            port: 0,
            weight: 0,
            status: NodeStatus::Prepare,
            role: NodeRole::Slave,
        }
    }

    /// Create a new node
    pub fn new(ip: String, port: u16, weight: u32, status: NodeStatus, role: NodeRole) -> Self {
        Self {
            ip,
            port,
            weight,
            status,
            role,
        }
    }

    /// Get the ip of the node
    pub fn ip(&self) -> &str {
        &self.ip
    }

    /// Get the port of the node
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the weight of the node
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// Set the ip of the node
    pub fn set_ip(&mut self, ip: String) {
        self.ip = ip;
    }

    /// Set the port of the node
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    /// Set the weight of the node
    pub fn set_weight(&mut self, weight: u32) {
        self.weight = weight;
    }

    /// Get the status of the node
    pub fn status(&self) -> NodeStatus {
        self.status.clone()
    }

    /// Get the role of the node
    pub fn role(&self) -> NodeRole {
        self.role.clone()
    }

    /// Get self clone
    pub fn dump(&self) -> Self {
        Self {
            ip: self.ip.clone(),
            port: self.port,
            weight: self.weight,
            status: self.status.clone(),
            role: self.role.clone(),
        }
    }

    /// Set online
    pub fn set_online(&mut self) {
        self.status = NodeStatus::Online;
    }

    /// Set offline
    pub fn set_offline(&mut self) {
        self.status = NodeStatus::Offline;
    }

    /// Set prepare
    pub fn set_prepare(&mut self) {
        self.status = NodeStatus::Prepare;
    }

    /// Set master
    pub fn set_master(&mut self) {
        self.role = NodeRole::Master;
    }

    /// Set slave
    pub fn set_slave(&mut self) {
        self.role = NodeRole::Slave;
    }
}

/// Node status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// The node is online
    Online,
    /// The node is offline
    Offline,
    /// The node is preparing
    Prepare,
}

impl FromStr for NodeStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "online" => Ok(NodeStatus::Online),
            "offline" => Ok(NodeStatus::Offline),
            "prepare" => Ok(NodeStatus::Prepare),
            _ => Err(format!("Unknown node status: {}", s)),
        }
    }
}

/// Node role
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeRole {
    /// The node is master
    Master,
    /// The node is slave
    Slave,
}

impl ToString for NodeRole {
    fn to_string(&self) -> String {
        match self {
            NodeRole::Master => "master".to_string(),
            NodeRole::Slave => "slave".to_string(),
        }
    }
}

impl FromStr for NodeRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "master" => Ok(NodeRole::Master),
            "slave" => Ok(NodeRole::Slave),
            _ => Err(format!("Unknown node role: {}", s)),
        }
    }
}

/// Node list
///
/// Node list is used to manage the physical nodes
#[derive(Debug)]
pub struct NodeList {
    inner: Arc<Mutex<Vec<Node>>>,
}

impl NodeList {
    /// Create a new node list
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add a node to the list
    pub fn add(&self, node: Node) {
        self.inner.lock().unwrap().push(node);
    }

    /// Get the node list
    pub fn list(&self) -> Vec<Node> {
        self.inner.lock().unwrap().clone()
    }

    /// Remove a node from the list by ip
    pub fn remove(&self, ip: &str) {
        let mut list = self.inner.lock().unwrap();
        list.retain(|node| node.ip() != ip);
    }

    /// Get the node by ip
    pub fn get(&self, ip: &str) -> Option<Node> {
        let list = match self.inner.lock() {
            Ok(lock) => lock,
            Err(_) => return None,
        };
        list.iter().find(|node| node.ip() == ip).cloned()
    }
}
