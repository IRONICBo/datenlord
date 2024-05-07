use std::str::FromStr;

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
            status: NodeStatus::Initializing,
        }
    }

    /// Create a new node
    pub fn new(ip: String, port: u16, weight: u32, status: NodeStatus) -> Self {
        Self {
            ip,
            port,
            weight,
            status,
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

    /// Change the status of the node
    /// We export this function to change the status of the node,
    /// The state machine is managed by the cluster manager
    pub fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }

    /// Get self clone
    pub fn dump(&self) -> Self {
        Self {
            ip: self.ip.clone(),
            port: self.port,
            weight: self.weight,
            status: self.status.clone(),
        }
    }
}

/// Node status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// The node is preparing
    Initializing,
    /// The node is registering
    Registering,
    /// The node is serve as slave
    Slave,
    /// The node is serve as master
    Master,
}

impl FromStr for NodeStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "initializing" => Ok(NodeStatus::Initializing),
            "registering" => Ok(NodeStatus::Registering),
            "slave" => Ok(NodeStatus::Slave),
            "master" => Ok(NodeStatus::Master),
            _ => Err(format!("Unknown node status: {}", s)),
        }
    }
}
