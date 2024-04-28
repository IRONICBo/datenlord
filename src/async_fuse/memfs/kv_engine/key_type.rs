use std::fmt::{self, Display, Formatter, Write};

use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::id_alloc::IdType;

/// The `KeyType` is used to locate the value in the distributed K/V storage.
/// Every key is prefixed with a string to indicate the type of the value.
/// If you want to add a new type of value, you need to add a new variant to the
/// enum. And you need to add a new match arm to the `get_key` function , make
/// sure the key is unique.
#[derive(Debug, Eq, PartialEq)]
pub enum KeyType {
    /// INum -> SerailNode
    INum2Node(INum),
    /// (paretn_id,child_name) -> DirEntry
    DirEntryKey((INum, String)),
    /// IdAllocator value key
    IdAllocatorValue(IdType),
    /// Node ip and port info : node_id -> "{node_ipaddr}:{port}"
    /// The corresponding value type is ValueType::String
    NodeIpPort(String),
    /// Volume information
    /// The corresponding value type is ValueType::RawData
    VolumeInfo(String),
    /// Node list
    /// The corresponding value type is ValueType::RawData
    FileNodeList(INum),
    /// Just a string key for testing the KVEngine.
    #[cfg(test)]
    String(String),
    /// Distributed cache node key
    CacheNode(String),
    /// Distributed hash ring key
    CacheRing,
    /// Distributed cache node master key
    CacheNodeMaster,
}

// ::<KeyType>::get() -> ValueType
/// Lock key type the memfs used.
#[derive(Debug, Eq, PartialEq)]
#[allow(variant_size_differences)]
pub enum LockKeyType {
    /// IdAllocator lock key
    IdAllocatorLock(IdType),
    /// ETCD volume information lock
    VolumeInfoLock,
    /// ETCD file node list lock
    FileNodeListLock(INum),
    /// Distributed cache node master key
    CacheNodeMaster,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            KeyType::INum2Node(ref inum) => write!(f, "INum2Node({inum})"),
            KeyType::DirEntryKey((ref parent_id, ref child_name)) => {
                write!(f, "DirEntryKey(({parent_id}, {child_name}))")
            }
            KeyType::IdAllocatorValue(ref id_type) => write!(f, "IdAllocatorValue({id_type})"),
            KeyType::NodeIpPort(ref s) => write!(f, "NodeIpPort({s})"),
            KeyType::VolumeInfo(ref s) => write!(f, "VolumeInfo({s})"),
            KeyType::FileNodeList(ref inum) => write!(f, "FileNodeList({inum})"),
            #[cfg(test)]
            KeyType::String(ref s) => write!(f, "String({s})"),
            KeyType::CacheNode(ref s) => write!(f, "CacheNode({s})"),
            KeyType::CacheRing => write!(f, "CacheRing/"), // CacheRing
            KeyType::CacheNodeMaster => write!(f, "CacheNodeMaster"), // CacheNodeMaster
        }
    }
}

impl Display for LockKeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            LockKeyType::IdAllocatorLock(ref id_type) => {
                write!(f, "IdAllocatorLock({id_type:?})")
            }
            LockKeyType::VolumeInfoLock => {
                write!(f, "VolumeInfoLock")
            }
            LockKeyType::FileNodeListLock(ref inum) => {
                write!(f, "FileNodeListLock({inum:?})")
            }
            LockKeyType::CacheNodeMaster => {
                write!(f, "CacheNodeMaster")
            }
        }
    }
}

impl KeyType {
    /// Creates a string representation of the key.
    /// This method concatenates the key's prefix with its formatted key value,
    #[must_use]
    pub fn to_string_key(&self) -> String {
        let mut result = String::new();
        let prefix = self.prefix();
        write!(&mut result, "{prefix}").unwrap();
        self.write_key(&mut result);
        result
    }

    /// Retrieves the specific prefix associated with the key type
    #[must_use]
    pub fn prefix(&self) -> &str {
        match *self {
            KeyType::INum2Node(_) => "I",
            KeyType::DirEntryKey(_) => "D",
            #[cfg(test)]
            KeyType::String(_) => "TEST_",
            KeyType::IdAllocatorValue(_) => "IdAlloc",
            KeyType::NodeIpPort(_) => "NodeIpPort",
            KeyType::VolumeInfo(_) => "VolumeInfo",
            KeyType::FileNodeList(_) => "FileNodeList",
            KeyType::CacheNode(_) => "CacheNode",
            KeyType::CacheRing => "CacheRing",
            KeyType::CacheNodeMaster => "CacheNodeMaster",
        }
    }

    /// Appends the unique part of the key to a mutable string buffer.
    fn write_key(&self, f: &mut String) {
        match *self {
            KeyType::INum2Node(ref inum) => {
                write!(f, "{inum}").unwrap();
            }
            KeyType::DirEntryKey((ref parent_id, ref child_name)) => {
                write!(f, "{parent_id}_{child_name}").unwrap();
            }
            #[cfg(test)]
            KeyType::String(ref s) => {
                write!(f, "{s}").unwrap();
            }
            KeyType::IdAllocatorValue(ref id_type) => {
                write!(f, "{id_type}").unwrap();
            }
            KeyType::NodeIpPort(ref s) => {
                write!(f, "{s}").unwrap();
            }
            KeyType::VolumeInfo(ref s) => {
                write!(f, "{s}").unwrap();
            }
            KeyType::FileNodeList(ref inum) => {
                write!(f, "{inum}").unwrap();
            }
            KeyType::CacheNode(ref s) => {
                write!(f, "{s}").unwrap();
            }
            KeyType::CacheRing => {
                write!(f, "").unwrap();
            }
            KeyType::CacheNodeMaster => {
                write!(f, "").unwrap();
            }
        }
    }
}

impl LockKeyType {
    /// Creates a string representation of the key.
    /// This method concatenates the key's prefix with its formatted key value,
    #[must_use]
    pub fn to_string_key(&self) -> String {
        let mut result = String::new();
        let prefix = self.prefix();
        write!(&mut result, "{prefix}").unwrap();
        self.write_key(&mut result);
        result
    }

    /// Retrieves the specific prefix associated with the lock key type.
    #[must_use]
    fn prefix(&self) -> &str {
        match *self {
            LockKeyType::IdAllocatorLock(_) => "IdAllocLock",
            LockKeyType::VolumeInfoLock => "VolumeInfoLock",
            LockKeyType::FileNodeListLock(_) => "FileNodeListLock",
            LockKeyType::CacheNodeMaster => "CacheNodeMaster",
        }
    }

    /// Appends the unique part of the key to a mutable string buffer.
    fn write_key(&self, f: &mut String) {
        match *self {
            LockKeyType::IdAllocatorLock(ref id_type) => {
                write!(f, "{id_type}").unwrap();
            }
            LockKeyType::VolumeInfoLock => {
                // No additional data is appended for VolumeInfoLock
            }
            LockKeyType::FileNodeListLock(ref inum) => {
                write!(f, "{inum}").unwrap();
            }
            LockKeyType::CacheNodeMaster => {
                write!(f, "").unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IdType, KeyType, LockKeyType};

    #[test]
    fn test_inum2node_key() {
        let key = KeyType::INum2Node(123);
        assert_eq!(key.to_string_key(), "I123", "INum2Node key mismatch");
    }

    #[test]
    fn test_direntry_key() {
        let key = KeyType::DirEntryKey((456, "child".to_owned()));
        assert_eq!(key.to_string_key(), "D456_child", "DirEntry key mismatch");
    }

    #[test]
    fn test_idallocatorvalue_key() {
        let key = KeyType::IdAllocatorValue(IdType::INum);
        assert_eq!(
            key.to_string_key(),
            "IdAllocINum",
            "IdAllocatorValue key mismatch"
        );
    }

    #[test]
    fn test_nodeipport_key() {
        let key = KeyType::NodeIpPort("127.0.0.1:8080".to_owned());
        assert_eq!(
            key.to_string_key(),
            "NodeIpPort127.0.0.1:8080",
            "NodeIpPort key mismatch"
        );
    }

    #[test]
    fn test_volumeinfo_key() {
        let key = KeyType::VolumeInfo("volume1".to_owned());
        assert_eq!(
            key.to_string_key(),
            "VolumeInfovolume1",
            "VolumeInfo key mismatch"
        );
    }

    #[test]
    fn test_filenodelist_key() {
        let key = KeyType::FileNodeList(789);
        assert_eq!(
            key.to_string_key(),
            "FileNodeList789",
            "FileNodeList key mismatch"
        );
    }

    #[cfg(test)]
    #[test]
    fn test_string_key() {
        let key = KeyType::String("test".to_owned());
        assert_eq!(key.to_string_key(), "TEST_test", "String key mismatch");
    }

    #[test]
    fn test_idallocatorlock_key() {
        let key = LockKeyType::IdAllocatorLock(IdType::INum);
        assert_eq!(
            key.to_string_key(),
            "IdAllocLockINum",
            "IdAllocatorLock key mismatch"
        );
    }

    #[test]
    fn test_volumeinfolock_key() {
        let key = LockKeyType::VolumeInfoLock;
        assert_eq!(
            key.to_string_key(),
            "VolumeInfoLock",
            "VolumeInfoLock key mismatch"
        );
    }

    #[test]
    fn test_filenodelistlock_key() {
        let key = LockKeyType::FileNodeListLock(123);
        assert_eq!(
            key.to_string_key(),
            "FileNodeListLock123",
            "FileNodeListLock key mismatch"
        );
    }
}
