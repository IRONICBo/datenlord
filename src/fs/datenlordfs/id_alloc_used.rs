//! id allocator for inum and fd

use std::sync::Arc;

use super::id_alloc::{DistIdAllocator, IdType};
use crate::common::error::DatenLordResult;
use crate::fs::fs_util::INum;
use crate::fs::kv_engine::KVEngine;

/// Inum allocator
#[derive(Debug)]
pub struct INumAllocator<K: KVEngine + 'static> {
    /// id allocator
    id_allocator: DistIdAllocator<K>,
}

impl<K: KVEngine + 'static> INumAllocator<K> {
    /// new `INumAllocator`
    pub fn new(kv_engine: Arc<K>) -> Self {
        Self {
            id_allocator: DistIdAllocator::new(kv_engine, IdType::INum, 2),
        }
    }

    /// get a unique inum for a new file
    /// return inum
    pub async fn alloc_inum_for_fnode(&self) -> DatenLordResult<INum> {
        self.id_allocator.alloc_id().await
    }
}
