use std::convert::AsRef;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64};

use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::index::{calc_hash, Node};

pub struct Head {
    is_move: bool,
    first_node: Option<Arc<Node>>,
}

impl Head {
    pub fn new() -> Head {
        Head {
            is_move: false,
            first_node: None,
        }
    }

    pub fn add(&mut self, hash: u64, file_id: u32, offset: u32, length: u32) {
        if let Some(next) = &self.first_node {
            let next = next.clone();

            let node = Some(Arc::new(Node {
                hash,
                file_id,
                offset,
                length,
                next_node: Some(next.clone()),
            }));
            self.first_node = node;
        } else {
            self.first_node = Some(Arc::new(Node {
                hash,
                file_id,
                offset,
                length,
                next_node: None,
            }));
        }
    }
}


pub struct ConcurrentHashIndex {
    // 最大容量
    capacity: u64,
    // 当前数据量
    size: AtomicU64,
    // 索引节点
    data: Vec<RwLock<Head>>,
}

impl ConcurrentHashIndex {
    pub fn new() -> ConcurrentHashIndex {
        let parallel = 1024;
        let mut data = Vec::with_capacity(parallel);
        for i in 0..parallel {
            data.push(RwLock::new(Head::new()));
        }

        ConcurrentHashIndex {
            capacity: u64::MAX,
            size: AtomicU64::new(0),
            data,
        }
    }

    pub async fn put(&self, key: &String, file_id: u32, offset: u32, length: u32) {
        let hash = calc_hash(key);
        let index = hash % (self.data.len() as u64);

        let head = self.data.get(index as usize).unwrap();
        let mut head_guard = head.write().await;
        head_guard.add(hash, file_id, offset, length);
    }

    fn get(&self, key: &String) -> Node {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::index::concurrent_hash_index::ConcurrentHashIndex;

    #[test]
    pub fn test_create() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let index = Arc::new(ConcurrentHashIndex::new());
        });
    }
}