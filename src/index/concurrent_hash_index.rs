use std::sync::atomic::AtomicU64;

use crate::index::{Node, Index};

pub struct ConcurrentHashIndex {
    // 最大容量
    capacity: u64,
    // 当前数据量
    size: AtomicU64,
    // 并行度
    parallel: AtomicU64,
    // 索引节点
    data: Vec<Node>,
}

impl ConcurrentHashIndex {
    pub fn new() -> ConcurrentHashIndex {
        ConcurrentHashIndex {
            capacity: u64::MAX,
            size: AtomicU64::new(0),
            parallel: AtomicU64::new(1024),
            data: vec![],
        }
    }
}

impl Index for ConcurrentHashIndex {
    fn put(key: &String) {
        todo!()
    }

    fn get(key: &String) -> Node {
        todo!()
    }
}