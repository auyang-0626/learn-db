use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

mod concurrent_hash_index;

/// 索引中的key，注意，
pub struct Node {
    // key的hash值
    hash: u64,
    // 文件id
    file_id: u32,
    // 偏移量
    offset: u32,
    // value大小
    length: u32,
    // 下一个node
    next_node: Option<Arc<Node>>,
}

/// 计算hash
pub fn calc_hash(key: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}