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
    next_node:Option<Node>,
}

/// 索引接口
pub trait Index {
    fn put(key: &String);
    fn get(key: &String) -> Node;
}