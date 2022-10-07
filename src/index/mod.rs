mod linked_hash_set;
mod parallel_index;
pub mod dynamic_index;

use serde::{Deserialize, Serialize};

/// 数据的位置
#[derive(Debug, Clone, PartialEq,Serialize, Deserialize)]
pub struct DataPosition {
    // 文件id
    pub file_id: u32,
    // 偏移量
    pub offset: u32,
    // value大小
    pub length: u32,
}

impl DataPosition {
    pub fn new(file_id: u32, offset: u32, length: u32) -> Self {
        DataPosition {
            file_id,
            offset,
            length,
        }
    }
}

type Link = Option<Box<Node>>;

/// 索引节点
#[derive(Debug)]
pub struct Node {
    key: String,
    dp: DataPosition,
    next: Link,
}

impl Node {
    pub fn update_dp(&mut self, dp: DataPosition) {
        self.dp = dp;
    }
}

