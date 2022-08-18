use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::RwLock;

use crate::calc_hash;
use crate::index::DataPosition;
use crate::index::linked_hash_set::LinkedHashSet;

/// 并行索引
#[derive(Debug)]
pub struct ParallelIndex {
    size: AtomicU64,
    parallel: u64,
    table: Vec<RwLock<LinkedHashSet>>,
}

impl ParallelIndex {
    /// 创建索引
    pub fn new(parallel: u64) -> ParallelIndex {
        let mut table = Vec::with_capacity(parallel as usize);

        for _ in 0..parallel {
            table.push(RwLock::new(LinkedHashSet::new()));
        }

        ParallelIndex {
            size: AtomicU64::new(0),
            parallel,
            table,
        }
    }


    /// 插入数据，
    /// 当插入成功时，返回true，
    /// 如果底层的linked_hash_set被移动，导致无法插入，返回false
    pub async fn push(&self, key: &String, dp: DataPosition) -> bool {
        let hash = calc_hash(key);
        let vec_i = hash % self.parallel;
        let mut set = self.get_link(vec_i).write().await;
        if set.is_moved() {
            return false;
        }
        self.size.fetch_add(set.push(key, dp) as u64, Ordering::SeqCst);
        true
    }

    /// 查找数据
    /// 第一个返回值表示 数据是否被移动
    pub async fn find(&self, key: &String) -> (bool, Option<DataPosition>) {
        let hash = calc_hash(key);
        let vec_i = hash % self.parallel;
        let set = self.get_link(vec_i).read().await;
        if set.is_moved() {
            return (false, None);
        }
        (true, set.find(key))
    }

    pub async fn del(&self, key: &String) -> bool {
        let hash = calc_hash(key);
        let vec_i = hash % self.parallel;
        let mut set = self.get_link(vec_i).write().await;
        if set.is_moved() {
            return false;
        }

        self.size.fetch_sub(set.del(key) as u64, Ordering::SeqCst);
        true
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst)
    }

    /// 当前并行度
    pub fn get_parallel(&self) -> u64 {
        self.parallel
    }

    pub fn get_link(&self, index: u64) -> &RwLock<LinkedHashSet> {
        self.table.get(index as usize).unwrap()
    }
}


#[cfg(test)]
mod tests {
    use crate::index::DataPosition;
    use crate::index::parallel_index::ParallelIndex;

    #[test]
    pub fn test_parallel_index() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let index = ParallelIndex::new(8);
            assert_eq!(index.push(&String::from("1"), DataPosition::new(1, 1, 1)).await, true);
            assert_eq!(index.push(&String::from("2"), DataPosition::new(2, 2, 2)).await, true);
            assert_eq!(index.push(&String::from("1"), DataPosition::new(3, 3, 3)).await, true);
            assert_eq!(index.push(&String::from("3"), DataPosition::new(4, 4, 4)).await, true);

            assert_eq!(index.find(&String::from("1")).await, (true, Some(DataPosition::new(3, 3, 3))));
            assert_eq!(index.del(&String::from("3")).await, true);
            assert_eq!(index.find(&String::from("3")).await, (true, None));

            assert_eq!(index.size(), 2);

            println!("hash_set:{:?}", index)
        });
    }
}