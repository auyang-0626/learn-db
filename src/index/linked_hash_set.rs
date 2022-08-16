use crate::index::{DataPosition, Link, Node};

/// hash链表
#[derive(Debug)]
pub struct LinkedHashSet {
    head: Link,
    moved: bool,
}

impl LinkedHashSet {
    pub fn new() -> Self {
        LinkedHashSet {
            head: None,
            moved: false,
        }
    }

    /// 插入或更新，如果存在hash相同的，更新，不存在则插入
    /// 返回 1 表示插入 0 表示更新
    pub fn push(&mut self, hash: u64, dp: DataPosition) -> u8 {
        let mut node = &mut self.head;
        // 如果找到，就更新
        while let Some(v) = node {
            if v.hash == hash {
                v.update_dp(dp);
                return 0;
            }
            node = &mut v.next;
        }
        // 没有找到，放在队列头部
        let head = Node {
            hash,
            dp,
            next: self.head.take(),
        };
        self.head = Some(Box::new(head));
        1
    }

    /// 移除第一个节点
    pub fn pop(&mut self) -> Option<Box<Node>> {
        match self.head.take() {
            None => None,
            Some(mut v) => {
                self.head = v.next.take();
                Some(v)
            }
        }
    }

    /// 根据hash返回数据的位置信息
    pub fn find(&self, hash: u64) -> Option<DataPosition> {
        let mut node = &self.head;
        // 如果找到，就更新
        while let Some(v) = node {
            if v.hash == hash {
                return Some(v.dp.clone());
            }
            node = &v.next;
        }
        None
    }

    /// 根据hash删除指定节点
    pub fn del(&mut self, hash: u64) -> u8 {
        let mut node = &mut self.head;
        if let Some(v) = node {
            if v.hash == hash {
                self.head = v.next.take();
                return 1;
            }
        }

        while let Some(v) = node {
            if let Some(next) = &mut v.next {
                if next.hash == hash {
                    v.next = next.next.take();
                    return 1;
                }
            }
            node = &mut v.next;
        }
        0
    }

    pub fn is_moved(&self) -> bool {
        self.moved
    }

    pub fn set_moved(&mut self, moved: bool) {
        self.moved = moved
    }
}


#[cfg(test)]
mod tests {
    use crate::index::DataPosition;
    use crate::index::linked_hash_set::LinkedHashSet;

    #[test]
    pub fn test_linked_hash_set() {
        let mut hash_set = LinkedHashSet::new();
        hash_set.push(1, DataPosition::new(1, 2, 3));
        hash_set.push(2, DataPosition::new(1, 2, 3));
        hash_set.push(1, DataPosition::new(1, 3, 3));
        hash_set.push(3, DataPosition::new(1, 2, 3));
        assert_eq!(hash_set.find(1), Some(DataPosition::new(1, 3, 3)));
        hash_set.del(2);
        assert_eq!(hash_set.find(2), None);
        println!("hash_set:{:?}", hash_set)
    }
}