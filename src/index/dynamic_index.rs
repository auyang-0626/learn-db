use std::cmp::min;
use std::sync::Arc;

use log::info;
use tokio::sync::RwLock;
use tokio::time;

use crate::index::{DataPosition, Node};
use crate::index::parallel_index::ParallelIndex;

struct DynamicParallelIndex {
    // 真实的索引数据
    parallel_index: ParallelIndex,
    // 移动完成后，通知等待的线程
    // 扩缩容时，放新的索引
    new_parallel_index: Option<ParallelIndex>,
}


/// 动态扩缩容的索引结构
/// 扩容：当 DynamicParallelIndex 的size 超过阙值时，就会触发扩容
///   移动期间，并不会阻塞其它线程的插入和查询
#[derive(Clone)]
pub struct DynamicParallelIndexWrapper {
    // 只有扩缩容期间，才会申请写锁，其它不管是读取还是写入数据，都申请读锁，所以不用担心这里的并行度
    inner: Arc<RwLock<DynamicParallelIndex>>,
}

impl DynamicParallelIndexWrapper {
    pub fn new(parallel: u64) -> DynamicParallelIndexWrapper {
        let wrapper = DynamicParallelIndexWrapper {
            inner: Arc::new(RwLock::new(DynamicParallelIndex {
                parallel_index: ParallelIndex::new(parallel),
                new_parallel_index: None,
            }))
        };
        // 启动自动扩缩容的定时任务
        DynamicParallelIndexWrapper::start_dynamic_capacity(wrapper.clone());
        wrapper
    }

    pub async fn push(&self, key: &String, dp: DataPosition) {
        let push_function = |inner: Arc<RwLock<DynamicParallelIndex>>, dp: DataPosition| async move {
            let index_gurad = inner.read().await;
            let success = index_gurad.parallel_index.push(key, dp.clone()).await;
            if success {
                return success;
            } else {
                info!("当前index正在扩缩容，原数据已被移动，正在去新索引查找....");
                // 要释放读锁，不然扩缩容那里无法获取写锁
                match &index_gurad.new_parallel_index {
                    None => {
                        // 这种情况，扩缩容正好完成，所以找不到新的索引了，直接重试就可以
                        return false;
                    }
                    Some(p) => {
                        return p.push(key, dp).await;
                    }
                }
            }
        };

        while !push_function(self.inner.clone(), dp.clone()).await {}
    }

    pub async fn del(&self, key: &String) {
        let del_function = |inner: Arc<RwLock<DynamicParallelIndex>>| async move {
            let index_gurad = inner.read().await;
            let success = index_gurad.parallel_index.del(key).await;
            if success {
                return success;
            } else {
                info!("当前index正在扩缩容，原数据已被移动，正在去新索引查找....");
                // 要释放读锁，不然扩缩容那里无法获取写锁
                match &index_gurad.new_parallel_index {
                    None => {
                        // 这种情况，扩缩容正好完成，所以找不到新的索引了，直接重试就可以
                        return false;
                    }
                    Some(p) => {
                        return p.del(key).await;
                    }
                }
            }
        };

        while !del_function(self.inner.clone()).await {}
    }

    pub async fn find(&self, key: &String) -> Option<DataPosition> {
        let find_function = |inner: Arc<RwLock<DynamicParallelIndex>>| async move {
            let index_gurad = inner.read().await;
            let (success, res) = index_gurad.parallel_index.find(key).await;
            if success {
                return (success, res);
            } else {
                info!("当前index正在扩缩容，原数据已被移动，正在去新索引查找....");
                // 要释放读锁，不然扩缩容那里无法获取写锁
                match &index_gurad.new_parallel_index {
                    None => {
                        // 这种情况，扩缩容正好完成，所以找不到新的索引了，直接重试就可以
                        return (false, None);
                    }
                    Some(p) => {
                        return p.find(key).await;
                    }
                }
            }
        };
        loop {
            let (success, res) = find_function(self.inner.clone()).await;
            if success {
                return res;
            }
        }
    }

    pub async fn size(&self) -> u64 {
        let inner = self.inner.read().await;
        inner.parallel_index.size()
    }

    /// 定时任务，检查是否需要扩缩容
    pub fn start_dynamic_capacity(wrapper_clone: DynamicParallelIndexWrapper) {
        // let wrapper_clone = wrapper.clone();
        // 定时查看，是否需要扩缩容
        tokio::spawn(async move {
            let mut interval = time::interval(time::Duration::from_secs(2));

            loop {
                interval.tick().await;

                // 返回true，表示需要调整容量
                if wrapper_clone.dynamic_capacity_check().await {
                    //todo 根据cpu设置
                    let thread_size = 8 as u64;
                    let mut threads = Vec::new();


                    for i in 0..thread_size {
                        let wrapper_clone = wrapper_clone.clone();
                        threads.push(tokio::spawn(async move {
                            let inner_gurad = wrapper_clone.inner.read().await;

                            if let Some(new_index) = &inner_gurad.new_parallel_index {
                                for j in 0..inner_gurad.parallel_index.get_parallel() {
                                    if j % thread_size == i {
                                        let linked_lock = inner_gurad.parallel_index.get_link(j);
                                        let mut set = linked_lock.write().await;


                                        if !set.is_moved() {
                                            while let Some(node) = set.pop() {
                                                let Node { key, dp, .. } = *node;
                                                new_index.push(&key, dp).await;
                                            }
                                            // 每移动完一个 linked_hash_set，就标记为已经移动
                                            set.set_moved(true);
                                        }
                                    }
                                }
                            }
                            i
                        }));
                    }

                    for handle in threads {
                        let id = handle.await;
                        info!("handle[{:?}]完成！", id);
                    }

                    let mut mut_inner = wrapper_clone.inner.write().await;
                    mut_inner.parallel_index = mut_inner.new_parallel_index.take().unwrap();

                    info!("扩容完成！");
                }
            }
        });
    }

    /// 检查容量是否健康
    /// 返回true 表示需要调整容量，并且会创建新的index，更新 self.new_parallel_index
    /// 返回false 表示不需要
    pub async fn dynamic_capacity_check(&self) -> bool {
        let inner_guard = self.inner.read().await;

        let curr_size = inner_guard.parallel_index.size();

        let rate = curr_size / inner_guard.parallel_index.get_parallel();
        if rate > 8 && curr_size < u32::MAX as u64 {
            drop(inner_guard);

            let new_size = min(curr_size * 8, u32::MAX as u64);
            info!("curr_size={},new_size={}", curr_size, new_size);

            let new_index = ParallelIndex::new(new_size);
            let mut inner_guard_mut = self.inner.write().await;
            inner_guard_mut.new_parallel_index = Some(new_index);
            true
        } else {
            // info!("curr_size={},parallel={},无需扩容！", curr_size, inner_guard.parallel_index.get_parallel());
            false
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::index::DataPosition;
    use crate::index::dynamic_index::DynamicParallelIndexWrapper;
    use crate::init_log;

    #[tokio::test]
    async fn test_new() {
        let index = DynamicParallelIndexWrapper::new(8);
        init_log();

        for i in 0..1024 {
            index.push(&i.to_string(), DataPosition::new(i as u32, i as u32, i as u32)).await;
        }
        assert_eq!(index.size().await, 1024);

        assert_eq!(index.find(&String::from("1")).await, Some(DataPosition::new(1, 1, 1)));
        assert_eq!(index.find(&String::from("8")).await, Some(DataPosition::new(8, 8, 8)));
        assert_eq!(index.find(&String::from("80000")).await, None);

        index.del(&String::from("8")).await;
        assert_eq!(index.find(&String::from("8")).await, None);
        //  std::thread::sleep(std::time::Duration::from_secs(10));
    }

    #[test]
    pub fn test_atomic_bool() {
        let flag = AtomicBool::new(false);

        println!("{:?}", flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed));
        println!("{:?}", flag.compare_exchange(true, true, Ordering::SeqCst, Ordering::Relaxed));
        println!("{:?}", flag.compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed));
    }
}
