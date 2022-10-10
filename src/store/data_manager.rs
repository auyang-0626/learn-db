use std::borrow::Borrow;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use crate::custom_err::CustomResult;
use crate::http_param::DataItem;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;
use crate::store::read_by_dp;
use crate::store::write_consumer::{start_write_consumer, WriteEvent};
use crate::Config;

#[derive(Clone)]
pub struct DataManager {
    // 工作目录
    workspace: Arc<String>,
    // 写入生产者
    write_provider: Sender<WriteEvent>,
    // 读取索引
    index: DynamicParallelIndexWrapper,
}

impl DataManager {
    pub fn new(cnf: Config) -> DataManager {
        // todo 实际上这里需要读取磁盘，暂时忽略
        let max_file_id = 1 as u32;
        // todo 这个需要从文件中恢复
        let index = DynamicParallelIndexWrapper::new(8);

        let (send, mut recv) = mpsc::channel(10000);

        // 写入的异步线程
        start_write_consumer(cnf.clone(), max_file_id, recv, index.clone());

        DataManager {
            workspace: Arc::new(cnf.workspace),
            write_provider: send,
            index,
        }
    }

    pub async fn push(&self, event: WriteEvent) -> CustomResult<()> {
        self.write_provider.send(event).await?;
        Ok(())
    }

    pub async fn find(&self, key: &String) -> Option<String> {
        let dp = self.index.find(key).await?;

        read_by_dp(self.workspace.borrow(), &dp).await.ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::http_param::DataItem;
    use crate::{init_log, Config};
    use crate::store::data_manager::DataManager;
    use crate::store::write_consumer::WriteEvent;

    #[tokio::test]
    async fn test_new() {
        init_log();

        let config = Config::new(String::from("/Users/yang/logs/learn-db"));


        let dm = DataManager::new(config);


        for i in 0..10000 {
            dm.push(WriteEvent::new_simple_event(DataItem {
                key: format!("name_{}", i),
                value: format!("ygy_{}", i),
            })).await;
        }

        let res = dm.find(&format!("name_{}", 0)).await;
        log::info!("read res={:?}", res);


        //std::thread::sleep(std::time::Duration::from_secs(10));
    }
}