use std::borrow::Borrow;
use std::path::Path;
use std::result::Result::Ok;
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use crate::Config;
use crate::custom_err::CustomResult;
use crate::http_param::DataItem;
use crate::index::DataPosition;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;
use crate::store::{get_index_file_name, get_log_file_name, read_by_dp};
use crate::store::compression_task::{generate_index_file, scan_file_id_vec, start_compression_task};
use crate::store::write_consumer::{start_write_consumer, WriteEvent};

#[derive(Clone)]
pub struct DataManager {
    // 工作目录
    pub workspace: Arc<String>,
    // 写入生产者
    write_provider: Sender<WriteEvent>,
    // 读取索引
    index: DynamicParallelIndexWrapper,
}

impl DataManager {
    pub async fn new(cnf: Config) -> DataManager {
        let max_file_id = calc_max_file_id(&cnf.workspace);
        log::info!("最新file_id={}", max_file_id);

        let index = recover_index_from_disk(&cnf.workspace).await;

        let (send, mut recv) = mpsc::channel(10000);

        // 写入的异步线程
        start_write_consumer(cnf.clone(), max_file_id, recv, index.clone());

        let dm = DataManager {
            workspace: Arc::new(cnf.workspace.clone()),
            write_provider: send,
            index,
        };
        // 整理文件的定时任务
        start_compression_task(cnf.clone(),dm.clone());
        dm
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

/// 从磁盘中恢复索引
pub async fn recover_index_from_disk(workspace: &String) -> DynamicParallelIndexWrapper {
    log::info!("开始从磁盘恢复索引...");

    let index = DynamicParallelIndexWrapper::new(8);
    let file_id_vec = scan_file_id_vec(workspace);
    for file_id in file_id_vec {
        let index_file_name = get_index_file_name(file_id, workspace);
        let index_path = Path::new(&index_file_name);
        if !index_path.exists() {
            generate_index_file(
                Path::new(&get_log_file_name(file_id, workspace)),
                index_path,
            ).await.unwrap();
        }

        let mut index_file = File::open(index_path).await.unwrap();
        while let Ok((key, offset)) = read_index_from_file(&mut index_file).await {
            index.push(&key, DataPosition::new(file_id, offset)).await;
        }

        log::info!("索引文件{:?}恢复完成", index_path);
    }

    index
}

pub async fn read_index_from_file(index_file: &mut File) -> CustomResult<(String, u32)> {
    let key_len = index_file.read_u32().await?;
    let mut buffer = vec![Default::default(); key_len as usize];
    index_file.read(&mut buffer).await?;
    let offset = index_file.read_u32().await?;
    let key = String::from_utf8(buffer)?;
    Ok((key, offset))
}

pub fn calc_max_file_id(workspace: &String) -> u32 {
    let vec = scan_file_id_vec(workspace);
    if vec.is_empty() {
        1
    } else {
        vec.get(vec.len() - 1).unwrap() + 1
    }
}

#[cfg(test)]
mod tests {
    use crate::{Config, init_log};
    use crate::http_param::DataItem;
    use crate::store::data_manager::DataManager;
    use crate::store::write_consumer::WriteEvent;

    #[tokio::test]
    async fn test_new() {
        init_log();

        let config = Config::new(String::from("/Users/yang/logs/learn-db"));


        let dm = DataManager::new(config).await;


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