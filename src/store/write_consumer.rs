
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time;

use crate::custom_err::CustomResult;
use crate::http_param::DataItem;
use crate::index::DataPosition;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;
use crate::store::get_file_name;

/// 启动写入消费者
pub fn start_write_consumer(workspace: String, max_file_id: u32,
                            mut recv: Receiver<DataItem>,
                            index: DynamicParallelIndexWrapper) {
    tokio::spawn(async move {
        log::info!("写入消费者已启动!");
        let mut data_file = WriteableFile::new(max_file_id, &workspace).await.unwrap();

        loop {
            let batch_size = 100;
            let mut vec = Vec::new();

            for _ in 0..batch_size {
                match recv.try_recv() {
                    Ok(event) => {
                        vec.push(event);
                    }
                    Err(err) => {
                        match err {
                            TryRecvError::Empty => {
                                break;
                            }
                            TryRecvError::Disconnected => {
                                log::warn!("write channel 关闭，写线程结束！");
                                return;
                            }
                        }
                    }
                }
            }
            log::info!("开始处理写入，size={}", vec.len());
            if vec.is_empty() {
                // 说明当前写入请求不多，休眠一段时间
                time::sleep(time::Duration::from_millis(100)).await
            } else {
                data_file.append(vec, &index).await;
            }
        }
    });
}


struct WriteableFile {
    // 文件编号
    id: u32,
    // 数据文件指针
    file: File,
    // 当前写入了多少数据
    offset: u32,
}

impl WriteableFile {
    async fn new(id: u32, dir: &String) -> CustomResult<WriteableFile> {

        let file_name = get_file_name(id, dir);
        log::info!("打开或创建可写入文件:{}",file_name);
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .await?;
        Ok(WriteableFile {
            id,
            file: f,
            offset: 0,
        })
    }

    /// 执行写入,并且更新索引
    async fn append(&mut self, events: Vec<DataItem>, index: &DynamicParallelIndexWrapper) -> CustomResult<()> {
        for data in events {
            let json = serde_json::to_vec(&data)?;
            let len = json.len() as u32;
            self.file.write(&json).await?;

            index.push(&data.key, DataPosition::new(self.id, self.offset, len)).await;

            self.offset = self.offset + len;
        }
        self.file.sync_data().await?;
        Ok(())
    }
}
