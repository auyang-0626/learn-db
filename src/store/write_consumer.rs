use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender as Callback;
use tokio::time;

use crate::custom_err::CustomResult;
use crate::http_param::DataItem;
use crate::index::DataPosition;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;
use crate::store::get_file_name;
use crate::Config;

/// 启动写入消费者
pub fn start_write_consumer(cnf: Config, max_file_id: u32,
                            mut recv: Receiver<WriteEvent>,
                            index: DynamicParallelIndexWrapper) {
    tokio::spawn(async move {
        log::info!("写入消费者已启动!");
        let mut data_file = WriteableFile::new(max_file_id, &cnf.workspace).await.unwrap();

        loop {
            // 文件超过最大尺寸时，切换写的新入点
            if data_file.offset > cnf.max_file_size {
                data_file = WriteableFile::new(data_file.id + 1, &cnf.workspace).await.unwrap();
            }

            // 批量写入，减少同步次数
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
            if vec.is_empty() {
                // 说明当前写入请求不多，休眠一段时间
                time::sleep(time::Duration::from_millis(100)).await
            } else {
                log::info!("开始处理写入，size={}", vec.len());

                data_file.append(vec, &index).await;
            }
        }
    });
}

/// 写入事件
pub struct WriteEvent {
    // 数据
    data_item: DataItem,
    // 如果传了该值，表示先比较，如果等于传入的，才会更新
    compare_dp: Option<DataPosition>,
    // 写入完成的回执
    callback: Option<Callback<()>>,
}

impl WriteEvent {
    pub fn new_simple_event(data_item: DataItem) -> WriteEvent {
        WriteEvent {
            data_item,
            compare_dp: None,
            callback: None,
        }
    }

    pub fn new_compare_event(data_item: DataItem, dp: DataPosition) -> WriteEvent {
        WriteEvent {
            data_item,
            compare_dp: Some(dp),
            callback: None,
        }
    }

    pub fn new_callback_event(data_item: DataItem, dp: Option<DataPosition>, callback: Callback<()>) -> WriteEvent {
        WriteEvent {
            data_item,
            compare_dp: dp,
            callback: Some(callback),
        }
    }
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
        log::info!("打开或创建可写入文件:{}", file_name);
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
    async fn append(&mut self, events: Vec<WriteEvent>, index: &DynamicParallelIndexWrapper) -> CustomResult<()> {
        // todo 这里先写buf，然后一次性写入文件性能会更好，后面再优化

        let mut callbacks = Vec::new();

        for event in events {
            let data = event.data_item;

            // 处理比较再写入的场景
            if let Some(dp) = event.compare_dp {
                match index.find(&data.key).await {
                    None => {
                        // 没有索引，说明数据被删除了，不需要写入了
                        return Ok(());
                    }
                    Some(i_dp) => {
                        if dp != i_dp {
                            // 说明数据已经更新了，也不需要再写入了
                            return Ok(());
                        }
                    }
                }
            }

            let json = serde_json::to_vec(&data)?;
            let len = json.len() as u32;
            self.file.write_u32(len).await?;
            self.file.write(&json).await?;

            index.push(&data.key, DataPosition::new(self.id, self.offset)).await;

            self.offset = self.offset + len + 4;

            if let Some(callback) = event.callback {
                callbacks.push(callback);
            }
        }
        self.file.sync_data().await?;

        // 处理需要写入完成回执的场景
        for callback in callbacks {
            callback.send(());
        }
        Ok(())
    }
}
