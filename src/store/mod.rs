use tokio::fs::{File, OpenOptions};
use crate::custom_err::CustomResult;
use crate::index::DataPosition;
use tokio::io::{SeekFrom, AsyncSeekExt, AsyncReadExt};
use crate::http_param::DataItem;

mod write_consumer;
mod data_manager;


pub struct ReadableFile {
    // 文件编号
    id: u32,
    // 数据文件指针
    file: File,
}

impl ReadableFile{
    async fn new(id: u32, dir: &String) -> CustomResult<ReadableFile> {
        let f = OpenOptions::new()
            .read(true)
            .open(get_file_name(id, dir))
            .await?;
        Ok(ReadableFile {
            id,
            file: f
        })
    }

    async fn read(&mut self, dp:&DataPosition)->CustomResult<String>{
        self.file.seek(SeekFrom::Start(dp.offset as u64)).await;

        let mut buffer = Vec::with_capacity(dp.length as usize);
        self.file.read(&mut buffer[..]).await;


        let set_param: DataItem = serde_json::from_str(&String::from_utf8(buffer)?)?;
        Ok(set_param.value)
    }
}

/// 按照固定的格式生成文件名
pub fn get_file_name(id: u32, dir: &String) -> String {
    format!("{}/{}.log", dir, id)
}