use std::collections::hash_map::RandomState;

use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use crate::custom_err::CustomResult;
use crate::http_param::DataItem;
use crate::index::DataPosition;

pub mod write_consumer;
pub mod data_manager;
mod compression_task;

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref FILE_MAP: DashMap<u32,File> = DashMap::new();
}

/// 按照固定的格式生成文件名
pub fn get_file_name(id: u32, dir: &String) -> String {
    format!("{}/{}.log", dir, id)
}

// 根据位置信息，读取文件内容
pub async fn read_by_dp(dir: &String, dp: &DataPosition) -> CustomResult<String> {
    let mut file = match FILE_MAP.get(&dp.file_id) {
        None => {
            let f = OpenOptions::new()
                .read(true)
                .open(get_file_name(dp.file_id, dir))
                .await?;
            FILE_MAP.insert(dp.file_id, f.try_clone().await?);
            f
        }
        Some(f) => {
            f.value().try_clone().await?
        }
    };

    file.seek(SeekFrom::Start(dp.offset as u64)).await;

    let len = file.read_u32().await?;

    let mut buffer = vec![Default::default(); len as usize];
    file.read(&mut buffer[..]).await;

    let set_param: DataItem = serde_json::from_str(&String::from_utf8(buffer)?)?;
    Ok(set_param.value)
}
