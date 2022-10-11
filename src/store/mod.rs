use std::collections::hash_map::RandomState;
use std::num::ParseIntError;
use std::path::Path;
use std::str::FromStr;

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

// 文件前缀
const FILE_PREFIX: &str = "learn_db_";
// 日志文件后缀
const LOG_FILE_SUFFIX: &str = ".log";
// 索引文件后缀
const INDEX_FILE_SUFFIX: &str = ".index";

/// 按照固定的格式生成文件名
pub fn get_log_file_name(id: u32, dir: &String) -> String {
    format!("{}/{}{}{}", dir, FILE_PREFIX, id, LOG_FILE_SUFFIX)
}

/// 按照固定的格式生成文件名
pub fn get_index_file_name(id: u32, dir: &String) -> String {
    format!("{}/{}{}{}", dir, FILE_PREFIX, id, INDEX_FILE_SUFFIX)
}

/// 按照固定的格式生成文件名
pub fn get_tmp_index_file_name(id: u32, dir: &String) -> String {
    format!("{}/tmp_{}{}{}", dir, FILE_PREFIX, id, INDEX_FILE_SUFFIX)
}


/// 指定的文件，是否是日志文件
pub fn is_log_file(path: &Path) -> bool {
    if let Some(file_name) = path.file_name() {
        if let Some(name) = file_name.to_str() {
            return name.starts_with(FILE_PREFIX) && name.ends_with(LOG_FILE_SUFFIX);
        }
    }
    false
}

/// 从日志文件中，解析出对应的文件ID
pub fn get_file_id_from_path(path: &Path) -> u32 {
    if let Some(file_name) = path.file_name() {
        if let Some(name) = file_name.to_str() {
            let num = &name[FILE_PREFIX.len()..name.len() - LOG_FILE_SUFFIX.len()];
            match u32::from_str(num) {
                Ok(v) => return v,
                Err(_) => return 0
            }
        }
    }
    0
}

// 根据位置信息，读取文件内容
pub async fn read_by_dp(dir: &String, dp: &DataPosition) -> CustomResult<String> {
    let mut file = match FILE_MAP.get(&dp.file_id) {
        None => {
            let f = OpenOptions::new()
                .read(true)
                .open(get_log_file_name(dp.file_id, dir))
                .await?;
            FILE_MAP.insert(dp.file_id, f.try_clone().await?);
            f
        }
        Some(f) => {
            f.value().try_clone().await?
        }
    };

    file.seek(SeekFrom::Start(dp.offset as u64)).await;

    Ok(read_data_item(&mut file).await?.1.value)
}

/// 从指定文件中，读取下一条数据
pub async fn read_data_item(file: &mut File) -> CustomResult<(u32,DataItem)> {
    let len = file.read_u32().await?;

    let mut buffer = vec![Default::default(); len as usize];
    file.read(&mut buffer[..]).await;

    let item: DataItem = serde_json::from_str(&String::from_utf8(buffer)?)?;
    Ok((len + 4,item))
}
