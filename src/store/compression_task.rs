use std::collections::HashMap;
use std::fs::{DirEntry, read_dir};
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::time;

use crate::Config;
use crate::custom_err::{common_err, CustomError, CustomResult};
use crate::index::DataPosition;
use crate::store::{get_file_id_from_path, get_index_file_name, get_log_file_name, get_tmp_index_file_name, is_log_file, read_by_dp, read_data_item};
use crate::store::data_manager::DataManager;
use crate::store::write_consumer::WriteEvent;

/// 异步整理线，主要做两件事
/// 1. 生成数据文件对应的索引文件
/// 2. 当数据文件超过配置的个数时，回收掉最老的一个
pub fn start_compression_task(cnf: Config, dm: DataManager) {
    tokio::spawn(async move {
        loop {
            let mut file_id_vec = scan_file_id_vec(&cnf.workspace);

            if !file_id_vec.is_empty() {
                // 最新的文件不需要处理，所以删除
                file_id_vec.remove(file_id_vec.len() - 1);

                for file_id in file_id_vec.clone() {
                    let index_file_str = get_index_file_name(file_id, &cnf.workspace);
                    let index_path = Path::new(&index_file_str);
                    // 只处理不存在的场景
                    if !index_path.exists() {
                        if let Err(e) = generate_index_file(
                            Path::new(&get_log_file_name(file_id, &cnf.workspace)),
                            index_path,
                        ).await {
                            log::error!("生成索引文件失败,{:?}", e);
                        }
                    }
                }

                // 回收超过数量的文件
                while file_id_vec.len() >= cnf.max_file_num as usize {
                    let file_id = file_id_vec.remove(0);
                    let file_name = get_log_file_name(file_id, &cnf.workspace);
                    log::info!("开始回收:{}", file_name);

                    if let Ok(mut file) = File::open(Path::new(&file_name)).await {
                        let mut pos = 0;
                        let mut last_item = None;
                        while let Ok((len, item)) = read_data_item(&mut file).await {
                            if last_item.is_none() {
                                last_item = Some(item.clone());
                            }

                            dm.push(WriteEvent::new_compare_event(item, DataPosition::new(
                                file_id, pos,
                            ))).await;
                            pos = pos + len;
                        }

                        // 因为写入是异步的，所以在最后写入一个同步消息，等这个消息有了回执，表明执行的都写完了
                        if let Some(item) = last_item {
                            let (tx, rx) = oneshot::channel();
                            dm.push(WriteEvent::new_callback_event(item, Some(DataPosition::new(
                                file_id, 0,
                            )), tx)).await;
                            rx.await;
                        }
                    }
                    std::fs::remove_file(Path::new(&file_name));
                    std::fs::remove_file(Path::new(&get_index_file_name(file_id, &cnf.workspace)));
                    log::info!("回收完成:{}", file_name);
                }
            }

            time::sleep(time::Duration::from_secs(10)).await
        }
    });
}

/// 从工作目录中扫描出数据文件，并解析出文件ID
pub fn scan_file_id_vec(workspace: &String) -> Vec<u32> {
    let mut file_id_vec: Vec<u32> = read_dir(Path::new(workspace))
        .unwrap()
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap())
        .filter(|f| f.path().is_file() && is_log_file(&f.path()))
        .map(|f| get_file_id_from_path(&f.path()))
        .collect();
    file_id_vec.sort();
    file_id_vec
}

/// 生成索引文件
pub async fn generate_index_file(log_path: &Path, index_path: &Path) -> CustomResult<()> {
    let mut log_file = File::open(log_path).await?;

    let tmp_index_path = format!("{}.tmp", index_path.to_str()
        .ok_or(common_err(format!("生成临时索引文件失败！")))?);
    log::info!("tmp_index_path = {}", tmp_index_path);

    // 先生成临时文件，防止写到一半出问题
    let mut index_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(Path::new(&tmp_index_path))
        .await?;

    let mut pos = 0;
    while let Ok((len, item)) = read_data_item(&mut log_file).await {
        let key = item.key.as_bytes();
        index_file.write_u32(key.len() as u32).await;
        index_file.write(key).await;
        index_file.write_u32(pos).await;

        pos = pos + len;
    }
    index_file.sync_data().await;

    std::fs::rename(tmp_index_path, index_path);

    log::info!("索引文件:{:?}生成完成", index_path);

    Ok(())
}