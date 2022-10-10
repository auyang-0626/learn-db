use crate::Config;
use crate::store::data_manager::DataManager;

/// 异步整理线，主要做两件事
/// 1. 生成数据文件对应的索引文件
/// 2. 当数据文件超过配置的个数时，回收掉最老的一个
pub fn start_compression_task(cnf: Config, dm: DataManager) {
    tokio::spawn(async move {

    });
}