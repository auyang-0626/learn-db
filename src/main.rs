use std::path::PathBuf;

use crate::index::DataPosition;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;

mod index;
mod custom_err;

#[tokio::main]
async fn main() {
    init_log();
    let index = DynamicParallelIndexWrapper::new(8);
    for i in 0..1024 {
        index.push(i, DataPosition::new(i as u32, i as u32, i as u32)).await;
    }
}


pub fn init_log() {
    let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    config_path.push("log4rs.yaml");
    println!("{:?}", config_path);
    // Path::new("log4rs.yaml").metadata()?.
    log4rs::init_file(config_path, Default::default()).unwrap();
}

