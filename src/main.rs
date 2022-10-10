#[macro_use]
extern crate lazy_static;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use log::info;
use tokio::sync::oneshot;

use crate::http_param::{DataItem, View};
use crate::index::DataPosition;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;
use crate::store::data_manager::DataManager;
use crate::store::write_consumer::WriteEvent;

mod index;
mod custom_err;
mod http_param;
mod store;

#[derive(Clone)]
pub struct Config {
    // 数据文件存储的目录
    pub workspace: String,
    // 单个数据文件最大值，超过后就会写新文件
    pub max_file_size: u32,
    // 最多允许的数据文件个数，超过后就会开始整理合并
    pub max_file_num: u32,
}

impl Config {
    pub fn new(workspace: String) -> Config {
        Config {
            workspace,
            max_file_size: 1024 * 1024 * 1,
            max_file_num: 10,
        }
    }
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    init_log();

    let config = Config::new(String::from("/Users/yang/logs/learn-db"));

    if !Path::new(&config.workspace).is_dir() {
        panic!("workspace[{}] 不合法,必须存在且是文件夹！", config.workspace);
    }

    let dm = DataManager::new(config);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(dm.clone()))
            .service(hello)
            .service(find)
            .service(push)
            .service(push_sync)
    })
        .bind(("127.0.0.1", 8848))?
        .run()
        .await
}


#[actix_web::get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Welcome to Learn-DB!")
}

#[actix_web::get("/get/{key}")]
async fn find(key: web::Path<String>, dm: web::Data<DataManager>) -> impl Responder {
    let key = key.into_inner();
    let res = dm.find(&key).await;
    info!("url=/get/{},value={:?}", &key, res);
    web::Json(View::success(res))
}

#[actix_web::post("/set")]
async fn push(param: web::Json<DataItem>, dm: web::Data<DataManager>) -> impl Responder {
    let param = param.into_inner();
    dm.push(WriteEvent::new_simple_event(param)).await;
    info!("url = /set");
    web::Json(View::success(""))
}

#[actix_web::post("/set_sync")]
async fn push_sync(param: web::Json<DataItem>, dm: web::Data<DataManager>) -> impl Responder {
    let param = param.into_inner();
    let (tx, rx) = oneshot::channel();
    dm.push(WriteEvent::new_callback_event(param, None, tx)).await;
    // 等待写入完成
    rx.await.unwrap();
    info!("url = /set");
    web::Json(View::success(""))
}

pub fn init_log() {
    let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    config_path.push("log4rs.yaml");
    println!("{:?}", config_path);
    // Path::new("log4rs.yaml").metadata()?.
    log4rs::init_file(config_path, Default::default()).unwrap();
    log::info!("日志初始化成功！");
}

/// 计算hash
pub fn calc_hash(key: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

