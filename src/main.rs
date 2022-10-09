use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use log::info;

#[macro_use]
extern crate lazy_static;

use crate::http_param::{DataItem, View};
use crate::index::DataPosition;
use crate::index::dynamic_index::DynamicParallelIndexWrapper;
use crate::store::data_manager::DataManager;

mod index;
mod custom_err;
mod http_param;
mod store;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    init_log();

    let dm = DataManager::new(String::from("/Users/yang/logs/learn-db"));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(dm.clone()))
            .service(hello)
            .service(find)
            .service(push)
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
    dm.push(param).await;
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

