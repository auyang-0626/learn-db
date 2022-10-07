use std::error::Error;
use std::string::FromUtf8Error;
use crate::http_param::DataItem;


pub type CustomResult<T> = std::result::Result<T, CustomError>;

#[derive(Debug,PartialEq)]
pub struct CustomError {
    pub code: usize,
    pub message: String,
}

pub fn common_err(msg: String) -> CustomError {
    CustomError {
        code: 10000,
        message: msg,
    }
}

impl CustomError {
    pub fn new(e: Box<dyn Error>) -> CustomError {
        common_err(e.to_string())
    }
}

impl From<std::io::Error> for CustomError {
    fn from(e: std::io::Error) -> Self {
        common_err(e.to_string())
    }
}

impl From<serde_json::Error> for CustomError {
    fn from(e: serde_json::Error) -> Self {
        common_err(e.to_string())
    }
}

impl From<FromUtf8Error> for CustomError {
    fn from(e: FromUtf8Error) -> Self {
        common_err(e.to_string())
    }
}

impl From<tokio::sync::mpsc::error::SendError<DataItem>> for CustomError {
    fn from(e: tokio::sync::mpsc::error::SendError<DataItem>) -> Self {
        common_err(e.to_string())
    }
}