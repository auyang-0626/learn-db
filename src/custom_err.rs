use std::error::Error;


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