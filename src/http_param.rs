use serde::{Deserialize, Serialize};

use crate::index::DataPosition;

#[derive(Serialize)]
pub struct View<T> {
    code: u32,
    data: T,
}

impl<T> View<T> {
    pub fn success(value: T) -> View<T> {
        View {
            code: 10000,
            data: value,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct SetParam {
    pub key: String,
    pub value: String,
}
