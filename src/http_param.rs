use serde::{Deserialize, Serialize};

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

#[derive(Deserialize, Serialize,Clone)]
pub struct DataItem {
    pub key: String,
    pub value: String,
}
