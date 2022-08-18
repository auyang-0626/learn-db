use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};

pub trait ReadableFile{
    fn read();
}

struct DataFile {
    id: u64,
    file:File,
}

impl DataFile {
    pub async fn new(id: u64, dir: &PathBuf) -> DataFile {

       /* let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{:?}/{}.log",dir,id))
            .await?;*/
        todo!()

    }
}