use std::{io, path::Path};

use super::file::File;

pub struct DmaFile {
    file: File,
}

impl DmaFile {
    pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<DmaFile> {
        Ok(DmaFile {
            file: File::open(path, flags | libc::O_DIRECT, mode)?.await?,
        })
    }

    pub fn as_file(&self) -> &File {
        &self.file
    }
}
