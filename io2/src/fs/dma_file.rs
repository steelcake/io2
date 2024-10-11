use std::{io, path::Path};

use super::file::File;

pub struct DmaFile {
    file: File,
    dio_mem_align: u32,
    dio_offset_align: u32,
}

impl DmaFile {
    pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<DmaFile> {
        let file = File::open(path, flags | libc::O_DIRECT, mode)?.await?;
        let statx = file.statx().await?;

        Ok(DmaFile {
            file,
            dio_mem_align: statx.stx_dio_mem_align,
            dio_offset_align: statx.stx_dio_offset_align,
        })
    }

    pub fn as_file(&self) -> &File {
        &self.file
    }

    pub fn into_file(self) -> File {
        self.file
    }
}
