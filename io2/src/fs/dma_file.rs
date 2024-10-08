use std::{io, path::Path};

use crate::executor::{BlockDeviceInfo, CURRENT_TASK_CONTEXT};

use super::file::File;

pub struct DmaFile {
    file: File,
    block_device_info: BlockDeviceInfo,
}

impl DmaFile {
    pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<DmaFile> {
        let file = File::open(path, flags | libc::O_DIRECT, mode)?.await?;
        let statx = file.statx().await?;
        let block_device_info = CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();

            if let Some(info) = ctx.get_block_device_info(statx.stx_dev_major) {
                return Ok::<_, io::Error>(info);
            }

            let info = todo!();

            ctx.set_block_device_info(statx.stx_dev_major, info);

            Ok(info)
        })?;

        Ok(DmaFile {
            file,
            block_device_info,
        })
    }

    pub fn as_file(&self) -> &File {
        &self.file
    }
}
