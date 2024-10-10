use std::{io, path::Path};

use crate::{
    executor::{BlockDeviceInfo, CURRENT_TASK_CONTEXT},
    local_alloc::LocalAlloc,
};

use super::file::{self, File};

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
            ctx.get_block_device_info(statx.stx_dev_major)
        });
        let block_device_info = match block_device_info {
            Some(info) => info,
            None => {
                let path = format!(
                    "/sys/dev/block/{}:0/logical_block_size",
                    statx.stx_dev_major
                );
                let path = Path::new(path.as_str());
                let data = file::read(path, LocalAlloc::new()).await?;
                let data = std::str::from_utf8(data.as_slice()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "failed to parse integer in logical_block_size file",
                    )
                })?;
                let contents = data.trim_matches('\n');
                let logical_block_size = contents.parse::<usize>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "failed to parse integer in logical_block_size file",
                    )
                })?;

                let info = BlockDeviceInfo { logical_block_size };

                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    let ctx = ctx.as_mut().unwrap();
                    ctx.set_block_device_info(statx.stx_dev_major, info);
                });

                info
            }
        };

        Ok(DmaFile {
            file,
            block_device_info,
        })
    }

    pub fn as_file(&self) -> &File {
        &self.file
    }
}
