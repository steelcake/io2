use std::{io, marker::PhantomData, path::Path};

use super::file::{Close, File, Read, SyncAll, Write};

pub struct DmaFile {
    file: File,
    dio_mem_align: u32,
    dio_offset_align: u32,
}

impl DmaFile {
    pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<DmaFile> {
        let file = File::open(path, flags | libc::O_DIRECT, mode)?.await?;
        let statx = file.statx().await?;

        if statx.stx_dio_mem_align == 0 || statx.stx_dio_offset_align == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "direct_io is not supported on this file, kernel might be old, or the file might be on an unsupported file system",
            ));
        }

        Ok(DmaFile {
            file,
            dio_mem_align: statx.stx_dio_mem_align,
            dio_offset_align: statx.stx_dio_offset_align,
        })
    }

    pub fn close(self) -> Close {
        self.file.close()
    }

    pub async fn file_size(&self) -> io::Result<u64> {
        self.file.file_size().await
    }

    pub fn sync_all(&self) -> SyncAll {
        self.file.sync_all()
    }

    pub fn write_aligned<'file, 'buf>(
        &'file self,
        buf: &'buf [u8],
        offset: u64,
    ) -> Write<'file, 'buf> {
        assert_eq!(
            buf.as_ptr()
                .align_offset(usize::try_from(self.dio_mem_align).unwrap()),
            0
        );
        assert_eq!(offset % u64::from(self.dio_offset_align), 0);
        assert_eq!(buf.len() % usize::try_from(self.dio_mem_align).unwrap(), 0);

        Write {
            offset,
            buf,
            file: &self.file,
            io_id: None,
            direct_io: true,
            _non_send: PhantomData,
        }
    }

    pub fn read_aligned<'file, 'buf>(
        &'file self,
        buf: &'buf mut [u8],
        offset: u64,
    ) -> Read<'file, 'buf> {
        assert_eq!(
            buf.as_ptr()
                .align_offset(usize::try_from(self.dio_mem_align).unwrap()),
            0
        );
        assert_eq!(offset % u64::from(self.dio_offset_align), 0);
        assert_eq!(buf.len() % usize::try_from(self.dio_mem_align).unwrap(), 0);

        Read {
            offset,
            buf,
            file: &self.file,
            io_id: None,
            direct_io: true,
            _non_send: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::ExecutorConfig;

    use super::*;

    #[test]
    fn smoke_test_dma_file() {
        let x = ExecutorConfig::new()
            .run(Box::pin(async {
                let start = std::time::Instant::now();
                let file = DmaFile::open(Path::new("src/lib.rs"), libc::O_RDONLY, 0)
                    .await
                    .unwrap();
                dbg!((file.dio_mem_align, file.dio_offset_align));
                let size = file.file_size().await.unwrap();
                dbg!(size);
                5
            }))
            .unwrap();

        assert_eq!(x, 5);
        dbg!(x);
    }
}
