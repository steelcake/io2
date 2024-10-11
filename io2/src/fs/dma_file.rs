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
                "direct_io is not supported on this file",
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
