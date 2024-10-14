use std::{io, marker::PhantomData, path::Path};

use super::file::{Close, File, Read, SyncAll, Write};

pub struct DioFile {
    file: File,
    dio_mem_align: u32,
    dio_offset_align: u32,
}

impl DioFile {
    pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<DioFile> {
        let file = File::open(path, flags | libc::O_DIRECT, mode)?.await?;
        let statx = file.statx().await?;

        if statx.stx_dio_mem_align == 0 || statx.stx_dio_offset_align == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "direct_io is not supported on this file, kernel might be old, or the file might be on an unsupported file system",
            ));
        }

        if !statx.stx_dio_mem_align.is_power_of_two()
            || !statx.stx_dio_offset_align.is_power_of_two()
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "at least one of direct_io alignment requirements returned by statx is not a power of two",
            ));
        }

        Ok(DioFile {
            file,
            dio_mem_align: statx.stx_dio_mem_align,
            dio_offset_align: statx.stx_dio_offset_align,
        })
    }

    pub fn dio_mem_align(&self) -> u32 {
        self.dio_mem_align
    }

    pub fn dio_offset_align(&self) -> u32 {
        self.dio_offset_align
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

    fn assert_alignment(&self, buf: &[u8], offset: u64) {
        assert_eq!(
            buf.as_ptr()
                .align_offset(usize::try_from(self.dio_mem_align).unwrap()),
            0
        );
        let len = u32::try_from(buf.len()).unwrap();
        assert_eq!(align_up(len, self.dio_offset_align), len);
        assert_eq!(align_down(offset, u64::from(self.dio_offset_align)), offset);
    }

    pub fn read_aligned<'file, 'buf>(
        &'file self,
        buf: &'buf mut [u8],
        offset: u64,
    ) -> Read<'file, 'buf> {
        self.assert_alignment(buf, offset);

        Read {
            file: &self.file,
            offset,
            buf,
            io_id: None,
            direct_io: true,
            _non_send: PhantomData,
        }
    }

    pub fn write_aligned<'file, 'buf>(
        &'file self,
        buf: &'buf [u8],
        offset: u64,
    ) -> Write<'file, 'buf> {
        self.assert_alignment(buf, offset);

        Write {
            offset,
            buf,
            file: &self.file,
            io_id: None,
            direct_io: true,
            _non_send: PhantomData,
        }
    }
}

fn align_up(v: u32, align: u32) -> u32 {
    (v + align - 1) & !(align - 1)
}

fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

#[cfg(test)]
mod tests {
    use crate::{executor::ExecutorConfig, io_buffer::IoBuffer, local_alloc::LocalAlloc};
    use std::alloc::Layout;

    use super::*;

    #[test]
    fn smoke_test_dio_file() {
        let x = ExecutorConfig::new()
            .run(Box::pin(async {
                let file = DioFile::open(Path::new("Cargo.toml"), libc::O_RDONLY, 0)
                    .await
                    .unwrap();
                let size = file.file_size().await.unwrap();
                let start = std::time::Instant::now();
                let read_size = align_up(u32::try_from(size).unwrap(), file.dio_offset_align);
                let layout = Layout::from_size_align(
                    usize::try_from(read_size).unwrap(),
                    usize::try_from(file.dio_mem_align()).unwrap(),
                )
                .unwrap();
                let mut buf = IoBuffer::new(layout, LocalAlloc::new()).unwrap();
                let n_read = file.read_aligned(buf.as_mut_slice(), 0).await.unwrap();
                if n_read != usize::try_from(size).unwrap() {
                    panic!("wrong sized read. Expected {} Got {}", size, n_read);
                }
                println!("{}", std::str::from_utf8(buf.as_slice()).unwrap());
                println!("delay {}ns", start.elapsed().as_nanos());
                5
            }))
            .unwrap();

        assert_eq!(x, 5);
        dbg!(x);
    }
}
