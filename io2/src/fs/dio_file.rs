use std::{io, marker::PhantomData, path::Path};

use super::file::{Close, File, Read, SyncAll, Write};

pub struct DioFile {
    file: File,
    dio_offset_align: u64,
    dio_mem_align: usize,
}

impl DioFile {
    pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<DioFile> {
        let file = File::open(path, flags | libc::O_DIRECT, mode)?.await?;
        let statx = file.statx().await?;

        let dio_mem_align = usize::try_from(statx.stx_dio_mem_align).unwrap();
        let dio_offset_align = u64::from(statx.stx_dio_offset_align);

        if dio_mem_align == 0 || dio_offset_align == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "direct_io is not supported on this file, kernel might be old, or the file might be on an unsupported file system",
            ));
        }

        if !dio_mem_align.is_power_of_two() || !dio_offset_align.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "at least one of direct_io alignment requirements returned by statx is not a power of two",
            ));
        }

        Ok(DioFile {
            file,
            dio_mem_align,
            dio_offset_align,
        })
    }

    pub fn dio_mem_align(&self) -> usize {
        self.dio_mem_align
    }

    pub fn dio_offset_align(&self) -> u64 {
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
        let iov = (offset, buf.len());
        assert_eq!(align_iov(self.dio_offset_align, iov), iov);
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

fn align_iov(dio_offset_align: u64, (offset, size): (u64, usize)) -> (u64, usize) {
    (
        align_down(offset, dio_offset_align),
        usize::try_from(align_up(u64::try_from(size).unwrap(), dio_offset_align)).unwrap(),
    )
}

fn align_up(v: u64, align: u64) -> u64 {
    (v + align - 1) & !(align - 1)
}

fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

#[cfg(test)]
mod tests {
    use crate::{executor::ExecutorConfig, io_buffer::IoBuffer};
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
                let size = usize::try_from(size).unwrap();
                let iov = align_iov(file.dio_offset_align(), (0, size));
                let mut buf = IoBuffer::<std::alloc::Global>::new(
                    Layout::from_size_align(iov.1, file.dio_mem_align()).unwrap(),
                    std::alloc::Global,
                )
                .unwrap();
                let start = std::time::Instant::now();
                let n_read = file.read_aligned(buf.as_mut_slice(), 0).await.unwrap();
                assert_eq!(n_read, size);
                println!("{}", std::str::from_utf8(buf.as_slice()).unwrap());
                println!("delay {}ns", start.elapsed().as_nanos());
                5
            }))
            .unwrap();

        assert_eq!(x, 5);
        dbg!(x);
    }
}
