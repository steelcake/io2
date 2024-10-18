use std::{
    alloc::{Allocator, Layout}, collections::VecDeque, future::Future, io, marker::PhantomData, path::Path
};

use io_uring::squeue;

use crate::{io_buffer::{IoBuffer, IoBufferView}, slab};

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

    pub async fn write_all_aligned(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut retry: Option<usize> = None;
        loop {
            match self.write_aligned(buf, offset).await {
                Ok(n) => {
                    if n < buf.len() {
                        if retry == Some(n) {
                            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                        }
                        retry = Some(n);
                    } else {
                        return Ok(());
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn write_all<A: Allocator>(
        &self,
        buf: &[u8],
        offset: u64,
        alloc: A,
    ) -> io::Result<()> {
        let write_offset = align_down(offset, u64::from(self.dio_offset_align));
        let write_size = usize::try_from(align_up(
            u32::try_from(buf.len()).unwrap(),
            self.dio_offset_align,
        ))
        .unwrap();

        if write_offset == offset && write_size == buf.len() {
            return self.write_all_aligned(buf, offset).await;
        }

        let layout =
            Layout::from_size_align(write_size, usize::try_from(self.dio_mem_align).unwrap())
                .unwrap();
        let mut write_buf = IoBuffer::new(layout, alloc).unwrap();

        let start_diff = offset.checked_sub(write_offset).unwrap();
        if start_diff > 0 {
            self.read_exact_aligned(
                &mut write_buf.as_mut_slice()[..usize::try_from(self.dio_offset_align).unwrap()],
                write_offset,
            )
            .await?;
        }

        let end_diff = (write_offset + u64::try_from(write_size).unwrap())
            .checked_sub(offset + u64::try_from(buf.len()).unwrap())
            .unwrap();
        if end_diff > 0 {
            self.read_exact_aligned(
                &mut write_buf.as_mut_slice()[usize::try_from(end_diff).unwrap()..],
                write_offset + end_diff,
            )
            .await?;
        }

        let start_diff = usize::try_from(start_diff).unwrap();
        write_buf.as_mut_slice()[start_diff..start_diff + buf.len()].copy_from_slice(buf);

        self.write_all_aligned(write_buf.as_slice(), write_offset)
            .await
    }

    pub async fn read<A: Allocator>(
        &self,
        offset: u64,
        size: usize,
        alloc: A,
    ) -> io::Result<IoBufferView<A>> {
        let read_offset = align_down(offset, u64::from(self.dio_offset_align));
        let read_size = usize::try_from(align_up(
            u32::try_from(size).unwrap(),
            self.dio_offset_align,
        ))
        .unwrap();
        let view_start = usize::try_from(offset.checked_sub(read_offset).unwrap()).unwrap();
        let view_len = size;

        let layout =
            Layout::from_size_align(read_size, usize::try_from(self.dio_mem_align).unwrap())
                .unwrap();
        let mut buf = IoBuffer::new(layout, alloc).unwrap();

        let n_read = self.read_aligned(buf.as_mut_slice(), read_offset).await?;
        let n_read_in_range = n_read.saturating_sub(view_start);

        let view = buf.view(view_start, view_len.min(n_read_in_range));

        Ok(view)
    }

    pub async fn read_exact_aligned(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let mut retry: Option<usize> = None;
        loop {
            match self.read_aligned(buf, offset).await {
                Ok(n) => {
                    if n < buf.len() {
                        if retry == Some(n) {
                            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                        }
                        retry = Some(n);
                    } else {
                        return Ok(());
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn read_exact<A: Allocator>(
        &self,
        offset: u64,
        size: usize,
        alloc: A,
    ) -> io::Result<IoBufferView<A>> {
        let read_offset = align_down(offset, u64::from(self.dio_offset_align));
        let read_size = usize::try_from(align_up(
            u32::try_from(size).unwrap(),
            self.dio_offset_align,
        ))
        .unwrap();
        let view_start = usize::try_from(offset.checked_sub(read_offset).unwrap()).unwrap();
        let view_len = size;

        let layout =
            Layout::from_size_align(read_size, usize::try_from(self.dio_mem_align).unwrap())
                .unwrap();
        let mut buf = IoBuffer::new(layout, alloc).unwrap();
        let mut buf_slice = buf.as_mut_slice();
        let mut read_offset = read_offset;
        let mut total_read = 0;
        let mut retry: Option<usize> = None;

        let min_offset_increment =
            usize::try_from(self.dio_offset_align.max(self.dio_mem_align)).unwrap();

        loop {
            match self.read_aligned(buf_slice, read_offset).await {
                Ok(0) => break,
                Ok(n) => {
                    if total_read + n >= view_start + view_len {
                        break;
                    }
                    if n >= min_offset_increment {
                        total_read += min_offset_increment;
                        buf_slice = &mut buf_slice[min_offset_increment..];
                        read_offset += u64::try_from(min_offset_increment).unwrap();
                        retry = None;
                    } else {
                        if retry == Some(n) {
                            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                        }
                        retry = Some(n);
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }

        Ok(buf.view(view_start, view_len))
    }

    pub fn read_stream<'file, 'iovs, A: Allocator, IOVS: Iterator<Item=(u64, usize)>>(&self, iovs: IOVS, concurrency: usize, max_single_read_size: u64, alloc: A) -> ReadStream<'file, 'iovs, A> {
        let mut io_queue = Vec::<(u64, usize), A>::with_capacity_in(128, alloc);
        let mut io: Option<(u64, usize)> = None;

        for iov in iovs {
            assert!(iov.0 >= io.0 + u64::try_from(io.1).unwrap());
            assert!(iov.1 > 0);

            if io.0.checked_sub(iov.0).unwrap() <= max_single_read_size {

            } else if io != (0, 0) {
                io_queue.push(io);
            }
        }

        todo!()
    }
}

// iovs [(x0, x1), (x2, x3), (x4, x5)]
// actual reads [(z0, z1), (z2, z3)]
// when read0 finishes return first two iovs
// 

#[must_use = "streams do nothing unless polled"]
struct ReadStream<'file, 'iovs, A: Allocator> {
    file: &'file DioFile,
    running_iovs: &'iovs [(u64, usize)],
    buf: IoBuffer<A>,
    io_queue: Vec<squeue::Entry, A>,
    running_io: String,
    return_queue: VecDeque<Option<IoBufferView<A>>, A>,
    _non_send: PhantomData<*mut ()>,
}

fn align_up(v: u32, align: u32) -> u32 {
    (v + align - 1) & !(align - 1)
}

fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

#[cfg(test)]
mod tests {
    use crate::{executor::ExecutorConfig, local_alloc::LocalAlloc};

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
                let start = std::time::Instant::now();
                let buf = file.read_exact(0, size, LocalAlloc::new()).await.unwrap();
                assert_eq!(buf.len(), size);
                println!("{}", std::str::from_utf8(buf.as_slice()).unwrap());
                println!("delay {}ns", start.elapsed().as_nanos());
                5
            }))
            .unwrap();

        assert_eq!(x, 5);
        dbg!(x);
    }
}
