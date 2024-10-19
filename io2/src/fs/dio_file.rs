use std::{
    alloc::{Allocator, Layout},
    collections::VecDeque,
    future::Future,
    io,
    marker::PhantomData,
    path::Path,
    rc::Weak,
    task::Poll,
};

use futures_core::Stream;
use io_uring::squeue;

use crate::{
    io_buffer::{IoBuffer, IoBufferView},
    slab,
    vecmap::VecMap,
};

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
        let (write_offset, write_size) = align_iov(self.dio_offset_align, (offset, buf.len()));

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
        let (read_offset, read_size) = align_iov(self.dio_offset_align, (offset, size));

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
        let (read_offset, read_size) = align_iov(self.dio_offset_align, (offset, size));

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

        let min_offset_increment = usize::try_from(self.dio_offset_align)
            .unwrap()
            .max(self.dio_mem_align);

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

    pub fn read_stream<'file, A: Allocator + Unpin + Copy>(
        &'file self,
        iovs: Vec<(u64, usize), A>,
        concurrency: usize,
        max_single_read_size: usize,
        max_merge_len: u64,
        alloc: A,
    ) -> ReadStream<'file, A> {
        ReadStream::Prep {
            iovs,
            file: self,
            concurrency,
            max_single_read_size,
            max_merge_len,
            alloc,
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

fn prep_dio_list<A: Allocator + Copy>(
    iovs: &[(u64, usize)],
    dio_offset_align: u64,
    dio_mem_align: usize,
    max_merge_len: u64,
    max_single_read_size: usize,
    alloc: A,
) -> (Vec<(u64, usize), A>, Vec<IoBuffer<A>, A>) {
    assert!(!iovs.is_empty());

    let mut dio_list = Vec::<(u64, usize), A>::with_capacity_in(iovs.len(), alloc);
    let mut io_buffers = Vec::<IoBuffer<A>, A>::with_capacity_in(iovs.len(), alloc);
    let mut dio = align_iov(dio_offset_align, iovs[0]);

    for &iov in &iovs[1..] {
        let diov = align_iov(dio_offset_align, iov);
        let diov_end = diov.0 + u64::try_from(diov.1).unwrap();

        let dio_end = dio.0 + u64::try_from(dio.1).unwrap();

        if diov_end <= dio_end {
            continue;
        }

        let extra_read_size = usize::try_from(diov_end - dio_end).unwrap();
        let new_read_size = dio.1 + extra_read_size;

        if diov_end <= dio_end + max_merge_len && new_read_size <= max_single_read_size {
            dio.1 += new_read_size;
        } else {
            dio_list.push(dio);
            io_buffers.push(
                IoBuffer::new(
                    Layout::from_size_align(dio.1, dio_mem_align).unwrap(),
                    alloc,
                )
                .unwrap(),
            );
            dio = if diov.0 >= dio_end {
                diov
            } else {
                (dio_end, extra_read_size)
            };
        }
    }
    dio_list.push(dio);
    io_buffers.push(
        IoBuffer::new(
            Layout::from_size_align(dio.1, dio_mem_align).unwrap(),
            alloc,
        )
        .unwrap(),
    );

    (dio_list, io_buffers)
}

// iovs [(x0, x1), (x2, x3), (x4, x5)]
// actual reads [(z0, z1), (z2, z3)]
// when read0 finishes return first two iovs
//

#[must_use = "streams do nothing unless polled"]
pub enum ReadStream<'file, A: Allocator + Unpin + Copy> {
    Prep {
        file: &'file DioFile,
        iovs: Vec<(u64, usize), A>,
        concurrency: usize,
        max_single_read_size: usize,
        max_merge_len: u64,
        alloc: A,
        _non_send: PhantomData<*mut ()>,
    },
}

impl<'file, A: Allocator + Unpin + Copy> Stream for ReadStream<'file, A> {
    type Item = IoBufferView<A>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let fut = self.get_mut();

        match fut {
            ReadStream::Prep {
                file,
                iovs,
                concurrency,
                max_single_read_size,
                max_merge_len,
                alloc,
                ..
            } => {
                if iovs.is_empty() {
                    return Poll::Ready(None);
                }

                iovs.sort_unstable_by_key(|iov| iov.0);

                let (dio_list, io_buffers) = prep_dio_list(
                    iovs,
                    file.dio_offset_align,
                    file.dio_mem_align,
                    *max_merge_len,
                    *max_single_read_size,
                    *alloc,
                );

                let mut io_map =
                    VecMap::<slab::Key, (u64, usize), A>::with_capacity_in(*concurrency, *alloc);
                let mut outputs =
                    VecMap::<slab::Key, IoBufferView<A>, A>::with_capacity_in(iovs.len(), *alloc);

                for &iov in iovs.iter() {}
            }
        }

        todo!()
    }
}

fn align_up(v: u64, align: u64) -> u64 {
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
