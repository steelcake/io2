use std::{
    alloc::{Allocator, Layout},
    future::Future,
    io,
    marker::PhantomData,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::{
    executor::CURRENT_TASK_CONTEXT,
    io_buffer::{IoBuffer, IoBufferView},
    slab,
};

use super::file::{Close, File, SyncAll};

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

        Ok(DioFile {
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

    pub fn read<A: Allocator + Unpin + Copy>(&self, offset: u64, size: usize, alloc: A) -> Read<A> {
        let read_offset = align_down(offset, u64::from(self.dio_offset_align));
        let read_size = align_up(u32::try_from(size).unwrap(), self.dio_offset_align);
        let view_start = usize::try_from(offset.checked_sub(read_offset).unwrap()).unwrap();

        Read {
            file: self,
            read_offset,
            read_size,
            mem_align: usize::try_from(self.dio_mem_align).unwrap(),
            view_start,
            view_len: size,
            alloc,
            io_id: None,
            buf: None,
            _non_send: PhantomData,
        }
    }
}

pub struct Read<'file, A: Allocator + Unpin + Copy> {
    file: &'file DioFile,
    read_offset: u64,
    read_size: u32,
    mem_align: usize,
    view_start: usize,
    view_len: usize,
    alloc: A,
    io_id: Option<slab::Key>,
    buf: Option<IoBuffer<A>>,
    _non_send: PhantomData<*mut ()>,
}

impl<'file, A: Allocator + Unpin + Copy> Future for Read<'file, A> {
    type Output = io::Result<IoBufferView<A>>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.get_mut();
            match fut.io_id {
                None => {
                    let layout = Layout::from_size_align(
                        usize::try_from(fut.read_size).unwrap(),
                        fut.mem_align,
                    )
                    .unwrap();
                    let mut buf = IoBuffer::new(layout, fut.alloc).unwrap();
                    let buf_ptr = buf.as_mut_ptr();
                    fut.buf = Some(buf);
                    fut.io_id = Some(unsafe {
                        ctx.queue_dio(
                            opcode::Read::new(Fd(fut.file.file.fd), buf_ptr, fut.read_size)
                                .offset(fut.read_offset)
                                .build(),
                        )
                    });
                    Poll::Pending
                }
                Some(io_id) => {
                    let io_result = match ctx.take_io_result(io_id) {
                        Some(io_result) => io_result,
                        None => {
                            return Poll::Pending;
                        }
                    };

                    if io_result < 0 {
                        Poll::Ready(Err(io::Error::from_raw_os_error(-io_result)))
                    } else {
                        let n_read = usize::try_from(io_result).unwrap();
                        let buf = fut.buf.take().unwrap();
                        let view = IoBufferView::new(buf, fut.view_start, fut.view_len.min(n_read));
                        Poll::Ready(Ok(view))
                    }
                }
            }
        })
    }
}

pub(crate) fn align_up(v: u32, align: u32) -> u32 {
    (v + align - 1) & !(align - 1)
}

pub(crate) fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

#[cfg(test)]
mod tests {
    use crate::{executor::ExecutorConfig, local_alloc::LocalAlloc};

    use super::*;

    #[test]
    fn smoke_test_dma_file() {
        let x = ExecutorConfig::new()
            .run(Box::pin(async {
                let file = DioFile::open(Path::new("Cargo.toml"), libc::O_RDONLY, 0)
                    .await
                    .unwrap();
                dbg!((file.dio_mem_align, file.dio_offset_align));
                let size = file.file_size().await.unwrap();
                let start = std::time::Instant::now();
                let buf = file
                    .read(0, usize::try_from(size).unwrap(), LocalAlloc::new())
                    .await
                    .unwrap();
                println!("{}", std::str::from_utf8(buf.as_slice()).unwrap());
                println!("delay {}ns", start.elapsed().as_nanos());
                5
            }))
            .unwrap();

        assert_eq!(x, 5);
        dbg!(x);
    }
}
