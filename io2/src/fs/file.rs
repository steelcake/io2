use std::ffi::CString;
use std::future::Future;
use std::io::{self, ErrorKind};
use std::marker::PhantomData;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode;
use io_uring::types::Fd;
use pin_project_lite::pin_project;

use crate::executor::{CURRENT_TASK_CONTEXT, FILES_TO_CLOSE};
use crate::slab;

pub struct File {
    pub(crate) fd: RawFd,
    _non_send: PhantomData<*mut ()>,
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Close {
    io_id: Option<slab::Key>,
    fd: RawFd,
    _non_send: PhantomData<*mut ()>,
}

impl Future for Close {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.get_mut();
            match fut.io_id {
                None => {
                    fut.io_id = Some(unsafe {
                        ctx.queue_io(opcode::Close::new(Fd(fut.fd)).build(), false)
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
                        Poll::Ready(Ok(()))
                    }
                }
            }
        })
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Open {
        path: CString,
        #[pin] how: libc::open_how,
        io_id: Option<slab::Key>,
        _non_send: PhantomData<*mut ()>,
    }
}

impl Future for Open {
    type Output = io::Result<File>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.project();
            match fut.io_id {
                None => {
                    *fut.io_id = Some(unsafe {
                        ctx.queue_io(
                            opcode::OpenAt2::new(
                                Fd(libc::AT_FDCWD),
                                fut.path.as_ptr(),
                                &*fut.how as *const libc::open_how as *const _,
                            )
                            .build(),
                            false,
                        )
                    });
                    Poll::Pending
                }
                Some(io_id) => {
                    let io_result = match ctx.take_io_result(*io_id) {
                        Some(io_result) => io_result,
                        None => {
                            return Poll::Pending;
                        }
                    };

                    let fd = if io_result < 0 {
                        return Poll::Ready(Err(io::Error::from_raw_os_error(-io_result)));
                    } else {
                        io_result
                    };

                    Poll::Ready(Ok(File {
                        fd,
                        _non_send: PhantomData,
                    }))
                }
            }
        })
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Read<'file, 'buf> {
    pub(crate) file: &'file File,
    pub(crate) offset: u64,
    pub(crate) buf: &'buf mut [u8],
    pub(crate) io_id: Option<slab::Key>,
    pub(crate) direct_io: bool,
    pub(crate) _non_send: PhantomData<*mut ()>,
}

impl<'file, 'buf> Future for Read<'file, 'buf> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.get_mut();
            match fut.io_id {
                None => {
                    fut.io_id = Some(unsafe {
                        ctx.queue_io(
                            opcode::Read::new(
                                Fd(fut.file.fd),
                                fut.buf.as_mut_ptr(),
                                fut.buf.len().try_into().unwrap(),
                            )
                            .offset(fut.offset)
                            .build(),
                            fut.direct_io,
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
                        Poll::Ready(Ok(io_result.try_into().unwrap()))
                    }
                }
            }
        })
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Write<'file, 'buf> {
    pub(crate) file: &'file File,
    pub(crate) offset: u64,
    pub(crate) buf: &'buf [u8],
    pub(crate) io_id: Option<slab::Key>,
    pub(crate) direct_io: bool,
    pub(crate) _non_send: PhantomData<*mut ()>,
}

impl<'file, 'buf> Future for Write<'file, 'buf> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.get_mut();
            match fut.io_id {
                None => {
                    fut.io_id = Some(unsafe {
                        ctx.queue_io(
                            opcode::Write::new(
                                Fd(fut.file.fd),
                                fut.buf.as_ptr(),
                                fut.buf.len().try_into().unwrap(),
                            )
                            .offset(fut.offset)
                            .build(),
                            fut.direct_io,
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
                        Poll::Ready(Ok(io_result.try_into().unwrap()))
                    }
                }
            }
        })
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub(crate) struct Statx<'file> {
        file: &'file File,
        io_id: Option<slab::Key>,
        #[pin] statx: libc::statx,
        _non_send: PhantomData<*mut ()>,
    }
}

static EMPTY_PATH: u8 = b'\0';

fn empty_path() -> *const libc::c_char {
    &EMPTY_PATH as *const u8 as *const libc::c_char
}

impl<'file> Future for Statx<'file> {
    type Output = io::Result<libc::statx>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.project();
            match fut.io_id {
                None => {
                    *fut.io_id = Some(unsafe {
                        ctx.queue_io(
                            opcode::Statx::new(
                                Fd(fut.file.fd),
                                empty_path(),
                                &*fut.statx as *const libc::statx as *mut _,
                            )
                            .flags(libc::AT_EMPTY_PATH)
                            .mask(libc::STATX_DIOALIGN)
                            .build(),
                            false,
                        )
                    });
                    Poll::Pending
                }
                Some(io_id) => {
                    let io_result = match ctx.take_io_result(*io_id) {
                        Some(io_result) => io_result,
                        None => {
                            return Poll::Pending;
                        }
                    };

                    if io_result < 0 {
                        Poll::Ready(Err(io::Error::from_raw_os_error(-io_result)))
                    } else {
                        Poll::Ready(Ok(*fut.statx))
                    }
                }
            }
        })
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SyncAll<'file> {
    file: &'file File,
    io_id: Option<slab::Key>,
    _non_send: PhantomData<*mut ()>,
}

impl<'file> Future for SyncAll<'file> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            let fut = self.get_mut();
            match fut.io_id {
                None => {
                    fut.io_id = Some(unsafe {
                        ctx.queue_io(opcode::Fsync::new(Fd(fut.file.fd)).build(), false)
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
                        Poll::Ready(Ok(()))
                    }
                }
            }
        })
    }
}

impl File {
    pub fn open(path: &Path, flags: i32, mode: i32) -> io::Result<Open> {
        let path = CString::new(path.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidInput, e))?;
        let mut how: libc::open_how = unsafe { std::mem::zeroed() };
        how.flags = flags as u64;
        how.mode = mode as u64;
        Ok(Open {
            path,
            how,
            io_id: None,
            _non_send: PhantomData,
        })
    }

    pub fn read<'file, 'buf>(&'file self, buf: &'buf mut [u8], offset: u64) -> Read<'file, 'buf> {
        Read {
            offset,
            buf,
            file: self,
            io_id: None,
            direct_io: false,
            _non_send: PhantomData,
        }
    }

    pub fn write<'file, 'buf>(&'file self, buf: &'buf [u8], offset: u64) -> Write<'file, 'buf> {
        Write {
            offset,
            buf,
            file: self,
            io_id: None,
            direct_io: false,
            _non_send: PhantomData,
        }
    }

    pub fn sync_all(&self) -> SyncAll {
        SyncAll {
            file: self,
            io_id: None,
            _non_send: PhantomData,
        }
    }

    pub fn close(self) -> Close {
        let fd = self.fd;
        std::mem::forget(self);
        Close {
            io_id: None,
            fd,
            _non_send: PhantomData,
        }
    }

    pub(crate) fn statx(&self) -> Statx<'_> {
        Statx {
            file: self,
            io_id: None,
            statx: unsafe { std::mem::zeroed() },
            _non_send: PhantomData,
        }
    }

    pub async fn file_size(&self) -> io::Result<u64> {
        let statx = self.statx().await?;
        Ok(statx.stx_size)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        FILES_TO_CLOSE.with_borrow_mut(|files| {
            files.push(self.fd);
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::ExecutorConfig;

    use super::*;

    #[test]
    fn smoke_test_file() {
        let x = ExecutorConfig::new()
            .run(Box::pin(async {
                let file = File::open(Path::new("Cargo.toml"), libc::O_RDONLY, 0)
                    .unwrap()
                    .await
                    .unwrap();
                dbg!(file.fd);
                let size = file.file_size().await.unwrap();
                dbg!(size);
                let mut out = vec![0; size.try_into().unwrap()];
                let start = std::time::Instant::now();
                let num_read = file.read(&mut out, 0).await.unwrap();
                dbg!(num_read);
                //file.close().await.unwrap();
                println!("{}", String::from_utf8(out).unwrap());
                println!("delay {}ns", start.elapsed().as_nanos());

                5
            }))
            .unwrap();

        assert_eq!(x, 5);
        dbg!(x);
    }
}
