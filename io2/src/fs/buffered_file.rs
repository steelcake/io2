use std::ffi::CString;
use std::io::{self, ErrorKind};
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::runtime::{Context, Future, PollResult};

pub struct BufferedFile {
    fd: RawFd,
    statx: libc::statx,
}

pub enum Close {
    Dummy,
    Start(RawFd),
    CloseFileDescr { io_id: usize },
}

impl Future for Close {
    type Output = io::Result<()>;

    fn poll(&mut self, ctx: &mut Context) -> PollResult<Self::Output> {
        match std::mem::replace(self, Self::Dummy) {
            Self::Start(fd) => {
                let io_id = unsafe { ctx.queue_io(opcode::Close::new(Fd(fd)).build()) };
                *self = Self::CloseFileDescr { io_id };
                PollResult::Pending
            }
            Self::CloseFileDescr { io_id } => {
                let ready_io = match ctx.take_ready_io(io_id) {
                    Some(ready_io) => ready_io,
                    None => {
                        *self = Self::CloseFileDescr { io_id };
                        return PollResult::Pending;
                    }
                };

                if ready_io.result < 0 {
                    PollResult::Ready(Err(io::Error::from_raw_os_error(-ready_io.result)))
                } else {
                    PollResult::Ready(Ok(()))
                }
            }
            Self::Dummy => unreachable!(),
        }
    }
}

pub enum Open {
    Dummy,
    Start {
        path: CString,
        how: Box<libc::open_how>,
    },
    OpenFileDescr {
        path: CString,
        how: Box<libc::open_how>,
        io_id: usize,
    },
    Statx {
        empty_path: CString,
        fd: RawFd,
        io_id: usize,
        statxbuf: Box<libc::statx>,
    },
    CloseFileDescr {
        io_id: usize,
        error: io::Error,
    },
}

impl Future for Open {
    type Output = io::Result<BufferedFile>;

    fn poll(&mut self, ctx: &mut Context) -> PollResult<Self::Output> {
        match std::mem::replace(self, Self::Dummy) {
            Self::Start { path, how } => {
                let io_id = unsafe {
                    ctx.queue_io(
                        opcode::OpenAt2::new(
                            Fd(libc::AT_FDCWD),
                            path.as_ptr(),
                            how.as_ref() as *const libc::open_how as *const _,
                        )
                        .build(),
                    )
                };
                *self = Self::OpenFileDescr { path, how, io_id };
                PollResult::Pending
            }
            Self::OpenFileDescr { path, how, io_id } => {
                let ready_io = match ctx.take_ready_io(io_id) {
                    Some(ready_io) => ready_io,
                    None => {
                        *self = Self::OpenFileDescr { path, how, io_id };
                        return PollResult::Pending;
                    }
                };

                let fd = if ready_io.result < 0 {
                    return PollResult::Ready(Err(io::Error::from_raw_os_error(-ready_io.result)));
                } else {
                    ready_io.result
                };

                let empty_path = CString::new(Vec::new()).unwrap();
                let statxbuf = Box::new(unsafe { std::mem::zeroed() });

                let io_id = unsafe {
                    ctx.queue_io(
                        opcode::Statx::new(
                            Fd(fd),
                            empty_path.as_ptr(),
                            &*statxbuf as *const libc::statx as *mut _,
                        )
                        .flags(libc::AT_EMPTY_PATH)
                        .build(),
                    )
                };

                *self = Self::Statx {
                    empty_path,
                    fd,
                    io_id,
                    statxbuf,
                };
                PollResult::Pending
            }
            Self::Statx {
                empty_path,
                fd,
                io_id,
                statxbuf,
            } => {
                let ready_io = match ctx.take_ready_io(io_id) {
                    Some(ready_io) => ready_io,
                    None => {
                        *self = Self::Statx {
                            empty_path,
                            fd,
                            io_id,
                            statxbuf,
                        };
                        return PollResult::Pending;
                    }
                };

                if ready_io.result < 0 {
                    let error = io::Error::from_raw_os_error(-ready_io.result);
                    let io_id = unsafe { ctx.queue_io(opcode::Close::new(Fd(fd)).build()) };
                    *self = Self::CloseFileDescr { io_id, error }
                }

                PollResult::Ready(Ok(BufferedFile {
                    fd,
                    statx: *statxbuf,
                }))
            }
            Self::CloseFileDescr { io_id, error } => {
                match ctx.take_ready_io(io_id) {
                    Some(_) => (),
                    None => {
                        *self = Self::CloseFileDescr { io_id, error };
                        return PollResult::Pending;
                    }
                }

                PollResult::Ready(Err(error))
            }
            Self::Dummy => unreachable!(),
        }
    }
}

pub enum Read {
    Dummy,
    Start {
        file: *const BufferedFile,
        offset: u64,
        out: *mut u8,
    },
    Read {
        io_id: usize,
    },
}

impl Future for Read {
    type Output = io::Result<usize>;

    fn poll(&mut self, ctx: &mut Context) -> PollResult<Self::Output> {
        match std::mem::replace(self, Self::Dummy) {
            Self::Start { file, offset, out } => {
                let io_id = unsafe {
                    ctx.queue_io(
                        opcode::Read::new(
                            Fd(file.fd),
                            out.as_mut_ptr(),
                            out.len().try_into().unwrap(),
                        )
                        .offset(offset)
                        .build(),
                    )
                };
                *self = Self::Read { io_id };
                PollResult::Pending
            }
            Self::Read { io_id } => {
                let ready_io = match ctx.take_ready_io(io_id) {
                    Some(ready_io) => ready_io,
                    None => {
                        *self = Self::Read { io_id };
                        return PollResult::Pending;
                    }
                };

                if ready_io.result < 0 {
                    PollResult::Ready(Err(io::Error::from_raw_os_error(-ready_io.result)))
                } else {
                    PollResult::Ready(Ok(ready_io.result.try_into().unwrap()))
                }
            }
            Self::Dummy => unreachable!(),
        }
    }
}

pub enum ReadExact {
    Dummy,
    Read {
        read: Read,
        out: *mut u8,
        to_read: usize,
        file: *const BufferedFile,
        offset: u64,
    },
}

impl<'a> Future for ReadExact<'a> {
    type Output = io::Result<()>;

    fn poll(&mut self, ctx: &mut Context) -> PollResult<Self::Output> {
        match std::mem::replace(self, Self::Dummy) {
            Self::Read {
                mut read,
                out,
                to_read,
                file,
                offset,
            } => match read.poll(ctx) {
                PollResult::Pending => PollResult::Pending,
                PollResult::Yield => PollResult::Yield,
                PollResult::Ready(n_read) => {
                    let n_read = match n_read {
                        Ok(n_read) => n_read,
                        Err(e) => return PollResult::Ready(Err(e)),
                    };
                    if n_read == to_read {
                        return PollResult::Ready(Ok(()));
                    }
                    assert!(n_read <= to_read);

                    let to_read = to_read - n_read;
                    let out = unsafe { out.add(n_read) };
                    let offset = offset + u64::try_from(n_read).unwrap();

                    let mut read = Read::Start {
                        file,
                        offset,
                        out: unsafe { std::slice::from_raw_parts_mut(out, to_read) },
                    };

                    // Start the internal read operation, should return Pending
                    assert!(matches!(read.poll(ctx), PollResult::Pending));

                    *self = Self::Read {
                        out,
                        to_read,
                        offset,
                        file,
                        read,
                    };

                    PollResult::Pending
                }
            },
            Self::Dummy => unreachable!(),
        }
    }
}

impl BufferedFile {
    pub fn open(path: &Path, flags: i32, mode: i32) -> io::Result<Open> {
        let path = CString::new(path.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidInput, e))?;
        let mut how: libc::open_how = unsafe { std::mem::zeroed() };
        how.flags = flags as u64;
        how.mode = mode as u64;
        let how = Box::new(how);
        Ok(Open::Start { path, how })
    }

    pub fn read<'a>(&'a self, offset: u64, out: &'a mut [u8]) -> Read<'a> {
        Read::Start {
            offset,
            out,
            file: self,
        }
    }

    pub fn read_exact<'a>(&'a self, offset: u64, out: &'a mut [u8]) -> ReadExact<'a> {
        ReadExact::Read {
            out: out.as_mut_ptr(),
            to_read: out.len(),
            file: self,
            read: self.read(offset, out),
            offset,
        }
    }

    pub fn close(self) -> Close {
        let fd = self.fd;
        std::mem::forget(self);
        Close::Start(fd)
    }

    pub fn statx(&self) -> libc::statx {
        self.statx
    }
}

impl Drop for BufferedFile {
    fn drop(&mut self) {
        let fd = self.fd;
        unsafe { libc::close(fd) };
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    enum TestFuture<'a> {
        Open(Open),
        ReadExact {
            file: BufferedFile,
            fut: ReadExact<'a>,
        },
        Close(Close),
        Dummy,
    }

    impl Future for TestFuture {
        type Output = ();

        fn poll(&mut self, ctx: &mut Context) -> PollResult<Self::Output> {
            match self.inner.poll(ctx) {
                PollResult::Pending => PollResult::Pending,
                PollResult::Yield => PollResult::Yield,
                PollResult::Ready(file) => {
                    let file = file.unwrap();
                    dbg!((file.fd, file.statx.stx_size));
                    PollResult::Ready(())
                }
            }
        }
    }

    #[test]
    fn test_open() {
        crate::runtime::run(
            64,
            Duration::from_millis(50),
            Box::new(TestOpen {
                inner: BufferedFile::open(Path::new("../Cargo.toml"), libc::O_RDONLY, 0).unwrap(),
            }),
        )
        .unwrap();
    }
}
