// use std::io;
// use std::path::Path;

// use crate::driver;
// use crate::io_buffer::IoBuffer;

// pub struct DmaFile {
//     handle: driver::FileHandle,
// }

// impl DmaFile {
//     pub async fn alloc_buffer(&self, size: usize) -> IoBuffer {
//         todo!()
//     }

//     pub async fn open(path: &Path, flags: i32, mode: i32) -> io::Result<Self> {
//         let handle = driver::open(path, flags, mode).await?;
//         Ok(Self { handle })
//     }

//     pub async fn close(self) -> io::Result<()> {
//         driver::close(self.handle).await
//     }

//     pub fn stats(&self) -> libc::statx {
//         self.handle.stats()
//     }
// }

// impl Drop for DmaFile {
//     fn drop(&mut self) {
//         let fd = self.handle.fd();
//         blocking::unblock(move || unsafe { libc::close(fd) }).detach();
//     }
// }
