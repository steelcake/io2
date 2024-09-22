use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io,
    os::fd::{FromRawFd, RawFd},
    pin::Pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

use io_uring::{cqueue, opcode, squeue, types::Fd, IoUring};
use nohash_hasher::{BuildNoHashHasher, IntMap};
use slab::Slab;

use crate::fs::BufferedFile;

thread_local! {
    pub static CURRENT_TASK_CONTEXT: RefCell<Option<CurrentTaskContext>> = RefCell::new(None);
    pub static FILES_TO_CLOSE: RefCell<Vec<RawFd>> = RefCell::new(Vec::with_capacity(64));
}

pub struct CurrentTaskContext {
    start: Instant,
    task_id: usize,
    to_spawn: Vec<Task>,
    io_results: IntMap<usize, i32>,
    io_queue: VecDeque<squeue::Entry>,
    preempt_duration: Duration,
    yielded: bool,
    block_devices: IntMap<u32, BlockDevice>,
    io: Slab<usize>,
}

impl CurrentTaskContext {
    pub(crate) fn take_io_result(&mut self, io_id: usize) -> Option<i32> {
        match self.io_results.remove(&io_id) {
            Some(res) => {
                self.io.remove(io_id);
                Some(res)
            }
            None => None,
        }
    }

    pub(crate) fn get_block_device(&self, major: u32) -> Option<BlockDevice> {
        self.block_devices.get(&major).copied()
    }

    pub(crate) fn set_block_device(&mut self, major: u32, block_device: BlockDevice) {
        if let Some(existing_device) = self.block_devices.insert(major, block_device) {
            assert_eq!(existing_device, block_device);
        }
    }

    pub fn should_yield(&self) -> bool {
        self.start.elapsed() >= self.preempt_duration
    }

    pub fn spawn(&mut self, task: Task) {
        self.to_spawn.push(task);
    }

    pub(crate) unsafe fn queue_io(&mut self, entry: squeue::Entry) -> usize {
        let io_id = self.io.insert(self.task_id);
        let entry = entry.user_data(io_id.try_into().unwrap());
        self.io_queue.push_back(entry);
        io_id
    }
}

type Task = Pin<Box<dyn Future<Output = ()>>>;

pub fn run(ring_depth: u32, preempt_duration: Duration, task: Task) -> io::Result<()> {
    let waker = noop_waker();
    let mut poll_ctx = Context::from_waker(&waker);

    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_single_issuer()
        .setup_submit_all()
        .build(ring_depth)?;
    let (submitter, mut sq, mut cq) = ring.split();

    let mut tasks = Slab::<Task>::with_capacity(128);
    let mut io = Slab::<usize>::with_capacity(128);
    let mut yielded_tasks = Vec::<usize>::with_capacity(128);
    let mut io_queue = VecDeque::<squeue::Entry>::with_capacity(128);
    let mut block_devices =
        IntMap::<u32, BlockDevice>::with_capacity_and_hasher(16, BuildNoHashHasher::default());
    let mut to_spawn = Vec::<Task>::with_capacity(128);
    let mut io_results = IntMap::<usize, i32>::with_capacity_and_hasher(
        usize::try_from(ring_depth).unwrap() * 3,
        BuildNoHashHasher::default(),
    );

    let close_file_task_id = tasks.insert(Box::pin(async {}));
    let close_file_io_id = io.insert(close_file_task_id);

    let task_id = tasks.insert(task);
    yielded_tasks.push(task_id);

    while tasks.len() > 1 {
        // poll yielded tasks
        for task_id in std::mem::take(&mut yielded_tasks) {
            CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                *ctx = Some(CurrentTaskContext {
                    start: Instant::now(),
                    task_id,
                    to_spawn: std::mem::take(&mut to_spawn),
                    io_results: Default::default(),
                    io_queue: std::mem::take(&mut io_queue),
                    preempt_duration,
                    yielded: false,
                    block_devices: std::mem::take(&mut block_devices),
                    io: std::mem::take(&mut io),
                });
            });
            let poll_result = tasks.get_mut(task_id).unwrap().as_mut().poll(&mut poll_ctx);
            let yielded = CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                let ctx = ctx.take().unwrap();
                to_spawn = ctx.to_spawn;
                io_queue = ctx.io_queue;
                block_devices = ctx.block_devices;
                io = ctx.io;
                ctx.yielded
            });
            match poll_result {
                Poll::Pending => {
                    if yielded {
                        yielded_tasks.push(task_id);
                    }
                }
                Poll::Ready(_) => {
                    let _ = tasks.remove(task_id);
                }
            }
        }

        sq.sync();
        if !sq.is_empty() {
            match submitter.submit() {
                Ok(_) => (),
                Err(err) => {
                    if err.raw_os_error() != Some(libc::EBUSY) {
                        panic!("failed to io_uring.submit_and_wait: {:?}", err);
                    }
                }
            };
            sq.sync();
        }

        // submit queued IO into uring
        loop {
            if sq.is_full() {
                match submitter.submit() {
                    Ok(_) => (),
                    Err(err) => {
                        if err.raw_os_error() != Some(libc::EBUSY) {
                            panic!("failed to io_uring.submit_and_wait: {:?}", err);
                        }
                        break;
                    }
                };
                sq.sync();
            }

            match io_queue.pop_front() {
                Some(entry) => unsafe {
                    if let Err(e) = sq.push(&entry) {
                        panic!("io_uring tried to push to sq while it was full: {:?}", e);
                    }
                },
                None => break,
            }
        }

        // poll tasks with finished io
        cq.sync();
        for cqe in &mut cq {
            let io_id = usize::try_from(cqe.user_data()).unwrap();
            if io_id == close_file_io_id {
                continue;
            }
            let task_id = *io.get(io_id).unwrap();
            io_results.insert(io_id, cqe.result());

            CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                *ctx = Some(CurrentTaskContext {
                    start: Instant::now(),
                    task_id,
                    to_spawn: std::mem::take(&mut to_spawn),
                    io_results: std::mem::take(&mut io_results),
                    io_queue: std::mem::take(&mut io_queue),
                    preempt_duration,
                    yielded: false,
                    block_devices: std::mem::take(&mut block_devices),
                    io: std::mem::take(&mut io),
                });
            });
            let poll_result = tasks.get_mut(task_id).unwrap().as_mut().poll(&mut poll_ctx);
            let yielded = CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                let ctx = ctx.take().unwrap();
                to_spawn = ctx.to_spawn;
                io_queue = ctx.io_queue;
                io_results = ctx.io_results;
                block_devices = ctx.block_devices;
                io = ctx.io;
                ctx.yielded
            });
            match poll_result {
                Poll::Pending => {
                    if yielded {
                        yielded_tasks.push(task_id);
                    }
                }
                Poll::Ready(_) => {
                    let _ = tasks.remove(task_id);
                }
            }
        }

        // spawn tasks
        while let Some(task) = to_spawn.pop() {
            let task_id = tasks.insert(task);
            yielded_tasks.push(task_id);
        }

        // close files
        FILES_TO_CLOSE.with_borrow_mut(|files| {
            while let Some(fd) = files.pop() {
                io_queue.push_back(opcode::Close::new(Fd(fd)).build().user_data(close_file_io_id.try_into().unwrap()));
            }
        });
    }

    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BlockDevice {
    pub logical_block_size: usize,
    pub max_sectors_size: usize,
    pub max_segment_size: usize,
}

unsafe fn noop_clone(_data: *const ()) -> RawWaker {
    noop_raw_waker()
}

unsafe fn noop(_data: *const ()) {}

const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);

const fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
}

#[inline]
pub fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(noop_raw_waker()) }
}
