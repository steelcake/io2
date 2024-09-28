use std::{
    alloc::Allocator,
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io,
    os::fd::RawFd,
    pin::Pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

use hashbrown::{HashMap, HashSet};
use io_uring::{cqueue, opcode, squeue, types::Fd, IoUring};
use nohash_hasher::BuildNoHashHasher;

use crate::slab::Slab;

thread_local! {
    pub static CURRENT_TASK_CONTEXT: RefCell<Option<CurrentTaskContext>> = const { RefCell::new(None) };
    pub static FILES_TO_CLOSE: RefCell<Vec<RawFd>> = RefCell::new(Vec::with_capacity(64));
}

type DynAllocator = &'static dyn Allocator;
type IoResults = HashMap<usize, i32, BuildNoHashHasher<usize>, DynAllocator>;
type BlockDeviceInfos = HashMap<u32, BlockDeviceInfo, BuildNoHashHasher<u32>, DynAllocator>;
type ToNotify = HashSet<usize, BuildNoHashHasher<usize>, DynAllocator>;
type Task = Pin<Box<dyn Future<Output = ()>, DynAllocator>>;

pub struct CurrentTaskContext {
    start: Instant,
    task_id: usize,
    to_spawn: *mut Vec<Task, DynAllocator>,
    io_results: *mut IoResults,
    io_queue: *mut VecDeque<squeue::Entry, DynAllocator>,
    preempt_duration: Duration,
    yielded: bool,
    block_device_infos: *mut BlockDeviceInfos,
    io: *mut Slab<usize>,
    alloc: DynAllocator,
}

impl CurrentTaskContext {
    pub(crate) fn take_io_result(&mut self, io_id: usize) -> Option<i32> {
        unsafe {
            match (*self.io_results).remove(&io_id) {
                Some(res) => {
                    (*self.io).remove(io_id);
                    Some(res)
                }
                None => None,
            }
        }
    }

    pub(crate) fn get_block_device_info(&self, major: u32) -> Option<BlockDeviceInfo> {
        unsafe { (*self.block_device_infos).get(&major).copied() }
    }

    pub(crate) fn set_block_device_info(&mut self, major: u32, block_device: BlockDeviceInfo) {
        unsafe {
            if let Some(existing_device) = (*self.block_device_infos).insert(major, block_device) {
                assert_eq!(existing_device, block_device);
            }
        }
    }

    pub fn should_yield(&self) -> bool {
        self.start.elapsed() >= self.preempt_duration
    }

    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, future: F) {
        unsafe {
            (*self.to_spawn).push(Box::pin_in(future, self.alloc));
        }
    }

    /// Task will be pinned until the entry is completely processed by io_uring.
    /// So it is safe to include pinned pointers to self when building the squeue entry.
    ///
    /// Safety: caller must make sure the squeue entry is valid.
    /// Caller future that queued this IO should not return Poll::Ready until it receives
    /// io completion via take_io_result
    pub(crate) unsafe fn queue_io(&mut self, entry: squeue::Entry) -> usize {
        unsafe {
            let io_id = (*self.io).insert(self.task_id);
            let entry = entry.user_data(io_id.try_into().unwrap());
            (*self.io_queue).push_back(entry);
            io_id
        }
    }
}

pub struct ExecutorConfig {
    alloc: DynAllocator,
    ring_depth: u32,
    preempt_duration: Duration,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorConfig {
    pub fn new() -> Self {
        Self {
            alloc: &std::alloc::Global,
            ring_depth: 64,
            preempt_duration: Duration::from_millis(100),
        }
    }

    pub fn alloc(mut self, alloc: DynAllocator) -> Self {
        self.alloc = alloc;
        self
    }

    pub fn ring_depth(mut self, ring_depth: u32) -> Self {
        self.ring_depth = ring_depth;
        self
    }

    pub fn preempt_duration(mut self, preempt_duration: Duration) -> Self {
        self.preempt_duration = preempt_duration;
        self
    }

    pub fn run<F: Future<Output = ()> + 'static>(self, future: F) -> io::Result<()> {
        run(
            self.alloc,
            self.ring_depth,
            self.preempt_duration,
            Box::pin_in(future, self.alloc),
        )
    }
}

fn run(
    alloc: DynAllocator,
    ring_depth: u32,
    preempt_duration: Duration,
    task: Task,
) -> io::Result<()> {
    let waker = noop_waker();
    let mut poll_ctx = Context::from_waker(&waker);

    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_single_issuer()
        .setup_submit_all()
        .setup_coop_taskrun()
        .build(ring_depth)?;
    let (submitter, mut sq, mut cq) = ring.split();

    let mut tasks = Slab::<Task>::with_capacity_in(128, alloc);
    let mut io = Slab::<usize>::with_capacity_in(128, alloc);
    let mut yielded_tasks = Vec::<usize, DynAllocator>::with_capacity_in(16, alloc);
    let mut tasks_to_yield = Vec::<usize, DynAllocator>::with_capacity_in(16, alloc);
    let mut io_queue = VecDeque::<squeue::Entry, DynAllocator>::with_capacity_in(128, alloc);
    let mut block_device_infos =
        BlockDeviceInfos::with_capacity_and_hasher_in(16, BuildNoHashHasher::default(), alloc);
    let mut to_spawn = Vec::<Task, DynAllocator>::with_capacity_in(128, alloc);
    let mut io_results = IoResults::with_capacity_and_hasher_in(
        usize::try_from(ring_depth).unwrap() * 4,
        BuildNoHashHasher::default(),
        alloc,
    );
    let mut to_notify =
        ToNotify::with_capacity_and_hasher_in(128, BuildNoHashHasher::default(), alloc);

    let close_file_task_id = tasks.insert(Box::pin_in(async {}, alloc));
    let close_file_io_id = io.insert(close_file_task_id);
    let mut files_closing = 0usize;

    let task_id = tasks.insert(task);
    yielded_tasks.push(task_id);

    while tasks.len() > 1 || files_closing > 0 || FILES_TO_CLOSE.with_borrow(|x| !x.is_empty()) {
        // poll yielded tasks
        for &task_id in yielded_tasks.iter() {
            CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                *ctx = Some(CurrentTaskContext {
                    start: Instant::now(),
                    task_id,
                    to_spawn: &mut to_spawn,
                    io_results: &mut io_results,
                    io_queue: &mut io_queue,
                    preempt_duration,
                    yielded: false,
                    block_device_infos: &mut block_device_infos,
                    io: &mut io,
                    alloc,
                });
            });
            let poll_result = tasks.get_mut(task_id).unwrap().as_mut().poll(&mut poll_ctx);
            let yielded = CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                let ctx = ctx.take().unwrap();
                ctx.yielded
            });
            match poll_result {
                Poll::Pending => {
                    if yielded {
                        tasks_to_yield.push(task_id);
                    }
                }
                Poll::Ready(_) => {
                    std::mem::drop(tasks.remove(task_id));
                }
            }
        }
        yielded_tasks.clear();
        std::mem::swap(&mut yielded_tasks, &mut tasks_to_yield);

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

        cq.sync();
        for cqe in &mut cq {
            let io_id = usize::try_from(cqe.user_data()).unwrap();
            if io_id == close_file_io_id {
                files_closing = files_closing.checked_sub(1).unwrap();
                continue;
            }
            let task_id = *io.get(io_id).unwrap();
            io_results.insert(io_id, cqe.result());
            to_notify.insert(task_id);
        }

        // poll tasks with finished io
        for &task_id in to_notify.iter() {
            CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                *ctx = Some(CurrentTaskContext {
                    start: Instant::now(),
                    task_id,
                    to_spawn: &mut to_spawn,
                    io_results: &mut io_results,
                    io_queue: &mut io_queue,
                    preempt_duration,
                    yielded: false,
                    block_device_infos: &mut block_device_infos,
                    io: &mut io,
                    alloc,
                });
            });
            let poll_result = tasks.get_mut(task_id).unwrap().as_mut().poll(&mut poll_ctx);
            let yielded = CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                let ctx = ctx.take().unwrap();
                ctx.yielded
            });
            match poll_result {
                Poll::Pending => {
                    if yielded {
                        yielded_tasks.push(task_id);
                    }
                }
                Poll::Ready(_) => {
                    std::mem::drop(tasks.remove(task_id));
                }
            }
        }
        to_notify.clear();

        // spawn tasks
        while let Some(task) = to_spawn.pop() {
            let task_id = tasks.insert(task);
            yielded_tasks.push(task_id);
        }

        // close files
        FILES_TO_CLOSE.with_borrow_mut(|files| {
            while let Some(fd) = files.pop() {
                files_closing = files_closing.checked_add(1).unwrap();
                io_queue.push_back(
                    opcode::Close::new(Fd(fd))
                        .build()
                        .user_data(close_file_io_id.try_into().unwrap()),
                );
            }
        });
    }

    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BlockDeviceInfo {
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

pub struct YieldIfNeeded;

impl Future for YieldIfNeeded {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            let ctx = ctx.as_mut().unwrap();
            if ctx.should_yield() {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
    }
}
