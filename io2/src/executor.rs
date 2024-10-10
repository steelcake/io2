use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io,
    os::fd::RawFd,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

use io_uring::{cqueue, opcode, squeue, types::Fd, IoUring};

use crate::{local_alloc::LocalAlloc, slab, vecmap::VecMap};

thread_local! {
    pub(crate) static CURRENT_TASK_CONTEXT: RefCell<Option<CurrentTaskContext>> = const { RefCell::new(None) };
    pub(crate) static FILES_TO_CLOSE: RefCell<Vec<RawFd, LocalAlloc>> = RefCell::new(Vec::with_capacity_in(128, LocalAlloc::new()));
}

type IoResults = VecMap<slab::Key, i32, LocalAlloc>;
type BlockDeviceInfos = VecMap<u32, BlockDeviceInfo, LocalAlloc>;
type ToNotify = VecMap<slab::Key, (), LocalAlloc>;
type Task = Pin<Box<dyn Future<Output = ()>, LocalAlloc>>;

pub(crate) struct CurrentTaskContext {
    start: Instant,
    task_id: slab::Key,
    tasks: *mut slab::Slab<Task, LocalAlloc>,
    io_results: *mut IoResults,
    io_queue: *mut VecDeque<squeue::Entry, LocalAlloc>,
    preempt_duration: Duration,
    block_device_infos: *mut BlockDeviceInfos,
    io: *mut slab::Slab<slab::Key, LocalAlloc>,
    to_notify: *mut ToNotify,
}

// This is to clear data in CURRENT_TASK_CONTEXT in case one of the tasks panic while getting polled
struct CurrentTaskContextGuard;

impl Drop for CurrentTaskContextGuard {
    fn drop(&mut self) {
        CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
            *ctx = None;
        });
    }
}

impl CurrentTaskContext {
    fn notify(&mut self, task_id: slab::Key) {
        unsafe {
            (*self.to_notify).insert(task_id, ());
        }
    }

    pub(crate) fn take_io_result(&mut self, io_id: slab::Key) -> Option<i32> {
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

    fn yield_if_needed(&self) -> bool {
        if self.start.elapsed() < self.preempt_duration {
            false
        } else {
            unsafe { (*self.to_notify).insert(self.task_id, ()) };
            true
        }
    }

    pub(crate) fn spawn<T: 'static, F: Future<Output = T> + 'static>(
        &mut self,
        future: F,
    ) -> JoinHandle<T> {
        let out = Rc::pin_in(RefCell::new(None), LocalAlloc::new());
        let join_handle = JoinHandle { out: out.clone() };
        let caller_task_id = self.task_id;
        let task = Box::pin_in(
            async move {
                *out.borrow_mut() = Some(future.await);
                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    let ctx = ctx.as_mut().unwrap();
                    ctx.notify(caller_task_id);
                });
            },
            LocalAlloc::new(),
        );

        let task_id = unsafe { (*self.tasks).insert(task) };
        self.notify(task_id);
        join_handle
    }

    /// Task will be pinned until the entry is completely processed by io_uring.
    /// So it is safe to include pinned pointers to self when building the squeue entry.
    ///
    /// Safety: Caller must make sure the squeue entry is valid as long as the caller future is pinned.
    /// Caller future should be careful about returning Poll::Ready before all io is complete because the executor will
    /// drop the future if it returns Poll::Ready and this might invalidate some io operation it queued
    /// while it is running in the kernel.
    pub(crate) unsafe fn queue_io(&mut self, entry: squeue::Entry) -> slab::Key {
        let io_id = (*self.io).insert(self.task_id);
        let entry = entry.user_data(io_id.into());
        (*self.io_queue).push_back(entry);
        io_id
    }
}

pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(future: F) -> JoinHandle<T> {
    CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
        let ctx = ctx.as_mut().unwrap();
        ctx.spawn(future)
    })
}

pub struct ExecutorConfig {
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
            ring_depth: 64,
            preempt_duration: Duration::from_millis(100),
        }
    }

    pub fn ring_depth(mut self, ring_depth: u32) -> Self {
        self.ring_depth = ring_depth;
        self
    }

    pub fn preempt_duration(mut self, preempt_duration: Duration) -> Self {
        self.preempt_duration = preempt_duration;
        self
    }

    pub fn run<T: 'static, F: Future<Output = T> + 'static>(self, future: F) -> io::Result<T> {
        run(self.ring_depth, self.preempt_duration, future)
    }
}

// TODO: Don't leak the file descriptors in FILES_TO_CLOSE when returning error.
// this is almost ok since they will be cleaned when/if another executor runs in this thread. But
// is a problem if user is spawning more and more threads and running executors in them.
fn run<T: 'static, F: Future<Output = T> + 'static>(
    ring_depth: u32,
    preempt_duration: Duration,
    future: F,
) -> io::Result<T> {
    // This is to cleanup the thread local variable if there is a panic.
    // It makes sure we are panic/unwind safe.
    // If we don't set CURRENT_TASK_CONTEXT to none on panic using this, it will have dangling pointers which will cause memory unsafety.
    let _current_task_context_guard = CurrentTaskContextGuard;

    let mut out = Option::<T>::None;
    let out_ptr = &mut out as *mut Option<T>;
    let task = Box::pin_in(
        async move {
            unsafe {
                *out_ptr = Some(future.await);
            }
        },
        LocalAlloc::new(),
    );

    let waker = noop_waker();
    let mut poll_ctx = Context::from_waker(&waker);

    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_single_issuer()
        .setup_submit_all()
        .setup_coop_taskrun()
        .build(ring_depth)?;

    let mut tasks = slab::Slab::<Task, LocalAlloc>::with_capacity_in(128, LocalAlloc::new());
    let mut io = slab::Slab::<slab::Key, LocalAlloc>::with_capacity_in(128, LocalAlloc::new());
    let mut io_queue =
        VecDeque::<squeue::Entry, LocalAlloc>::with_capacity_in(128, LocalAlloc::new());
    let mut block_device_infos = BlockDeviceInfos::with_capacity_in(16, LocalAlloc::new());
    let mut io_results =
        IoResults::with_capacity_in(usize::try_from(ring_depth).unwrap() * 4, LocalAlloc::new());
    let mut to_notify = ToNotify::with_capacity_in(128, LocalAlloc::new());
    let mut notifying = Vec::<slab::Key, LocalAlloc>::with_capacity_in(128, LocalAlloc::new());

    let close_file_task_id = tasks.insert(Box::pin_in(async {}, LocalAlloc::new()));
    let close_file_io_id = io.insert(close_file_task_id);
    let mut files_closing = 0usize;

    let task_id = tasks.insert(task);
    to_notify.insert(task_id, ());

    while out.is_none() || files_closing > 0 || FILES_TO_CLOSE.with_borrow(|x| !x.is_empty()) {
        let last_io_poll = Instant::now();

        // run notified tasks
        while !to_notify.is_empty() {
            notifying.clear();
            notifying.extend(to_notify.iter().map(|kv| kv.0));
            to_notify.clear();
            for &task_id in notifying.iter() {
                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    *ctx = Some(CurrentTaskContext {
                        start: last_io_poll,
                        task_id,
                        // This is safe because slab contains only pointers to actual tasks,
                        // we take a pointer and execute our task through it.
                        // Even if the running tasks spawn another task and the pointer of the running task moves in the slab,
                        // the actual task doesn't move.
                        tasks: &mut tasks,
                        io_results: &mut io_results,
                        io_queue: &mut io_queue,
                        preempt_duration,
                        block_device_infos: &mut block_device_infos,
                        io: &mut io,
                        to_notify: &mut to_notify,
                    });
                });
                let poll_result = tasks
                    .get_mut(task_id)
                    .map(|task| task.as_mut().poll(&mut poll_ctx));
                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    let _ = ctx.take().unwrap();
                });
                let poll_result = match poll_result {
                    Some(p) => p,
                    None => continue,
                };
                match poll_result {
                    Poll::Pending => {}
                    Poll::Ready(_) => {
                        std::mem::drop(tasks.remove(task_id));
                    }
                }
            }

            try_submit_io(&mut io_queue, &mut ring);

            if last_io_poll.elapsed() > preempt_duration {
                break;
            }
        }

        try_submit_io(&mut io_queue, &mut ring);

        // submit queued IO into uring
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let io_id = slab::Key::from(cqe.user_data());
            if io_id == close_file_io_id {
                files_closing = files_closing.checked_sub(1).unwrap();
                continue;
            }
            let task_id = *io.get(io_id).unwrap();
            io_results.insert(io_id, cqe.result());
            to_notify.insert(task_id, ());
        }

        // close files
        FILES_TO_CLOSE.with_borrow_mut(|files| {
            for &fd in files.iter() {
                files_closing = files_closing.checked_add(1).unwrap();
                io_queue.push_back(
                    opcode::Close::new(Fd(fd))
                        .build()
                        .user_data(close_file_io_id.into()),
                );
            }
            files.clear();
        });
    }

    Ok(out.unwrap())
}

fn try_submit_io(io_queue: &mut VecDeque<squeue::Entry, LocalAlloc>, ring: &mut IoUring) {
    let (submitter, mut sq, _) = ring.split();

    sq.sync();
    while !io_queue.is_empty() {
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
            // The unsafety is moved to CurrentTaskContext::queue_io function
            // We require the caller of that function to give a valid squeue entry so the push call here should be safe.
            Some(entry) => unsafe {
                if let Err(e) = sq.push(&entry) {
                    panic!("io_uring tried to push to sq while it was full: {:?}", e);
                }
            },
            None => break,
        }
    }

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
            if !ctx.yield_if_needed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }
}

pub struct JoinHandle<T> {
    out: Pin<Rc<RefCell<Option<T>>, LocalAlloc>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().out.take() {
            Some(v) => Poll::Ready(v),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    #[test]
    fn test_spawn() {
        let r = ExecutorConfig::new()
            .run(async {
                for _ in 0..5 {
                    YieldIfNeeded.await;
                }

                let handle1 = spawn(async {
                    YieldIfNeeded.await;
                    1
                });

                YieldIfNeeded.await;

                let handle2 = spawn(async { 2 });

                YieldIfNeeded.await;

                assert_eq!(2, handle2.await);
                assert_eq!(1, handle1.await);

                0
            })
            .unwrap();
        assert_eq!(r, 0);
    }

    #[test]
    fn test_unwind_cleanup() {
        let _ = catch_unwind(|| {
            ExecutorConfig::new()
                .run(async { panic!("unwind to leak CURRENT_TASK_CONTEXT") })
                .unwrap();
        });

        assert!(CURRENT_TASK_CONTEXT.with_borrow_mut(|x| x.is_none()));
    }
}
