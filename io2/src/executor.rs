use std::{
    alloc::Global as GlobalAlloc,
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

use crate::{slab, vecmap::VecMap};

thread_local! {
    pub(crate) static CURRENT_TASK_CONTEXT: RefCell<Option<CurrentTaskContext>> = const { RefCell::new(None) };
    pub(crate) static FILES_TO_CLOSE: RefCell<Vec<RawFd>> = RefCell::new(Vec::with_capacity(128));
}

type IoResults = VecMap<slab::Key, i32>;
type ToNotify = VecMap<slab::Key, ()>;
type Task = Pin<Box<dyn Future<Output = ()>>>;

struct TaskEntry {
    // number of IO that is currently running in io_uring
    num_io: usize,
    // finished io
    finished_io: Vec<slab::Key>,
    // false if the task finished it's execution
    finished: bool,
    task: Task,
}

struct NotifyWhen {
    timer: Vec<Instant>,
    task_id: Vec<slab::Key>,
}

pub(crate) struct CurrentTaskContext {
    start: Instant,
    task_id: slab::Key,
    tasks: *mut slab::Slab<TaskEntry>,
    io_results: *mut IoResults,
    io_queue: *mut VecDeque<squeue::Entry>,
    dio_queue: *mut VecDeque<squeue::Entry>,
    preempt_duration: Duration,
    io: *mut slab::Slab<slab::Key>,
    to_notify: *mut ToNotify,
    notify_when: *mut NotifyWhen,
    num_dio_running: *mut usize,
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
                    {
                        let task = (*self.tasks).get_mut(self.task_id).unwrap();
                        let idx = task.finished_io.iter().position(|&id| id == io_id).unwrap();
                        task.finished_io.swap_remove(idx);
                    }
                    (*self.io).remove(io_id);
                    Some(res)
                }
                None => None,
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
        let out = Rc::pin(RefCell::new(None));
        let join_handle = JoinHandle { out: out.clone() };
        let caller_task_id = self.task_id;
        let task = Box::pin(async move {
            *out.borrow_mut() = Some(future.await);
            CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                let ctx = ctx.as_mut().unwrap();
                ctx.notify(caller_task_id);
            });
        });

        let task_id = unsafe {
            (*self.tasks).insert(TaskEntry {
                task,
                num_io: 0,
                finished_io: Vec::new(),
                finished: false,
            })
        };
        self.notify(task_id);
        join_handle
    }

    /// Task will be pinned until the entry is completely processed by io_uring.
    /// So it is safe to include pinned pointers to self when building the squeue entry.
    ///
    /// Safety: Caller must make sure the squeue entry is valid as long as the caller future is pinned.
    pub(crate) unsafe fn queue_io(&mut self, entry: squeue::Entry, direct_io: bool) -> slab::Key {
        {
            let task = (*self.tasks).get_mut(self.task_id).unwrap();
            task.num_io = task.num_io.checked_add(1).unwrap();
        }
        let io_id = (*self.io).insert(self.task_id);
        let entry = entry.user_data(io_id.into());
        let queue = if direct_io {
            *self.num_dio_running = (*self.num_dio_running).checked_add(1).unwrap();
            self.dio_queue
        } else {
            self.io_queue
        };
        (*queue).push_back(entry);
        io_id
    }

    pub(crate) fn notify_when(&mut self, when: Instant) {
        unsafe {
            let n = &mut *self.notify_when;
            n.timer.push(when);
            n.task_id.push(self.task_id);
        };
    }
}

/// Spawns a future to run in the background.
///
/// This should only be used if the future to be spawned is doing significant CPU work,
/// otherwise it is recommended to just nest it into the current future using mechanisms like `futures::future::join` and similar.
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
            preempt_duration: Duration::from_millis(10),
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
    let task = Box::pin(async move {
        unsafe {
            *out_ptr = Some(future.await);
        }
    });

    let waker = noop_waker();
    let mut poll_ctx = Context::from_waker(&waker);

    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_single_issuer()
        .setup_submit_all()
        .setup_coop_taskrun()
        .build(ring_depth)?;
    let mut dio_ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_single_issuer()
        .setup_submit_all()
        .setup_coop_taskrun()
        .setup_iopoll()
        .build(ring_depth)?;

    let mut tasks = slab::Slab::<TaskEntry>::with_capacity_in(128, GlobalAlloc);
    let mut io = slab::Slab::<slab::Key>::with_capacity_in(128, GlobalAlloc);
    let mut io_queue = VecDeque::<squeue::Entry>::with_capacity(128);
    let mut dio_queue = VecDeque::<squeue::Entry>::with_capacity(128);
    let mut io_results =
        IoResults::with_capacity_in(usize::try_from(ring_depth).unwrap() * 4, GlobalAlloc);
    let mut to_notify = ToNotify::with_capacity_in(128, GlobalAlloc);
    let mut notifying = Vec::<slab::Key>::with_capacity(128);
    let mut notify_when = NotifyWhen {
        timer: Vec::<Instant>::with_capacity(128),
        task_id: Vec::<slab::Key>::with_capacity(128),
    };
    let mut num_dio_running = 0usize;

    let close_file_task_id = tasks.insert(TaskEntry {
        num_io: 0,
        task: Box::pin(async {}),
        finished: false,
        finished_io: Vec::new(),
    });
    let close_file_io_id = io.insert(close_file_task_id);
    let mut files_closing = 0usize;

    let task_id = tasks.insert(TaskEntry {
        task,
        num_io: 0,
        finished: false,
        finished_io: Vec::new(),
    });
    to_notify.insert(task_id, ());

    while out.is_none() || files_closing > 0 || FILES_TO_CLOSE.with_borrow(|x| !x.is_empty()) {
        {
            let (_, sq, mut cq) = ring.split();
            let (dio_submitter, dio_sq, mut dio_cq) = dio_ring.split();

            // nothing to submit, nothing completed yet and there are no tasks to run
            if sq.is_empty()
                && cq.is_empty()
                && to_notify.is_empty()
                && io_queue.is_empty()
                && FILES_TO_CLOSE.with_borrow(|x| x.is_empty())
                && dio_sq.is_empty()
                && dio_cq.is_empty()
                && dio_queue.is_empty()
            {
                'wait: loop {
                    for _ in 0..16 {
                        if cq.is_empty() && dio_cq.is_empty() && to_notify.is_empty() {
                            notify_timers(&mut notify_when, &mut to_notify);
                            cq.sync();
                            if num_dio_running > 0 {
                                match dio_submitter.submit_and_wait(0) {
                                    Ok(_) => (),
                                    Err(err) => {
                                        if err.raw_os_error() != Some(libc::EBUSY) {
                                            panic!("failed to io_uring.submit_and_wait on direct_io ring: {:?}", err);
                                        }
                                    }
                                }
                                dio_cq.sync();
                            }
                        } else {
                            break 'wait;
                        }
                    }
                    // Not sure if this is the best way to do it. It gives more latency than std::thread::yield_now() (apparently should never use yield_now in linux)
                    // but it makes cpu usage negligible if all we are doing is waiting for some io.
                    // Anyway it is better than using 100% cpu when we are only waiting for io.
                    std::thread::sleep(Duration::from_nanos(1));
                }
            }
        }

        let start = Instant::now();
        if !to_notify.is_empty() {
            notifying.extend(to_notify.iter_keys());
            to_notify.clear();
            while let Some(task_id) = notifying.pop() {
                let task_start = Instant::now();
                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    *ctx = Some(CurrentTaskContext {
                        start,
                        task_id,
                        // This is safe because slab contains only pointers to actual tasks,
                        // we take a pointer and execute our task through it.
                        // Even if the running tasks spawn another task and the pointer of the running task moves in the slab,
                        // the actual task doesn't move.
                        tasks: &mut tasks,
                        io_results: &mut io_results,
                        io_queue: &mut io_queue,
                        dio_queue: &mut dio_queue,
                        preempt_duration,
                        io: &mut io,
                        to_notify: &mut to_notify,
                        notify_when: &mut notify_when,
                        num_dio_running: &mut num_dio_running,
                    });
                });
                let poll_result = {
                    let task_ref = match tasks.get_mut(task_id) {
                        Some(task_ref) => task_ref,
                        None => continue,
                    };
                    if !task_ref.finished {
                        task_ref.task.as_mut().poll(&mut poll_ctx)
                    } else {
                        Poll::Ready(())
                    }
                };
                if task_start.elapsed() > preempt_duration {
                    log::warn!("a task is using too much cpu time, this might cause other tasks to starve. calling yield_if_needed() more frequently should fix this.");
                }
                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    let _ = ctx.take().unwrap();
                });
                match poll_result {
                    Poll::Pending => {}
                    Poll::Ready(_) => {
                        let task = tasks.get_mut(task_id).unwrap();
                        // set finished marker so this task is not polled again
                        // we still want it alive and the memory intact because it might
                        // have given pointers of itself to io_uring, we want to wait for
                        // the io_uring entries to finish running before destroying/modifying the
                        // memory of the task at all
                        task.finished = true;

                        // If there is no io remaning in the uring, we should clean everything
                        // relating to this task and finally destroy it.
                        // If there is remaning IO, the io_uring cqe handling loop will do the
                        // cleanup.
                        if task.num_io == 0 {
                            let task = tasks.remove(task_id).unwrap();
                            for io_id in task.finished_io {
                                io.remove(io_id).unwrap();
                            }

                            // Not deleting the notification won't cause UB. Only thing that
                            // can happen is we remove this task. Another task is spawned and is
                            // assigned the same id. Some later task in the loop intends to notify
                            // this task but ends up notifying the new task instead.
                            //
                            // This is all fine since notifying random tasks for no reason is ok.
                            to_notify.remove(&task_id);
                        }
                    }
                }

                if start.elapsed() > preempt_duration {
                    break;
                }

                try_submit_io(&mut io_queue, &mut ring, false);
                try_submit_io(&mut dio_queue, &mut dio_ring, false);
            }
        }

        try_submit_io(&mut io_queue, &mut ring, false);
        try_submit_io(&mut dio_queue, &mut dio_ring, true);

        let mut dio_cq = dio_ring.completion();
        let mut cq = ring.completion();
        cq.sync();
        dio_cq.sync();
        num_dio_running = num_dio_running.checked_sub(dio_cq.len()).unwrap();
        for cqe in cq.chain(dio_cq) {
            let io_id = slab::Key::from(cqe.user_data());
            if io_id == close_file_io_id {
                files_closing = files_closing.checked_sub(1).unwrap();
                continue;
            }
            let task_id = *io.get(io_id).unwrap();

            io_results.insert(io_id, cqe.result());
            to_notify.insert(task_id, ());

            let task = tasks.get_mut(task_id).unwrap();
            task.num_io = task.num_io.checked_sub(1).unwrap();
            task.finished_io.push(io_id);

            // If task had already finished execution and we have io coming out of uring,
            // we need to check if there are any remaning io in the uring and delete the task
            // completely if there is no io remaning in uring. Doing this when there is remaning io
            // in the uring is unsafe.
            //
            // Doing this here is not really needed since the task will be notified and the
            // poll loop will check it and delete the resources when it realises there is no
            // pending io on it and it finished execution.
            // But we do it here anyways to short circuit and not waste more time on this.
            //
            // TODO: it is smart to refactor this into a function to not repeat it two times.
            if task.finished && task.num_io == 0 {
                let task = tasks.remove(task_id).unwrap();
                for io_id in task.finished_io {
                    io.remove(io_id).unwrap();
                }
                to_notify.remove(&task_id);
            }
        }

        notify_timers(&mut notify_when, &mut to_notify);

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

fn notify_timers(notify_when: &mut NotifyWhen, to_notify: &mut VecMap<slab::Key, ()>) {
    let time = Instant::now();
    let mut i = 0;
    loop {
        if i >= notify_when.timer.len() {
            break;
        }

        let timer = *notify_when.timer.get(i).unwrap();
        if timer >= time {
            i += 1;
        } else {
            notify_when.timer.swap_remove(i);
            let task_id = notify_when.task_id.swap_remove(i);
            to_notify.insert(task_id, ());
        }
    }
}

fn try_submit_io(io_queue: &mut VecDeque<squeue::Entry>, ring: &mut IoUring, force_submit: bool) {
    let (submitter, mut sq, _) = ring.split();

    while !io_queue.is_empty() {
        if sq.is_full() {
            sq.sync();
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

    if force_submit || !sq.is_empty() {
        sq.sync();
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

#[must_use = "futures do nothing unless you `.await` or poll them"]
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
    out: Pin<Rc<RefCell<Option<T>>>>,
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
