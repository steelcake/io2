use std::{
    collections::VecDeque,
    io,
    time::{Duration, Instant},
};

use io_uring::{cqueue, squeue, IoUring};
use nohash_hasher::IntMap;
use slab::Slab;

pub fn run(
    ring_depth: u32,
    preempt_duration: Duration,
    task: Box<dyn Future<Output = ()>>,
) -> io::Result<()> {
    let mut ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
        .setup_single_issuer()
        .setup_submit_all()
        .build(ring_depth)?;
    let (submitter, mut sq, mut cq) = ring.split();

    let mut tasks = Slab::<Task>::with_capacity(128);
    let mut io = Slab::<usize>::with_capacity(128);
    let mut yielded_tasks = Vec::<usize>::with_capacity(128);
    let mut io_queue = VecDeque::<IoQueueEntry>::with_capacity(128);
    let mut block_devices = IntMap::<u32, BlockDevice>::default();

    let task_id = tasks.insert(Task(task));
    yielded_tasks.push(task_id);

    while tasks.len() > 0 {
        let mut to_spawn = Vec::new();

        // poll yielded tasks
        for task_id in std::mem::take(&mut yielded_tasks) {
            let poll_result = tasks.get_mut(task_id).unwrap().0.poll(&mut Context {
                to_spawn: &mut to_spawn,
                ready_io: None,
                start: Instant::now(),
                preempt_duration,
                task_id,
                io_queue: &mut io_queue,
                io: &mut io,
                block_devices: &mut block_devices,
            });
            match poll_result {
                PollResult::Pending => (),
                PollResult::Yield => {
                    yielded_tasks.push(task_id);
                }
                PollResult::Ready(_) => {
                    tasks.remove(task_id);
                }
            }
        }

        match submitter.submit() {
            Ok(_) => (),
            Err(err) => {
                if err.raw_os_error() != Some(libc::EBUSY) {
                    panic!("failed to io_uring.submit_and_wait: {:?}", err);
                }
            }
        };
        sq.sync();

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
                    if let Err(e) = sq.push(&entry.squeue_entry) {
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
            let task_id = io.remove(io_id);
            let poll_result = tasks.get_mut(task_id).unwrap().0.poll(&mut Context {
                to_spawn: &mut to_spawn,
                ready_io: Some(ReadyIo {
                    io_id,
                    result: cqe.result(),
                }),
                start: Instant::now(),
                preempt_duration,
                task_id,
                io_queue: &mut io_queue,
                io: &mut io,
                block_devices: &mut block_devices,
            });
            match poll_result {
                PollResult::Pending => (),
                PollResult::Yield => {
                    yielded_tasks.push(task_id);
                }
                PollResult::Ready(_) => {
                    tasks.remove(task_id);
                }
            }
        }

        // spawn tasks
        for task in to_spawn {
            let task_id = tasks.insert(task);
            yielded_tasks.push(task_id);
        }
    }

    Ok(())
}

pub struct Context<'runtime> {
    to_spawn: &'runtime mut Vec<Task>,
    ready_io: Option<ReadyIo>,
    start: Instant,
    preempt_duration: Duration,
    task_id: usize,
    io_queue: &'runtime mut VecDeque<IoQueueEntry>,
    io: &'runtime mut Slab<usize>,
    block_devices: &'runtime mut IntMap<u32, BlockDevice>,
}

impl<'runtime> Context<'runtime> {
    pub fn take_ready_io(&mut self, io_id: usize) -> Option<ReadyIo> {
        if let Some(ready_io) = self.ready_io.as_ref() {
            if ready_io.io_id == io_id {
                return self.ready_io.take();
            }
        }

        None
    }

    pub fn get_block_device(&self, major: u32) -> Option<BlockDevice> {
        self.block_devices.get(&major).copied()
    }

    pub fn set_block_device(&mut self, major: u32, block_device: BlockDevice) {
        if let Some(existing_device) = self.block_devices.insert(major, block_device) {
            assert_eq!(existing_device, block_device);
        }
    }

    pub fn should_yield(&self) -> bool {
        self.start.elapsed() >= self.preempt_duration
    }

    pub fn spawn_task(&mut self, task: Box<dyn Future<Output = ()>>) {
        self.to_spawn.push(Task(task));
    }

    pub unsafe fn queue_io(&mut self, entry: squeue::Entry) -> usize {
        let io_id = self.io.insert(self.task_id);
        let entry = entry.user_data(io_id.try_into().unwrap());
        self.io_queue.push_back(IoQueueEntry {
            squeue_entry: entry,
        });
        io_id
    }
}

pub trait Future {
    type Output;

    fn poll(&mut self, ctx: &mut Context) -> PollResult<Self::Output>;
}

pub enum PollResult<T> {
    Ready(T),
    Pending,
    Yield,
}

struct Task(Box<dyn Future<Output = ()>>);

#[derive(Clone, Copy)]
pub struct ReadyIo {
    pub io_id: usize,
    pub result: i32,
}

struct IoQueueEntry {
    squeue_entry: squeue::Entry,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BlockDevice {
    pub logical_block_size: usize,
    pub max_sectors_size: usize,
    pub max_segment_size: usize,
}
