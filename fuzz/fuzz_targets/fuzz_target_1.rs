#![feature(allocator_api)]

#![no_main]

use std::alloc::Layout;
use std::time::Duration;
use std::path::PathBuf;

use io2::io_buffer::IoBuffer;
use libc::O_CREAT;
use libfuzzer_sys::{arbitrary, fuzz_target};
use rand::RngCore;

use io2::executor::{JoinHandle, ExecutorConfig, spawn, yield_if_needed};
use io2::fs::dio_file::DioFile;
use io2::fs::file::File;
use io2::time::sleep;
use uuid::Uuid;

#[derive(Clone, Debug, arbitrary::Arbitrary)]
enum Operation {
    Open,
    Close {
        file_index: usize,
    },
    Drop {
        file_index: usize,
    },
    Read {
        file_index: usize,
        offset: u64,
        size: usize,
    },
    Write {
        file_index: usize,
        offset: u64,
        size: usize,
    },
    Sleep {
        time: Duration,
    },
    Spawn {
        ops: Vec<Operation>,
    },
    YieldIfNeeded,
    Join {
        task_index: usize,
    },
}

fn make_iov(dio_file: &DioFile, file_size: u64, offset: u64, size: usize) -> (u64, usize) {
    let iov = (offset, size);

    let iov_end = iov.0 + u64::try_from(iov.1).unwrap();
    let iov_end = iov_end.min(file_size+1);

    let iov_start = iov_end.saturating_sub(u64::try_from(iov.1).unwrap()); 
    let iov_len = usize::try_from(iov_end-iov_start).unwrap();
    let iov = (iov_start, iov_len);

    dio_file.align_iov(iov)
}

#[derive(Default)]
struct State {
    files: Vec<File>,
    dio_files: Vec<DioFile>,
    tasks: Vec<JoinHandle<()>>,
    num_files_created: usize,
}

const MAX_NUM_FILES: usize = 16;
const FILE_SIZE: usize = 1 << 27;

fn make_file_path() -> PathBuf {
    let mut path = PathBuf::from("data");
    path.push(Uuid::new_v4().to_string());
    path
}

async fn run_op(state: &mut State, op: Operation) {
    match op {
        Operation::Open => {
            if state.num_files_created >= MAX_NUM_FILES {
                return;
            }

            state.num_files_created += 1;

            let file = File::open(&make_file_path(), libc::O_RDWR | O_CREAT, 0).unwrap().await.unwrap();
            let dio_file = DioFile::open(&make_file_path(), libc::O_RDWR | O_CREAT, 0).await.unwrap();

            state.files.push(file);
            state.dio_files.push(dio_file);
        },
        Operation::Close { file_index } => {
            if state.files.is_empty() {
                return;
            }

            let file = state.files.swap_remove(file_index % state.files.len());
            file.close().await.unwrap();

            let dio_file = state.dio_files.swap_remove(file_index % state.dio_files.len());
            dio_file.close().await.unwrap();
        },
        Operation::Drop { file_index } => {
            if state.files.is_empty() {
                return;
            }

            state.files.swap_remove(file_index % state.files.len());
            state.dio_files.swap_remove(file_index % state.dio_files.len());
        },
        Operation::Read { file_index, offset, size } => {
            if state.files.is_empty() {
                return; 
            }

            let size = size.min(1 << 23);

            let file = &state.files[file_index % state.files.len()];
            let dio_file = &state.dio_files[file_index % state.dio_files.len()];

            let file_size = file.file_size().await.unwrap();
            assert_eq!(dio_file.file_size().await.unwrap(), file_size);

            if file_size == 0 {
                return;
            }

            let iov = make_iov(dio_file, file_size, offset, size);

            let read_size = iov.1.min(usize::try_from((file_size+1).checked_sub(iov.0).unwrap()).unwrap());

            let mut file_buf = vec![0; iov.1];
            let n = file.read(&mut file_buf, iov.0).await.unwrap();
            assert_eq!(n, read_size);

            let layout = Layout::from_size_align(iov.1, dio_file.dio_mem_align()).unwrap();
            let mut dio_file_buf = IoBuffer::new(layout, std::alloc::Global).unwrap();

            let n = dio_file.read_aligned(dio_file_buf.as_mut_slice(), iov.0).await.unwrap();
            assert_eq!(n, read_size);

            assert_eq!(&dio_file_buf.as_slice()[..read_size], &file_buf.as_slice()[..read_size]);
        },
        Operation::Write { file_index, offset, size } => {
            if state.files.is_empty() {
                return; 
            }

            let size = size.min(1 << 23); 

            let file = &state.files[file_index % state.files.len()];
            let dio_file = &state.dio_files[file_index % state.dio_files.len()];

            let file_size = file.file_size().await.unwrap();
            assert_eq!(dio_file.file_size().await.unwrap(), file_size);

            let iov = make_iov(dio_file, file_size, offset, size);

            let layout = Layout::from_size_align(iov.1, dio_file.dio_mem_align()).unwrap();
            let mut buf = IoBuffer::new(layout, std::alloc::Global).unwrap();
            
            let mut rng = rand::thread_rng();
            rng.fill_bytes(buf.as_mut_slice());

            dio_file.write_aligned(buf.as_slice(), iov.0).await.unwrap();
            file.write(buf.as_slice(), iov.0).await.unwrap();
        },
        Operation::Sleep { time } => {
            sleep(time).await;
        },
        Operation::Spawn { ops } => {
            state.tasks.push(spawn(async move {
                let mut inner_state = State::default();

                for op in ops {
                    run_op(&mut inner_state, op).await;
                }
            }));
        },
        Operation::YieldIfNeeded => yield_if_needed().await,
        Operation::Join { task_index } => {
            if state.tasks.is_empty() {
                return;
            }

            let handle = state.tasks.swap_remove(task_index % state.tasks.len());

            handle.await;
        },
    }
}

async fn to_fuzz(operations: Vec<Operation>) {
    let mut state = State::default(); 

    for op in operations {
        run_op(&mut state, op).await;
    }
}

fuzz_target!(|operations: Vec<Operation>| {
    ExecutorConfig::new()
        .run(async move {
            to_fuzz(operations).await;
        }).unwrap();
});

