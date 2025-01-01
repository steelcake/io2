#![feature(allocator_api)]

#![no_main]

use std::alloc::Layout;
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::time::Duration;
use std::path::PathBuf;

use io2::io_buffer::IoBuffer;
use libc::O_CREAT;
use libfuzzer_sys::{arbitrary, fuzz_target};
use rand::RngCore;

use io2::executor::{JoinHandle, ExecutorConfig, spawn, yield_if_needed};
use io2::fs::dio_file::{align_up, DioFile};
use io2::fs::file::File;
use io2::time::sleep;

#[derive(Clone, Debug, arbitrary::Arbitrary)]
enum Operation {
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

struct InnerState {
    files: [Rc<File>; NUM_FILES],
    dio_files: [Rc<DioFile>; NUM_FILES],
    tasks: Vec<JoinHandle<()>>,
}

struct State {
    inner: Rc<RefCell<InnerState>>,
}

impl State {
    fn get_mut<'a>(&'a self) -> std::cell::RefMut<'a, InnerState> {
        RefCell::borrow_mut(&self.inner)
    }
}

const NUM_FILES: usize = 16;
const FILE_SIZE: usize = 1 << 27; // 128 MB

fn make_file_path(file_name: &str) -> PathBuf {
    let mut path = PathBuf::from("./data");
    path.push(file_name);
    path
}

async fn open_files() -> ([Rc<File>; NUM_FILES], [Rc<DioFile>; NUM_FILES]) {
    let mut name_letter = 'a' as u8;

    let mut files: [MaybeUninit<Rc<File>>; NUM_FILES] = [const { MaybeUninit::uninit() }; NUM_FILES]; 
    let mut dio_files: [MaybeUninit<Rc<DioFile>>; NUM_FILES] = [const { MaybeUninit::uninit() }; NUM_FILES]; 
    for index in 0..NUM_FILES {
        let file_name = format!("{}", name_letter);
        let dio_file_name = format!("{}_dio", name_letter);
        name_letter += 1;

        let file_p = make_file_path(&file_name);
        let file = File::open(&file_p, libc::O_RDWR | O_CREAT, 0o666).unwrap().await.unwrap();
        let dio_file = DioFile::open(&make_file_path(&dio_file_name), libc::O_RDWR | O_CREAT, 0o666).await.unwrap();

        let file_size = align_up(u64::try_from(FILE_SIZE).unwrap(), dio_file.dio_offset_align());
        let file_size = usize::try_from(file_size).unwrap();

        let current_file_size = file.file_size().await.unwrap();
        if current_file_size > 0 {
            assert_eq!(current_file_size, u64::try_from(FILE_SIZE).unwrap());
        } else {
            let mut file_data = IoBuffer::new(
                Layout::from_size_align(file_size, dio_file.dio_mem_align()).unwrap(),
                std::alloc::Global,
            ).unwrap();

            let mut rng = rand::thread_rng();
            rng.fill_bytes(file_data.as_mut_slice());

            let n = file.write(file_data.as_slice(), 0).await.unwrap();
            assert_eq!(n, file_size);

            let n = dio_file.write_aligned(file_data.as_slice(), 0).await.unwrap();
            assert_eq!(n, file_size);
        }

        files[index].write(Rc::new(file));
        dio_files[index].write(Rc::new(dio_file));
    }
    
    unsafe {
        (std::mem::transmute(files), std::mem::transmute(dio_files))
    }
}

async fn run_op(state: &State, op: Operation) {
    match op {
        Operation::Read { file_index, offset, size } => {
            let this = state.get_mut();

            if this.files.is_empty() {
                return; 
            }

            let file = this.files[file_index % this.files.len()].clone();
            let dio_file = this.dio_files[file_index % this.dio_files.len()].clone();

            std::mem::drop(this);

            let file_size = file.file_size().await.unwrap();
            assert_eq!(dio_file.file_size().await.unwrap(), file_size);
            assert_eq!(file_size, u64::try_from(FILE_SIZE).unwrap());

            let offset = offset.min(file_size);
            let size = size.min(1 << 23);
            let iov = make_iov(&dio_file, file_size, offset, size);

            let read_size = iov.1.min(usize::try_from((file_size).checked_sub(iov.0).unwrap()).unwrap());

            let mut file_buf = vec![0; iov.1];
            let n = file.read(&mut file_buf, iov.0).await.unwrap();
            assert_eq!(n, read_size);

            let layout = Layout::from_size_align(iov.1, dio_file.dio_mem_align()).unwrap();
            let mut dio_file_buf = IoBuffer::new(layout, std::alloc::Global).unwrap();

            let n = dio_file.read_aligned(dio_file_buf.as_mut_slice(), iov.0).await.unwrap();
            assert_eq!(n, read_size);

            if &dio_file_buf.as_slice()[..read_size] != &file_buf.as_slice()[..read_size] {
                panic!("read bytes are not equal");
            }
        },
        Operation::Write { file_index, offset, size } => {
            let this = state.get_mut();

            if this.files.is_empty() {
                return; 
            }

            let file = this.files[file_index % this.files.len()].clone();
            let dio_file = this.dio_files[file_index % this.dio_files.len()].clone();

            std::mem::drop(this);

            let file_size = file.file_size().await.unwrap();
            assert_eq!(dio_file.file_size().await.unwrap(), file_size);
            assert_eq!(file_size, u64::try_from(FILE_SIZE).unwrap());

            let offset = offset.min(file_size);
            let size = size.min(1 << 23);
            let iov = make_iov(&dio_file, file_size, offset, size);
            assert!(iov.0 + u64::try_from(iov.1).unwrap() <= file_size);

            let layout = Layout::from_size_align(iov.1, dio_file.dio_mem_align()).unwrap();
            let mut buf = IoBuffer::new(layout, std::alloc::Global).unwrap();
            
            let mut rng = rand::thread_rng();
            rng.fill_bytes(buf.as_mut_slice());

            dio_file.write_aligned(buf.as_slice(), iov.0).await.unwrap();
            file.write(buf.as_slice(), iov.0).await.unwrap();
        },
        Operation::Sleep { time } => {
            let time = time.min(Duration::from_micros(100));
            sleep(time).await;
        },
        Operation::Spawn { ops } => {
            let inner = Rc::clone(&state.inner);
            state.get_mut().tasks.push(spawn(async move {
                let mut inner_state = State { inner };

                for op in ops {
                    run_op(&mut inner_state, op).await;
                }
            }));
        },
        Operation::YieldIfNeeded => yield_if_needed().await,
        Operation::Join { task_index } => {
            let mut this = state.get_mut();

            if this.tasks.is_empty() {
                return;
            }

            let idx = task_index % this.tasks.len();
            let handle = this.tasks.swap_remove(idx);

            std::mem::drop(this);

            handle.await;
        },
    }
}

async fn to_fuzz(operations: Vec<Operation>) {
    let (files, dio_files) = open_files().await;
        
    let mut state = State {
        inner: Rc::new(RefCell::new(InnerState {
            files,
            dio_files,
            tasks: Vec::new(),
        })),
    };

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

