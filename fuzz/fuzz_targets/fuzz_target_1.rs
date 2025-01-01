#![feature(allocator_api)]

#![no_main]

use std::time::Duration;

use libfuzzer_sys::{arbitrary, fuzz_target};

use io2::executor::{JoinHandle, ExecutorConfig, spawn, yield_if_needed};
use io2::time::sleep;

#[derive(Clone, Debug, arbitrary::Arbitrary)]
enum Operation {
    Sleep {
        micros: u64,
    },
    Spawn {
        ops: Vec<Operation>,
    },
    YieldIfNeeded,
    Join {
        task_index: usize,
    },
}

#[derive(Default)]
struct State {
    tasks: Vec<JoinHandle<()>>,
}

async fn run_op(state: &mut State, op: Operation) {
    match op {
        Operation::Sleep { micros } => {
            let time = Duration::from_micros(micros.min(500));
            sleep(time).await;
        },
        Operation::Spawn { ops } => {
            state.tasks.push(spawn(async move {
                let mut inner_state = State::default();

                for op in ops {
                    run_op(&mut inner_state, op).await;
                }

                for handle in inner_state.tasks {
                    handle.await;
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

    for handle in state.tasks {
        handle.await;
    }
}

fuzz_target!(|operations: Vec<Operation>| {
    ExecutorConfig::new()
        .run(async move {
            to_fuzz(operations).await;
        }).unwrap();
});
