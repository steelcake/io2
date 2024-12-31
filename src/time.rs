use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::executor::CURRENT_TASK_CONTEXT;

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct NotifyWhen {
    timer: Option<Instant>,
}

impl Future for NotifyWhen {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = self.get_mut();
        match fut.timer.take() {
            Some(timer) => {
                CURRENT_TASK_CONTEXT.with_borrow_mut(|ctx| {
                    let ctx = ctx.as_mut().unwrap();
                    ctx.notify_when(timer);
                });
                Poll::Pending
            }
            None => Poll::Ready(()),
        }
    }
}

pub fn sleep(duration: Duration) -> NotifyWhen {
    let now = Instant::now();
    let timer = now.checked_add(duration).unwrap();
    NotifyWhen { timer: Some(timer) }
}

pub fn sleep_until(instant: Instant) -> NotifyWhen {
    NotifyWhen {
        timer: Some(instant),
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::ExecutorConfig;

    use super::*;

    #[test]
    #[ignore]
    fn test_sleep() {
        ExecutorConfig::new()
            .run(async {
                for _ in 0..20 {
                    sleep(Duration::from_secs(1)).await;
                    println!("SLEEPING");
                }
            })
            .unwrap();
    }
}
