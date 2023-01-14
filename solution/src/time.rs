use executor::ModuleRef;
use rand::Rng;
use tokio::sync::oneshot;
use tokio::time::{interval_at, Duration, Instant};

use crate::{Raft, Node};

/// Re-implementation of a timer, similar to what is implemented
/// in the executor, but this one cancels the tick once dropped.
pub(crate) struct Timer {
    inner: TimerInner,
    handle: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
struct TimerInner {
    raft: ModuleRef<Raft>,
    period: Duration,
    msg: Timeout,
}

impl Timer {
    pub(crate) fn new(raft: ModuleRef<Raft>, period: Duration, msg: Timeout) -> Timer {
        let mut timer = Timer {
            inner: TimerInner { raft, period, msg },
            handle: None,
        };
        timer.reset();
        timer
    }

    pub(crate) fn new_election_timer(node: &Node) -> Timer {
        let dur = rand::thread_rng().gen_range(node.config.election_timeout_range.clone());
        Timer::new(node.self_ref(), dur, Timeout::Election)
    }

    pub(crate) fn new_heartbeat_timer(node: &Node) -> Timer {
        Timer::new(node.self_ref(), node.config.heartbeat_timeout, Timeout::Election)
    }

    pub(crate) fn reset(&mut self) {
        let (tx, mut rx) = oneshot::channel();
        self.handle = Some(tx);

        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut interval = interval_at(Instant::now() + inner.period, inner.period);
            loop {
                tokio::select! {
                    _ = interval.tick() => inner.raft.send(inner.msg.clone()).await,
                    _ = &mut rx => break,
                }
            }
        });
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Timeout {
    Election,
    Heartbeat,
}
