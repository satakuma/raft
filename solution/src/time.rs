use executor::ModuleRef;
use rand::Rng;
use tokio::sync::oneshot;
use tokio::time::{interval_at, Duration, Instant};

use crate::{Raft, Server};

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
    msg: Tick,
}

impl Timer {
    pub(crate) fn new(raft: ModuleRef<Raft>, period: Duration, msg: Tick) -> Timer {
        let mut timer = Timer {
            inner: TimerInner { raft, period, msg },
            handle: None,
        };
        timer.reset();
        timer
    }

    pub(crate) fn new_election_timer(server: &Server) -> Timer {
        let dur = rand::thread_rng().gen_range(server.config.election_timeout_range.clone());
        Timer::new(server.self_ref(), dur, Tick::Election)
    }

    pub(crate) fn new_minimum_election_timer(server: &Server) -> Timer {
        let dur = *server.config.election_timeout_range.start();
        Timer::new(server.self_ref(), dur, Tick::ElectionMinimum)
    }

    pub(crate) fn new_catch_up_round_timer(server: &Server) -> Timer {
        let dur = rand::thread_rng().gen_range(server.config.election_timeout_range.clone());
        Timer::new(server.self_ref(), dur, Tick::CatchUpRound)
    }

    pub(crate) fn new_max_catch_up_round_timer(server: &Server) -> Timer {
        let dur = 2 * *server.config.election_timeout_range.end();
        Timer::new(server.self_ref(), dur, Tick::CatchUpTimeout)
    }

    pub(crate) fn new_heartbeat_timer(server: &Server) -> Timer {
        Timer::new(
            server.self_ref(),
            server.config.heartbeat_timeout,
            Tick::Heartbeat,
        )
    }

    pub(crate) fn new_heartbeat_response_timer(server: &Server) -> Timer {
        let dur = rand::thread_rng().gen_range(server.config.election_timeout_range.clone());
        Timer::new(server.self_ref(), dur, Tick::HeartbeatResponse)
    }

    pub(crate) fn reset(&mut self) {
        let (tx, mut rx) = oneshot::channel();
        self.handle = Some(tx); // old handle gets dropped here

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
pub(crate) enum Tick {
    Election,
    ElectionMinimum,
    Heartbeat,
    HeartbeatResponse,
    CatchUpRound,
    CatchUpTimeout,
}
