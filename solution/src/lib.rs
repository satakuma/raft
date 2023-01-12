#![allow(clippy::await_holding_refcell_ref)]

use executor::{Handler, ModuleRef, System};
use std::{cell::RefCell, rc::Rc, time::SystemTime};
use uuid::Uuid;

mod domain;
pub use domain::*;

pub(crate) mod storage;
use storage::{Persistent, PersistentVec};

pub struct Raft {
    config: ServerConfig,
    state: ServerState,
    current_term: Persistent<u64>,
    voted_for: Persistent<Uuid>,
    log: PersistentVec<LogEntry>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let storage = Rc::new(RefCell::new(stable_storage));
        let current_term = Persistent::new("current_term", 0, &storage).await;
        let voted_for = Persistent::new("voted_for", Uuid::nil(), &storage).await;
        let log = PersistentVec::new("log", &storage).await;

        let raft = Raft {
            config,
            state: ServerState::Follower,
            current_term,
            voted_for,
            log,
        };
        system.register_module(raft).await
    }

    async fn handle_raft_msg(&mut self, msg: RaftMessage) {}
    async fn handle_client_request(&mut self, req: ClientRequest) {}
}

// Safety: Send has to be manually implemented due to the `Rc` inside.
// Fields using Rc are private and are never shared outside of the
// Raft struct. Thus the data pointed by Rc is never shared between threads.
unsafe impl Send for Raft {}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        self.handle_raft_msg(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        self.handle_client_request(msg).await;
    }
}

enum ServerState {
    Follower,
    Candidate,
    Leader {
        next_index: Vec<usize>,
        match_index: Vec<usize>,
    },
}
