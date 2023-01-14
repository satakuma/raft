#![allow(clippy::await_holding_refcell_ref)]

use executor::{Handler, ModuleRef, System};
use std::{collections::HashSet, time::SystemTime};
use uuid::Uuid;

mod domain;
pub use domain::*;

pub(crate) mod storage;
use storage::{Log, Persistent, Storage};

mod state;
pub(crate) use state::{Candidate, Follower, Leader, RaftState, ServerState};

pub(crate) mod util;

pub(crate) mod time;
pub(crate) use time::{Timeout, Timer};

struct Server {
    config: ServerConfig,
    self_ref: Option<ModuleRef<Raft>>,
    storage: Storage,
    state_machine: Box<dyn StateMachine>,
    sender: Box<dyn RaftSender>,

    servers: HashSet<Uuid>,

    current_term: Persistent<u64>,
    voted_for: Persistent<Option<Uuid>>,
    leader_id: Persistent<Option<Uuid>>,
    log: Persistent<Log>,
}

impl Server {
    pub(crate) fn self_ref(&self) -> ModuleRef<Raft> {
        // Unwrapping is safe because this field has been set during Raft initialization.
        self.self_ref.as_ref().unwrap().clone()
    }

    pub(crate) async fn send(&self, target: &Uuid, msg: RaftMessage) {
        assert_ne!(*target, self.config.self_id);
        self.sender.send(target, msg).await
    }

    pub(crate) async fn broadcast(&self, msg: RaftMessage) {
        for server_id in &self.servers {
            if *server_id != self.config.self_id {
                self.send(server_id, msg.clone()).await;
            }
        }
    }
}

pub struct Raft {
    server: Server,
    state: ServerState,
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
        let storage = Storage::new(stable_storage);
        let current_term = Persistent::new("current_term", 0, &storage).await;
        let voted_for = Persistent::new("voted_for", None, &storage).await;
        let leader_id = Persistent::new("leader_id", None, &storage).await;

        let mut log = Persistent::new("voted_for", Log::empty(), &storage).await;
        if log.is_empty() {
            let mut guard = log.mutate();
            guard.push(LogEntry {
                content: LogEntryContent::Configuration {
                    servers: config.servers.clone(),
                },
                term: 0,
                timestamp: first_log_entry_timestamp,
            });
            guard.save().await;
        }

        let server = Server {
            self_ref: None,
            storage,
            state_machine,
            sender: message_sender,
            servers: config.servers.clone(),
            current_term,
            voted_for,
            leader_id,
            log,
            config,
        };
        let raft = Raft {
            server,
            state: ServerState::initial(),
        };
        let mref = system.register_module(raft).await;
        mref.send(Start).await;
        mref
    }

    async fn handle_raft_msg(&mut self, msg: RaftMessage) {
        let transition = self.state.handle_raft_msg(&mut self.server, msg).await;
        if let Some(next_state) = transition {
            self.state = next_state;
        }
    }

    async fn handle_client_req(&mut self, req: ClientRequest) {
        self.state.handle_client_req(&mut self.server, req).await;
    }

    async fn handle_timeout(&mut self, msg: Timeout) {
        let transition = self.state.handle_timeout(&mut self.server, msg).await;
        if let Some(next_state) = transition {
            self.state = next_state;
        }
    }
}

/// Message sent exactly once at Raft initialization.
/// Allows to set the self ModuleRef and start timeout timers.
struct Start;

#[async_trait::async_trait]
impl Handler<Start> for Raft {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, _msg: Start) {
        self.server.self_ref = Some(self_ref.clone());
        self.state.follower_mut().unwrap().reset_timer(&self.server);
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        self.handle_raft_msg(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        self.handle_client_req(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: Timeout) {
        self.handle_timeout(msg).await;
    }
}
