use async_channel::Sender;
use executor::{Handler, ModuleRef, System};
use std::time::SystemTime;

mod domain;
pub use domain::*;

mod server;
pub(crate) use server::Server;

mod state;
pub(crate) use state::{Candidate, Follower, Leader, RaftState, ServerState};

mod client_manager;
pub(crate) use client_manager::{ClientManager, CommandStatus};

mod log;
pub(crate) use crate::log::{Log, LogEntryMetadata};

pub(crate) mod snapshot;
pub(crate) use snapshot::Snapshot;

mod storage;
pub(crate) use storage::{Persistent, Storage};

mod time;
pub(crate) use time::{Timeout, Timer};

pub(crate) type ClientSender = Sender<ClientRequestResponse>;

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
        let mut server = Server::new(
            config,
            first_log_entry_timestamp,
            state_machine,
            stable_storage,
            message_sender,
        )
        .await;
        let raft = Raft {
            state: ServerState::init_recovery(&mut server).await,
            server,
        };

        let mref = system.register_module(raft).await;
        mref.send(Start).await;

        mref
    }

    async fn handle_raft_msg(&mut self, msg: RaftMessage) {
        if !self.state.ignore_raft_msg(&self.server, &msg) {
            let transition = self.state.handle_raft_msg(&mut self.server, msg).await;
            if let Some(next_state) = transition {
                self.state = next_state;
            }
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
        self.server.set_self_ref(self_ref.clone());
        self.state.raft_server_start(&self.server);
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
