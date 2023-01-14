use executor::{Handler, ModuleRef, System};
use std::{cmp::min, collections::HashSet, time::SystemTime};
use uuid::Uuid;

mod domain;
pub use domain::*;

pub(crate) mod storage;
use storage::{Log, Persistent, PersistentState, Storage};

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

    all_servers: HashSet<Uuid>,

    pstate: Persistent<PersistentState>,
    log: Persistent<Log>,
    commit_index: usize,
    last_applied: usize,
}

impl Server {
    pub(crate) fn self_ref(&self) -> ModuleRef<Raft> {
        // Unwrapping is safe because this field has been set during Raft initialization.
        self.self_ref.as_ref().unwrap().clone()
    }

    pub(crate) fn can_vote_for(&self, candidate_id: Uuid) -> bool {
        self.pstate.voted_for.is_none() || self.pstate.voted_for == Some(candidate_id)
    }

    pub(crate) async fn update_commit_index(&mut self, leader_commit_index: usize) {
        let new_commit_index = min(self.log.last_index(), leader_commit_index);
        if self.commit_index < new_commit_index {
            self.commit_index = new_commit_index;
            // TODO: apply to the state machine
        }
    }

    pub(crate) async fn send(&self, target: Uuid, msg_content: RaftMessageContent) {
        assert_ne!(target, self.config.self_id);
        let msg = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.pstate.current_term,
            },
            content: msg_content,
        };
        self.sender.send(&target, msg).await
    }

    pub(crate) async fn broadcast(&self, msg_content: RaftMessageContent) {
        for server_id in &self.all_servers {
            if *server_id != self.config.self_id {
                self.send(*server_id, msg_content.clone()).await;
            }
        }
    }

    pub(crate) async fn respond_with(&self, msg_hdr: &RaftMessageHeader, res: RaftMessageContent) {
        self.send(msg_hdr.source, res.into()).await;
    }

    pub(crate) async fn respond_false(&self, msg: &RaftMessage) {
        let response_content: RaftMessageContent = match &msg.content {
            RaftMessageContent::AppendEntries(_) => AppendEntriesResponseArgs {
                success: false,
                last_verified_log_index: self.log.last_index(),
            }
            .into(),
            RaftMessageContent::RequestVote(_) => RequestVoteResponseArgs {
                vote_granted: false,
            }
            .into(),
            RaftMessageContent::InstallSnapshot(_) => todo!(),
            _ => unreachable!(),
        };
        self.respond_with(&msg.header, response_content).await;
    }

    pub(crate) async fn respond_not_leader(&self, req: ClientRequest, leader_hint: Option<Uuid>) {
        let response = match req.content {
            ClientRequestContent::Command { .. } => CommandResponseArgs {
                client_id: todo!(),
                sequence_num: todo!(),
                content: CommandResponseContent::NotLeader { leader_hint },
            }
            .into(),
            ClientRequestContent::AddServer { .. } => AddServerResponseArgs {
                new_server: todo!(),
                content: AddServerResponseContent::NotLeader { leader_hint },
            }
            .into(),
            ClientRequestContent::RemoveServer { .. } => RemoveServerResponseArgs {
                old_server: todo!(),
                content: RemoveServerResponseContent::NotLeader { leader_hint },
            }
            .into(),
            ClientRequestContent::RegisterClient => RegisterClientResponseArgs {
                content: RegisterClientResponseContent::NotLeader { leader_hint },
            }
            .into(),
            ClientRequestContent::Snapshot => return,
        };
        let _ = req.reply_to.send(response).await;
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
        let pstate = Persistent::new(
            "raft_persistent_state",
            PersistentState {
                current_term: 0,
                voted_for: None,
                leader_id: None,
            },
            &storage,
        )
        .await;

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
            all_servers: config.servers.clone(),
            pstate,
            log,
            config,
            commit_index: 0,
            last_applied: 0,
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
