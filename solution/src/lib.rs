use async_channel::Sender;
use executor::{Handler, ModuleRef, System};
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
use uuid::Uuid;

mod domain;
pub use domain::*;

mod state;
pub(crate) use state::{Candidate, Follower, Leader, RaftState, ServerState};

pub(crate) mod storage;
pub(crate) use storage::{Log, Persistent, PersistentState, Storage};

pub(crate) mod snapshot;
pub(crate) use snapshot::Snapshot;

pub(crate) mod time;
pub(crate) use time::{Timeout, Timer};

type ClientSender = Sender<ClientRequestResponse>;

struct Server {
    config: ServerConfig,
    self_ref: Option<ModuleRef<Raft>>,
    storage: Storage,
    state_machine: Box<dyn StateMachine>,
    sender: Box<dyn RaftSender>,

    all_servers: HashSet<Uuid>,
    clients: HashMap<Uuid, ClientSender>,

    pstate: Persistent<PersistentState>,
    commit_index: usize,
    last_applied: usize,
}

impl Server {
    pub(crate) fn self_ref(&self) -> ModuleRef<Raft> {
        // Unwrapping is safe because this field has been set during Raft initialization.
        self.self_ref.as_ref().unwrap().clone()
    }

    pub(crate) fn log(&self) -> &Log {
        &self.pstate.log
    }

    pub(crate) fn can_vote_for(&self, candidate_id: Uuid) -> bool {
        self.pstate.voted_for.is_none() || self.pstate.voted_for == Some(candidate_id)
    }

    pub(crate) fn get_client_id(&self, index: usize) -> Uuid {
        Uuid::from_u128(index as _)
    }

    pub(crate) async fn update_commit_index(&mut self, new_commit_index: usize) {
        let new_commit_index = min(self.log().last_index(), new_commit_index);
        self.commit_index = max(self.commit_index, new_commit_index);
        while self.last_applied < self.commit_index {
            self.apply_log_entry(self.last_applied + 1).await;
            self.last_applied += 1;
        }
    }

    async fn apply_log_entry(&mut self, index: usize) {
        let entry = self.pstate.log.get(index).unwrap();
        match &entry.content {
            LogEntryContent::Command {
                data,
                client_id,
                sequence_num,
                lowest_sequence_num_without_response: _,
            } => {
                let output = self.state_machine.apply(data).await;
                if let Some(sender) = self.clients.get(client_id) {
                    // TODO sequence_num?
                    let response = CommandResponseArgs {
                        client_id: *client_id,
                        sequence_num: *sequence_num,
                        content: CommandResponseContent::CommandApplied { output },
                    };
                    let _ = sender.send(response.into()).await;
                }
            }
            LogEntryContent::Configuration { .. } => {}
            LogEntryContent::RegisterClient => {
                let client_id = self.get_client_id(index);
                if let Some(sender) = self.clients.get(&client_id) {
                    let response = RegisterClientResponseArgs {
                        content: RegisterClientResponseContent::ClientRegistered { client_id },
                    };
                    let _ = sender.send(response.into()).await;
                }
            }
            LogEntryContent::NoOp => {}
        }
    }

    pub(crate) async fn install_snapshot(&mut self, snapshot: Snapshot) {
        self.state_machine.initialize(&snapshot.data).await;

        assert!(self.last_applied <= snapshot.last_included.index);
        self.last_applied = snapshot.last_included.index;
        self.commit_index = max(self.commit_index, self.last_applied);

        let mut guard = self.pstate.mutate();
        guard.log.apply_snapshot(snapshot);
        guard.save().await;
    }

    pub(crate) async fn take_snapshot(&mut self, client_sender: ClientSender) {
        let data = self.state_machine.serialize().await;

        let mut guard = self.pstate.mutate();
        let num_entries_snapshotted = guard.log.take_snapshot(data, self.last_applied);
        guard.save().await;

        let last_included_index = self.last_applied;
        let content = if num_entries_snapshotted > 0 {
            SnapshotResponseContent::SnapshotCreated {
                last_included_index,
            }
        } else {
            SnapshotResponseContent::NothingToSnapshot {
                last_included_index,
            }
        };
        let _ = client_sender
            .send(SnapshotResponseArgs { content }.into())
            .await;
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
                last_verified_log_index: self.log().last_index(),
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
            ClientRequestContent::Command {
                client_id,
                sequence_num,
                ..
            } => CommandResponseArgs {
                client_id,
                sequence_num,
                content: CommandResponseContent::NotLeader { leader_hint },
            }
            .into(),
            ClientRequestContent::AddServer { new_server } => AddServerResponseArgs {
                new_server,
                content: AddServerResponseContent::NotLeader { leader_hint },
            }
            .into(),
            ClientRequestContent::RemoveServer { old_server } => RemoveServerResponseArgs {
                old_server,
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
        mut state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let storage = Storage::new(stable_storage);
        let mut pstate = Persistent::new(
            "raft_persistent_state",
            PersistentState {
                current_term: 0,
                voted_for: None,
                leader_id: None,
                log: Log::empty(),
            },
            &storage,
        )
        .await;

        if pstate.log.is_empty() {
            let mut guard = pstate.mutate();
            guard.log.push(LogEntry {
                content: LogEntryContent::Configuration {
                    servers: config.servers.clone(),
                },
                term: 0,
                timestamp: first_log_entry_timestamp,
            });
            guard.save().await;
        }
        let last_applied = pstate
            .log
            .snapshot_last_included()
            .map(|md| md.index)
            .unwrap_or(0);
        if let Some(data) = pstate.log.snapshot_data() {
            state_machine.initialize(data).await;
        }

        let server = Server {
            self_ref: None,
            storage,
            state_machine,
            sender: message_sender,
            all_servers: config.servers.clone(),
            clients: HashMap::new(),
            pstate,
            config,
            commit_index: last_applied,
            last_applied,
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
        self.server.self_ref = Some(self_ref.clone());
        self.state = Follower::new(&self.server).into();
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
