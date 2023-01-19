use executor::ModuleRef;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use std::cmp::{max, min};
use std::collections::HashSet;
use std::time::SystemTime;

use crate::domain::*;
use crate::{ClientManager, ClientSender, CommandStatus, Log, Persistent, Raft, Snapshot, Storage};

#[derive(Serialize, Deserialize)]
pub(crate) struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<Uuid>,
    pub log: Log,
}

pub(crate) struct Server {
    self_ref: Option<ModuleRef<Raft>>,
    storage: Storage,
    state_machine: Box<dyn StateMachine>,
    sender: Box<dyn RaftSender>,

    commit_index: usize,
    last_applied: usize,

    pub all_servers: HashSet<Uuid>,
    pub client_manager: ClientManager,
    pub pstate: Persistent<PersistentState>,
    pub config: ServerConfig,
}

impl Server {
    pub(super) async fn new(
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        mut state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> Server {
        let storage = Storage::new(stable_storage);
        let mut pstate = Persistent::recover_or(
            "raft_persistent_state",
            &storage,
            PersistentState {
                current_term: 0,
                voted_for: None,
                log: Log::empty(),
            },
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

        Server {
            self_ref: None,
            storage,
            state_machine,
            sender: message_sender,
            all_servers: config.servers.clone(),
            client_manager: ClientManager::new(config.session_expiration),
            pstate,
            config,
            commit_index: last_applied,
            last_applied,
        }
    }

    pub(super) fn set_self_ref(&mut self, self_ref: ModuleRef<Raft>) {
        self.self_ref = Some(self_ref);
    }

    pub(crate) fn self_ref(&self) -> ModuleRef<Raft> {
        // Unwrapping is safe because this field has been set during Raft initialization.
        self.self_ref.as_ref().unwrap().clone()
    }

    pub(crate) fn log(&self) -> &Log {
        &self.pstate.log
    }

    pub(crate) fn storage(&self) -> &Storage {
        &self.storage
    }

    pub(crate) fn commit_index(&self) -> usize {
        self.commit_index
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
                lowest_sequence_num_without_response,
            } => {
                if !self.client_manager.is_expired(*client_id, entry.timestamp) {
                    let output = match self
                        .client_manager
                        .command_status(*client_id, *sequence_num)
                    {
                        CommandStatus::Pending => self.state_machine.apply(data).await,
                        CommandStatus::Finished(output) => output,
                        CommandStatus::Discarded => {
                            self.client_manager
                                .reply_session_expired(*client_id, *sequence_num)
                                .await;
                            return;
                        }
                    };
                    self.client_manager
                        .command(*client_id, *sequence_num, output, entry.timestamp)
                        .await;
                    self.client_manager
                        .prune_outputs(*client_id, *lowest_sequence_num_without_response);
                } else {
                    self.client_manager.expire(*client_id, *sequence_num).await;
                }
            }
            LogEntryContent::Configuration { .. } => {}
            LogEntryContent::RegisterClient => {
                let client_id = self.get_client_id(index);
                self.client_manager
                    .register_client(client_id, entry.timestamp)
                    .await;
            }
            LogEntryContent::NoOp => {}
        }
    }

    pub(crate) async fn install_snapshot(&mut self, snapshot: Snapshot) {
        // Initialize config and client sessions.
        self.all_servers = snapshot.last_config;
        self.client_manager.initialize(&snapshot.client_sessions);

        // Initialize state machine and related variables.
        self.state_machine.initialize(&snapshot.log.data).await;
        self.last_applied = snapshot.log.last_included.index;
        self.commit_index = max(self.commit_index, self.last_applied);

        // Write the log snapshot to storage.
        let mut guard = self.pstate.mutate();
        guard.log.apply_snapshot(snapshot.log);
        guard.save().await;
    }

    pub(crate) async fn take_log_snapshot(&mut self, client_sender: ClientSender) {
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
        self.send(msg_hdr.source, res).await;
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
