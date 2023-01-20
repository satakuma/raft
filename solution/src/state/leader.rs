use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    time::SystemTime,
};
use uuid::Uuid;

use crate::domain::*;
use crate::{
    snapshot, ClientSender, Follower, RaftState, Server, ServerState, Snapshot, Tick, Timer,
};

pub(crate) struct Leader {
    next_index: HashMap<Uuid, usize>,
    match_index: HashMap<Uuid, usize>,
    snapshot_senders: HashMap<Uuid, snapshot::Sender>,

    heartbeat_timer: Timer,
    heartbeat_response_timer: Timer,
    heartbeat_responders: HashSet<Uuid>,

    cluster_change: Option<Change>,
    stepping_down: bool,
}

impl Leader {
    fn get_next_index(&mut self, server: &Server, follower: Uuid) -> usize {
        *self
            .next_index
            .entry(follower)
            .or_insert_with(|| server.log().last_index())
    }

    fn get_match_index(&mut self, follower: Uuid) -> usize {
        *self.match_index.entry(follower).or_insert(0)
    }

    fn get_servers<'a>(&'a self, server: &'a Server) -> &'a HashSet<Uuid> {
        if let Some(chg) = &self.cluster_change {
            &chg.new_config
        } else {
            &server.config.servers
        }
    }

    /// Assumes that required entries to append are present in our log (and not in a snapshot).
    async fn send_append_entries_to_follower(&mut self, server: &Server, follower: Uuid) {
        let next_index = self.get_next_index(server, follower);
        let match_index = self.get_match_index(follower);

        let prev_log_index = next_index - 1;
        let prev_log_term = server.log().get_metadata(prev_log_index).unwrap().term;

        let entries = if match_index + 1 == next_index {
            let num_entries = min(
                server.log().last_index() - prev_log_index,
                server.config.append_entries_batch_size,
            );
            server
                .log()
                .slice(prev_log_index + 1, prev_log_index + 1 + num_entries)
                .to_vec()
        } else {
            Vec::new()
        };

        let msg = AppendEntriesArgs {
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: server.commit_index(),
        };
        server.send(follower, msg.into()).await;
    }

    async fn replicate_log_with_follower(&mut self, server: &Server, follower: Uuid) {
        // Check if there is snapshot transmission in progress.
        if let Some(sender) = self.snapshot_senders.get(&follower) {
            sender.send_chunk(server).await;
            return;
        }

        // Check if we have required entires to replicate in the log.
        if server.log().first_not_snapshotted_index() <= self.get_next_index(server, follower) {
            self.send_append_entries_to_follower(server, follower).await;
        } else {
            // Otherwise send our snapshot to this slow follower.
            let snapshot = Snapshot {
                log: server.log().snapshot().unwrap().clone(),
                last_config: server.config.servers.clone(), // use committed config
                client_sessions: server.client_manager.sessions().clone(),
            };
            let sender =
                snapshot::Sender::new(snapshot, server.config.snapshot_chunk_size, follower);
            sender.send_chunk(server).await;
            self.snapshot_senders.insert(follower, sender);
        }
    }

    async fn replicate_log(&mut self, server: &mut Server) {
        let ids = self.get_servers(server).clone();
        // let ids = server.config.servers.clone();
        for server_id in ids {
            if server_id != server.config.self_id {
                self.replicate_log_with_follower(server, server_id).await;
            }
        }
    }

    async fn heartbeat(&mut self, server: &mut Server) {
        self.replicate_log(server).await;
    }

    async fn advance_cluster_change(&mut self, server: &mut Server) -> bool {
        match &mut self.cluster_change {
            None => false,
            Some(chg) => match chg.status {
                ChangeStatus::AwaitingCurrentTerm if server.ready_for_config_change() => {
                    let log_entry = LogEntry {
                        term: server.pstate.current_term,
                        timestamp: SystemTime::now(),
                        content: LogEntryContent::Configuration {
                            servers: chg.new_config.clone(),
                        },
                    };
                    server
                        .pstate
                        .update_with(|ps| {
                            ps.log.push(log_entry);
                        })
                        .await;
                    chg.status = ChangeStatus::Commiting {
                        index: server.log().last_index(),
                    };
                    self.replicate_log(server).await;
                    true
                }
                ChangeStatus::Commiting { index } if index <= server.commit_index() => {
                    let chg = self.cluster_change.take().unwrap();
                    let _ = chg.sender.send(chg.success_response).await;

                    if !self.included_in_config(server) {
                        self.stepping_down = true;
                    }
                    false
                }
                _ => false,
            },
        }
    }

    async fn advance_commit_index(&mut self, server: &mut Server) {
        loop {
            let mut indexes = Vec::new();
            for follower in self.get_servers(server) {
                let index = if *follower == server.config.self_id {
                    server.log().last_index()
                } else {
                    self.match_index[follower]
                };
                indexes.push(index);
            }

            let num_quorum = self.get_servers(server).len() / 2 + 1;
            let quorum_index = indexes
                .select_nth_unstable_by(num_quorum - 1, |a, b| b.cmp(a))
                .1;
            server.update_commit_index(*quorum_index).await;

            // Try to advance with cluster change and optionally update commit index again.
            if !self.advance_cluster_change(server).await {
                break;
            }
        }
    }

    pub(crate) fn included_in_config(&self, server: &Server) -> bool {
        self.get_servers(server).contains(&server.config.self_id)
    }

    fn heartbeat_response_reset(&mut self, server: &Server) {
        self.heartbeat_response_timer.reset();
        self.heartbeat_responders = if self.included_in_config(server) {
            [server.config.self_id].into()
        } else {
            HashSet::new()
        };
    }

    fn acknowledge_response(&mut self, source: Uuid) {
        self.heartbeat_responders.insert(source);
        if let Some(chg) = &mut self.cluster_change {
            if let ChangeStatus::CatchingUp { max_timer, .. } = &mut chg.status {
                max_timer.reset();
            }
        }
    }

    pub(crate) async fn transition_from_canditate(server: &mut Server) -> ServerState {
        let mut guard = server.pstate.mutate();
        guard.voted_for = Some(server.config.self_id);
        let noop_entry = LogEntry {
            term: guard.current_term,
            timestamp: SystemTime::now(),
            content: LogEntryContent::NoOp,
        };
        guard.log.push(noop_entry);
        guard.save().await;

        let mut leader = Leader {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            snapshot_senders: HashMap::new(),
            heartbeat_timer: Timer::new_heartbeat_timer(server),
            heartbeat_responders: HashSet::new(),
            heartbeat_response_timer: Timer::new_heartbeat_response_timer(server),
            cluster_change: None,
            stepping_down: false,
        };
        leader.heartbeat_response_reset(server);
        leader.into()
    }

    fn maybe_step_down(&self) -> Option<ServerState> {
        if self.stepping_down {
            Some(Follower::new_observer().into())
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl RaftState for Leader {
    fn ignore_raft_msg(&self, _server: &Server, msg: &RaftMessage) -> bool {
        // We filter out RequestVote messages since we are the leader in this term.
        matches!(msg.content, RaftMessageContent::RequestVote(_))
    }

    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        match msg.content {
            RaftMessageContent::AppendEntriesResponse(args) => {
                let source = msg.header.source;
                let last_log_index = args.last_verified_log_index;

                self.acknowledge_response(source);

                if args.success {
                    self.match_index.insert(source, last_log_index);
                    self.next_index.insert(source, last_log_index + 1);
                } else {
                    self.next_index.insert(
                        source,
                        (self.next_index[&source] - 1).clamp(1, server.log().last_index() + 1),
                    );
                }

                self.advance_commit_index(server).await;

                if self.get_next_index(server, source) <= server.log().last_index() {
                    self.replicate_log_with_follower(server, source).await;
                }
            }
            RaftMessageContent::InstallSnapshotResponse(args) => {
                let follower = msg.header.source;
                self.acknowledge_response(follower);

                if let Some(sender) = self.snapshot_senders.get_mut(&follower) {
                    if sender.last_included().index == args.last_included_index {
                        match sender.chunk_acknowledged(args.offset) {
                            snapshot::Status::Pending => sender.send_chunk(server).await,
                            snapshot::Status::Done => {
                                let last_included_index = sender.last_included().index;
                                self.match_index.insert(follower, last_included_index);
                                self.next_index.insert(follower, last_included_index + 1);
                                self.snapshot_senders.remove(&follower);
                                self.replicate_log_with_follower(server, follower).await;
                            }
                            snapshot::Status::Duplicate => {}
                        }
                    }
                }
            }
            _ => (),
        };

        self.maybe_step_down()
    }

    async fn handle_client_req(
        &mut self,
        server: &mut Server,
        req: ClientRequest,
    ) -> Option<ServerState> {
        match req.content {
            ClientRequestContent::RegisterClient => {
                let log_entry = LogEntry {
                    term: server.pstate.current_term,
                    timestamp: SystemTime::now(),
                    content: LogEntryContent::RegisterClient,
                };
                server
                    .pstate
                    .update_with(|ps| {
                        ps.log.push(log_entry);
                    })
                    .await;

                let client_id = server.get_client_id(server.log().last_index());
                server
                    .client_manager
                    .prepare_register_client(client_id, req.reply_to);
            }
            ClientRequestContent::Command {
                command,
                client_id,
                sequence_num,
                lowest_sequence_num_without_response,
            } => {
                let log_entry = LogEntry {
                    term: server.pstate.current_term,
                    timestamp: SystemTime::now(),
                    content: LogEntryContent::Command {
                        data: command,
                        client_id,
                        sequence_num,
                        lowest_sequence_num_without_response,
                    },
                };
                server
                    .pstate
                    .update_with(|ps| {
                        ps.log.push(log_entry);
                    })
                    .await;

                server
                    .client_manager
                    .prepare_command(client_id, sequence_num, req.reply_to);
            }
            ClientRequestContent::AddServer { new_server } => {
                if self.cluster_change.is_some() {
                    let response = AddServerResponseArgs {
                        new_server,
                        content: AddServerResponseContent::ChangeInProgress,
                    };
                    let _ = req.reply_to.send(response.into()).await;
                    return None;
                }

                let mut new_config = server.config.servers.clone();
                if !new_config.insert(new_server) {
                    let response = AddServerResponseArgs {
                        new_server,
                        content: AddServerResponseContent::AlreadyPresent,
                    };
                    let _ = req.reply_to.send(response.into()).await;
                    return None;
                }

                let success_response = AddServerResponseArgs {
                    new_server,
                    content: AddServerResponseContent::ServerAdded,
                }
                .into();
                self.cluster_change = Some(Change {
                    success_response,
                    sender: req.reply_to,
                    new_config,
                    status: ChangeStatus::CatchingUp {
                        new_server,
                        round: 0,
                        round_timer: Timer::new_catch_up_round_timer(server),
                        max_timer: Timer::new_max_catch_up_round_timer(server),
                    },
                });
                self.next_index.insert(new_server, 1);
                self.match_index.insert(new_server, 0);
            }
            ClientRequestContent::RemoveServer { old_server } => {
                if self.cluster_change.is_some() {
                    let response = RemoveServerResponseArgs {
                        old_server,
                        content: RemoveServerResponseContent::ChangeInProgress,
                    };
                    let _ = req.reply_to.send(response.into()).await;
                    return None;
                }

                let mut new_config = server.config.servers.clone();
                if !new_config.remove(&old_server) {
                    let response = RemoveServerResponseArgs {
                        old_server,
                        content: RemoveServerResponseContent::NotPresent,
                    };
                    let _ = req.reply_to.send(response.into()).await;
                    return None;
                }
                if new_config.is_empty() {
                    let response = RemoveServerResponseArgs {
                        old_server,
                        content: RemoveServerResponseContent::OneServerLeft,
                    };
                    let _ = req.reply_to.send(response.into()).await;
                    return None;
                }

                let success_response = RemoveServerResponseArgs {
                    old_server,
                    content: RemoveServerResponseContent::ServerRemoved,
                }
                .into();
                self.cluster_change = Some(Change {
                    success_response,
                    sender: req.reply_to,
                    new_config,
                    status: ChangeStatus::AwaitingCurrentTerm,
                });
            }
            ClientRequestContent::Snapshot => unreachable!(),
        };

        self.replicate_log(server).await;
        self.advance_commit_index(server).await;

        self.maybe_step_down()
    }

    async fn handle_timeout(&mut self, server: &mut Server, msg: Tick) -> Option<ServerState> {
        match msg {
            Tick::Heartbeat => {
                self.heartbeat(server).await;
                self.heartbeat_timer.reset();
            }
            Tick::HeartbeatResponse => {
                if self.heartbeat_responders.len() <= self.get_servers(server).len() / 2 {
                    return Some(Follower::new(server).into());
                } else {
                    self.heartbeat_response_timer.reset();
                    self.heartbeat_responders = [server.config.self_id].into();
                }
            }
            Tick::CatchUpRound => {
                if let Some(chg) = &mut self.cluster_change {
                    if let ChangeStatus::CatchingUp {
                        new_server,
                        round,
                        round_timer,
                        ..
                    } = &mut chg.status
                    {
                        *round += 1;
                        if *round >= server.config.catch_up_rounds
                            || self.next_index[new_server] > server.log().last_index()
                        {
                            chg.status = ChangeStatus::AwaitingCurrentTerm;
                        } else {
                            round_timer.reset();
                        }
                    }
                }
            }
            Tick::CatchUpTimeout => {
                if let Some(chg) = &mut self.cluster_change {
                    if let ChangeStatus::CatchingUp { new_server, .. } = chg.status {
                        let response = AddServerResponseArgs {
                            new_server,
                            content: AddServerResponseContent::Timeout,
                        };
                        let _ = chg.sender.send(response.into()).await;
                        self.cluster_change = None;
                    }
                }
            }
            _ => {}
        };
        None
    }
}

struct Change {
    success_response: ClientRequestResponse,
    sender: ClientSender,
    new_config: HashSet<Uuid>,
    status: ChangeStatus,
}

enum ChangeStatus {
    CatchingUp {
        new_server: Uuid,
        round: u64,
        round_timer: Timer,
        max_timer: Timer,
    },
    AwaitingCurrentTerm,
    Commiting {
        index: usize,
    },
}
