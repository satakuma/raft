use uuid::Uuid;

use crate::{
    snapshot, AppendEntriesResponseArgs, Candidate, ClientRequest, InstallSnapshotResponseArgs,
    RaftMessage, RaftMessageContent, RaftState, RequestVoteResponseArgs, Server, ServerState,
    Timeout, Timer,
};

pub(crate) struct Follower {
    leader: Option<Uuid>,
    snapshot_recv: Option<snapshot::Receiver>,
    election_timer: Option<Timer>,
    minimum_election_timer: Option<Timer>,
}

impl Follower {
    // Creates a passive follower which will not become a candidate.
    pub(crate) fn observer() -> Follower {
        Follower {
            leader: None,
            snapshot_recv: None,
            election_timer: None,
            minimum_election_timer: None,
        }
    }

    pub(crate) fn new(server: &Server) -> Follower {
        Follower {
            leader: None,
            snapshot_recv: None,
            election_timer: Timer::new_election_timer(server).into(),
            minimum_election_timer: None,
        }
    }

    pub(crate) fn new_leader_discovered(server: &Server, leader: Uuid) -> ServerState {
        Follower {
            leader: leader.into(),
            snapshot_recv: None,
            election_timer: Timer::new_election_timer(server).into(),
            minimum_election_timer: Timer::new_minimum_election_timer(server).into(),
        }
        .into()
    }

    pub(crate) async fn new_term_discovered(server: &mut Server, term: u64) -> ServerState {
        server
            .pstate
            .update_with(|ps| {
                ps.current_term = term;
                ps.voted_for = None;
                ps.leader_id = None;
            })
            .await;
        Follower::new(server).into()
    }

    pub(crate) fn reset_timers(&mut self, server: &Server) {
        // We stop old timers by dropping them.
        self.election_timer = Some(Timer::new_election_timer(server));
        self.minimum_election_timer = Some(Timer::new_minimum_election_timer(server));
    }

    fn heartbeat_received(&mut self, server: &Server, source: Uuid) {
        // Follow the leader.
        self.leader = Some(source);

        // Reset timers.
        self.reset_timers(server);
    }
}

#[async_trait::async_trait]
impl RaftState for Follower {
    fn ignore_raft_msg(&self, _server: &Server, msg: &RaftMessage) -> bool {
        // We filter out RequestVote messages until minimum election timer goes off.
        matches!(msg.content, RaftMessageContent::RequestVote(_))
            && self.minimum_election_timer.is_some()
    }

    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        match msg.content {
            RaftMessageContent::AppendEntries(args) => {
                self.heartbeat_received(server, msg.header.source);

                // Save `last_verified_log_index` for later
                let last_verified_log_index = args.entries.len() + args.prev_log_index;

                // Append entries to the log, save it.
                let mut guard = server.pstate.mutate();
                let success = guard.log.append_entries(
                    args.entries,
                    (args.prev_log_term, args.prev_log_index).into(),
                );
                guard.save().await;

                // Update the commit index and possible apply entries to the state machine.
                server.update_commit_index(args.leader_commit).await;

                // Send response to the leader.
                let response = AppendEntriesResponseArgs {
                    success,
                    last_verified_log_index,
                };
                server.respond_with(&msg.header, response.into()).await;
            }
            RaftMessageContent::RequestVote(args) => {
                // Check if we can vote in the current term for this candidate
                // and the candidate's log is up to date with our log.
                let candidate_log_md = (args.last_log_term, args.last_log_index).into();
                let vote_granted = if server.can_vote_for(msg.header.source)
                    && server.log().is_up_to_date_with(candidate_log_md)
                {
                    // If so, take our voting ticket and vote for the candidate.
                    server
                        .pstate
                        .update_with(|ps| {
                            ps.voted_for = Some(msg.header.source);
                        })
                        .await;
                    true
                } else {
                    false
                };

                let response = RequestVoteResponseArgs { vote_granted }.into();
                server.respond_with(&msg.header, response).await;
            }
            RaftMessageContent::InstallSnapshot(args) => {
                self.heartbeat_received(server, msg.header.source);

                let response = InstallSnapshotResponseArgs {
                    last_included_index: args.last_included_index,
                    offset: args.offset,
                }
                .into();

                let receiver = self.snapshot_recv.get_or_insert(snapshot::Receiver::new());
                if receiver.receive_chunk(args) == snapshot::Status::Done {
                    let snapshot = self.snapshot_recv.take().unwrap().into_snapshot();
                    server.install_snapshot(snapshot).await;
                }

                server.respond_with(&msg.header, response).await;
            }
            _ => (),
        }

        // Follower does not transition to other states because of a Raft message.
        // The only way to become a candidate is by election timeout.
        None
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        server.respond_not_leader(req, self.leader).await;
    }

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState> {
        match msg {
            Timeout::Election => Some(Candidate::transition_from_follower(server).await),
            Timeout::ElectionMinimum => {
                self.minimum_election_timer.take();
                None
            }
            _ => None,
        }
    }
}
