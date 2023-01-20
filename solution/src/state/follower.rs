use uuid::Uuid;

use crate::domain::*;
use crate::{snapshot, Candidate, RaftState, Server, ServerState, Timeout, Timer};

pub(crate) struct Follower {
    leader: Option<Uuid>,
    snapshot_recv: Option<snapshot::Receiver>,
    election_timer: Option<Timer>,
    minimum_election_timer: Option<Timer>,
}

impl Follower {
    /// Constructs a Follower at Raft server initialization or recovery.
    /// It will retrieve an unfinished snapshot if there is one.
    pub(crate) async fn init_recovery(server: &mut Server) -> Follower {
        let snapshot_recv = snapshot::Receiver::recovery(server).await;
        let mut follower = Follower {
            leader: None,
            snapshot_recv,
            election_timer: None,
            minimum_election_timer: None,
        };
        follower.try_install_snapshot(server).await;
        follower
    }

    /// Hook called when the Raft server starts operating.
    /// Boils down to starting the election timer.
    pub(crate) fn raft_server_start(&mut self, server: &Server) {
        self.election_timer = Some(Timer::new_election_timer(server));
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
            })
            .await;
        server.config.discard_change();
        Follower::new(server).into()
    }

    pub(crate) fn reset_timers(&mut self, server: &Server) {
        // We stop old timers by dropping them.
        self.election_timer = Some(Timer::new_election_timer(server));
        self.minimum_election_timer = Some(Timer::new_minimum_election_timer(server));
    }

    fn heartbeat_received(&mut self, server: &Server, source: Uuid) {
        // Follow the leader and reset timers.
        self.leader = Some(source);
        self.reset_timers(server);
    }

    async fn try_install_snapshot(&mut self, server: &mut Server) {
        if let Some(recv) = &mut self.snapshot_recv {
            if recv.is_ready() {
                recv.install_snapshot(server).await;
                self.snapshot_recv = None;
            } else if recv.is_finished() {
                self.snapshot_recv = None;
            }
        }
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

                let recv = if let Some(recv) = self.snapshot_recv.as_mut() {
                    recv
                } else {
                    self.snapshot_recv = Some(snapshot::Receiver::new(server).await);
                    self.snapshot_recv.as_mut().unwrap()
                };
                recv.receive_chunk(args).await;
                self.try_install_snapshot(server).await;

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
