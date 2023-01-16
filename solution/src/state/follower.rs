use uuid::Uuid;

use crate::{
    AppendEntriesResponseArgs, Candidate, ClientRequest, ClientRequestContent, RaftMessage,
    RaftMessageContent, RaftState, RequestVoteResponseArgs, Server, ServerState, Timeout, Timer,
};

pub(crate) struct Follower {
    leader: Option<Uuid>,
    election_timer: Option<Timer>,
    minimum_election_timer: Option<Timer>,
}

impl Follower {
    // Creates a passive follower which will not become a candidate.
    pub(crate) fn observer() -> Follower {
        Follower {
            leader: None,
            election_timer: None,
            minimum_election_timer: None,
        }
    }

    pub(crate) fn new(server: &Server) -> Follower {
        Follower {
            leader: None,
            election_timer: Timer::new_election_timer(server).into(),
            minimum_election_timer: None,
        }
    }

    pub(crate) fn new_leader_discovered(server: &Server, leader: Uuid) -> ServerState {
        Follower {
            leader: leader.into(),
            election_timer: Timer::new_election_timer(server).into(),
            minimum_election_timer: Timer::new_minimum_election_timer(server).into(),
        }
        .into()
    }

    pub(crate) async fn new_term_discovered(server: &mut Server, term: u64) -> ServerState {
        println!("follower: new term discovered");
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
}

#[async_trait::async_trait]
impl RaftState for Follower {
    fn filter_raft_msg(&self, _server: &Server, msg: &RaftMessage) -> bool {
        // We filter out RequestVote messages until minimum election timer goes off.
        !(matches!(msg.content, RaftMessageContent::RequestVote(_))
            && self.minimum_election_timer.is_some())
    }

    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        match msg.content {
            RaftMessageContent::AppendEntries(args) => {
                println!("follower: append entries");

                // Follow the leader.
                self.leader = Some(msg.header.source);

                // Reset timers.
                self.reset_timers(server);

                // Save `last_verified_log_index` for later
                let last_verified_log_index = args.entries.len() + args.prev_log_index;

                // Append entries to the log, save it.
                let mut guard = server.pstate.mutate();
                let success = guard
                    .log
                    .append_entries(
                        args.entries,
                        (args.prev_log_term, args.prev_log_index).into(),
                    )
                    .await;
                guard.save().await;

                // Update the commit index and possible apply entries to the state machine.
                server.update_commit_index(args.leader_commit).await;

                // Send response to the leader.
                println!(
                    "follower: returning last index: {:?}",
                    last_verified_log_index
                );
                let response = AppendEntriesResponseArgs {
                    success,
                    last_verified_log_index,
                };
                server.respond_with(&msg.header, response.into()).await;
            }
            RaftMessageContent::RequestVote(args) => {
                println!("follower: requested vote");
                // Check if we can vote in the current term for this candidate
                // and the candidate's log is up to date with our log.
                let candidate_log_md = (args.last_log_term, args.last_log_index).into();
                let vote_granted = if server.can_vote_for(msg.header.source)
                    && server.log().is_up_to_date_with(candidate_log_md)
                {
                    println!("follower: vote granted");
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
                server
                    .respond_with(&msg.header, RequestVoteResponseArgs { vote_granted }.into())
                    .await;
            }
            RaftMessageContent::InstallSnapshot(_args) => unimplemented!("Snapshots omitted"),
            RaftMessageContent::InstallSnapshotResponse(_args) => {
                unimplemented!("Snapshots omitted")
            }
            _ => (),
        }

        // Follower does not transition to other states because of a Raft message.
        // The only way to become a candidate is by an election timeout.
        None
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        match req.content {
            ClientRequestContent::Snapshot => todo!(),
            _ => server.respond_not_leader(req, self.leader).await,
        }
    }

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState> {
        println!("follower: timeout!");
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
