use uuid::Uuid;

use crate::{
    AppendEntriesResponseArgs, Candidate, ClientRequest, ClientRequestContent, RaftMessage,
    RaftMessageContent, RaftState, RequestVoteResponseArgs, Server, ServerState, Timeout, Timer,
};

pub(crate) struct Follower {
    leader: Option<Uuid>,
    election_timer: Option<Timer>,
}

impl Follower {
    pub(crate) fn initial() -> Follower {
        Follower {
            leader: None,
            election_timer: None,
        }
    }

    pub(crate) fn follow_leader(server: &mut Server, leader: Uuid) -> ServerState {
        Follower {
            leader: leader.into(),
            election_timer: Timer::new_election_timer(server).into(),
        }
        .into()
    }

    pub(crate) async fn follow_new_term_leader(
        server: &mut Server,
        term: u64,
        leader: Uuid,
    ) -> ServerState {
        server
            .pstate
            .update_with(|ps| {
                ps.current_term = term;
                ps.voted_for = None;
                ps.leader_id = None;
            })
            .await;
        Follower::follow_leader(server, leader)
    }

    pub(crate) fn reset_timer(&mut self, server: &Server) {
        if let Some(timer) = &mut self.election_timer {
            timer.reset();
        } else {
            self.election_timer = Some(Timer::new_election_timer(server));
        }
    }
}

#[async_trait::async_trait]
impl RaftState for Follower {
    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        if msg.header.term < server.pstate.current_term {
            server.respond_false(&msg).await;
            return None;
        }
        match msg.content {
            RaftMessageContent::AppendEntries(args) => {
                // Reset election timer.
                self.reset_timer(server);

                // Append entries to the log, save it.
                let mut guard = server.log.mutate();
                let success = guard
                    .append_entries(
                        args.entries,
                        (args.prev_log_term, args.prev_log_index).into(),
                    )
                    .await;
                guard.save().await;

                // Update the commit index and possible apply entries to the state machine.
                server.update_commit_index(args.leader_commit).await;

                // Send response to the leader.
                let response = AppendEntriesResponseArgs {
                    success,
                    last_verified_log_index: server.log.last_index(),
                };
                server.respond_with(&msg.header, response.into()).await;
            }
            RaftMessageContent::RequestVote(args) => {
                // Check if we can vote in the current term for this candidate
                // and the candidate's log is up to date with our log.
                let vote_granted = if server.can_vote_for(msg.header.source)
                    && server
                        .log
                        .is_up_to_date_with((args.last_log_term, args.last_log_index).into())
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
        match msg {
            Timeout::Election => Some(Candidate::transition_from_follower(server).await),
            _ => None,
        }
    }
}
