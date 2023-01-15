use std::collections::HashSet;
use uuid::Uuid;

use crate::{
    ClientRequest, ClientRequestContent, Follower, Leader, RaftMessage, RaftMessageContent,
    RaftState, RequestVoteResponseArgs, Server, ServerState, Timeout, Timer,
};

pub(crate) struct Candidate {
    ballot_box: BallotBox,
    election_timer: Timer,
}

impl Candidate {
    async fn new(server: &mut Server) -> ServerState {
        server
            .pstate
            .update_with(|ps| {
                ps.current_term += 1;
                ps.voted_for = Some(server.config.self_id);
            })
            .await;

        let mut candidate = Candidate {
            ballot_box: BallotBox::new(server.all_servers.len()),
            election_timer: Timer::new_election_timer(server),
        };

        if candidate.ballot_box.add_vote(Vote::self_vote(server)) == VotingResult::Won {
            Leader::transition_from_canditate(server).await.into()
        } else {
            candidate.into()
        }
    }

    pub(crate) async fn transition_from_follower(server: &mut Server) -> ServerState {
        Candidate::new(server).await
    }

    pub(crate) async fn loop_from_candidate(server: &mut Server) -> ServerState {
        Candidate::new(server).await
    }
}

#[async_trait::async_trait]
impl RaftState for Candidate {
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
            RaftMessageContent::AppendEntries(_) => {
                // If we receive an AppendEntries message with current term, we convert
                // to a follower and handle the message as a follower.
                let mut follower = Follower::follow_leader(server, msg.header.source);
                let followers_transition = follower.handle_raft_msg(server, msg).await;
                assert!(followers_transition.is_none());
                Some(follower)
            }
            RaftMessageContent::RequestVote(_) => {
                // Respond false because we already voted for ourself.
                server.respond_false(&msg).await;
                None
            }
            RaftMessageContent::RequestVoteResponse(args) => {
                // Receive the vote and possibly convert to a leader.
                let vote = Vote::from_msg(msg.header.source, args.vote_granted);
                if self.ballot_box.add_vote(vote) == VotingResult::Won {
                    Some(Leader::transition_from_canditate(server).await.into())
                } else {
                    None
                }
            }
            RaftMessageContent::InstallSnapshot(_args) => unimplemented!("Snapshots omitted"),
            RaftMessageContent::InstallSnapshotResponse(_args) => {
                unimplemented!("Snapshots omitted")
            }
            _ => None,
        }
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        match req.content {
            ClientRequestContent::Snapshot => todo!(),
            _ => server.respond_not_leader(req, None).await,
        }
    }

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState> {
        match msg {
            Timeout::Election => Some(Candidate::loop_from_candidate(server).await),
            _ => None,
        }
    }
}

struct BallotBox {
    votes: HashSet<Uuid>,
    num_voters: usize,
}

impl BallotBox {
    fn new(num_voters: usize) -> BallotBox {
        BallotBox {
            votes: HashSet::new(),
            num_voters,
        }
    }

    fn result(&self) -> VotingResult {
        if self.votes.len() > self.num_voters / 2 {
            VotingResult::Won
        } else {
            VotingResult::Pending
        }
    }

    fn add_vote(&mut self, vote: Vote) -> VotingResult {
        if vote.granted {
            self.votes.insert(vote.from);
        }
        self.result()
    }
}

#[derive(Clone, Copy)]
struct Vote {
    from: Uuid,
    granted: bool,
}

impl Vote {
    fn self_vote(server: &Server) -> Vote {
        Vote {
            from: server.config.self_id,
            granted: true,
        }
    }

    fn from_msg(from: Uuid, granted: bool) -> Vote {
        Vote { from, granted }
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum VotingResult {
    Pending,
    Won,
}
