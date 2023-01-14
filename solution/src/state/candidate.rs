use std::collections::HashSet;

use uuid::Uuid;

use crate::time::{Timeout, Timer};
use crate::util::{
    not_leader_add_server, not_leader_command, not_leader_register_client, not_leader_remove_server,
};
use crate::{
    ClientRequest, ClientRequestContent, Follower, Leader, RaftMessage, RaftMessageContent,
    RaftState, RequestVoteResponseArgs, Server, ServerState,
};

pub(crate) struct Candidate {
    ballot_box: BallotBox,
    election_timer: Timer,
}

impl Candidate {
    async fn new(server: &mut Server) -> ServerState {
        server.current_term.set(*server.current_term + 1).await;
        // TODO: fix number of servers
        let mut candidate = Candidate {
            ballot_box: BallotBox::new(server.servers.len()),
            election_timer: Timer::new_election_timer(server),
        };

        server.voted_for.set(Some(server.config.self_id)).await;
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
        match &msg.content {
            RaftMessageContent::AppendEntries(args) => todo!(), // turn to leader
            RaftMessageContent::RequestVote(args) => todo!(),   // respond with no
            RaftMessageContent::RequestVoteResponse(args) => {
                let vote = Vote::from_msg(msg.header.source, args.vote_granted);
                if self.ballot_box.add_vote(vote) == VotingResult::Won {
                    Some(Leader::transition_from_canditate(server).await.into())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        match req.content {
            ClientRequestContent::Command { .. } => {
                let _ = req.reply_to.send(not_leader_command(None)).await;
            }
            ClientRequestContent::Snapshot => todo!(),
            ClientRequestContent::AddServer { .. } => {
                let _ = req.reply_to.send(not_leader_add_server(None)).await;
            }
            ClientRequestContent::RemoveServer { .. } => {
                let _ = req.reply_to.send(not_leader_remove_server(None)).await;
            }
            ClientRequestContent::RegisterClient => {
                let _ = req.reply_to.send(not_leader_register_client(None)).await;
            }
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
