use std::collections::HashSet;

use uuid::Uuid;

use crate::leader::Leader;
use crate::time::{Timeout, Timer};
use crate::util::{
    not_leader_add_server, not_leader_command, not_leader_register_client, not_leader_remove_server,
};
use crate::{ClientRequest, ClientRequestContent, Node, NodeState, RaftMessage, RequestVoteResponseArgs, RaftMessageContent};

pub(crate) struct Candidate {
    ballot_box: BallotBox,
    election_timer: Timer,
}

impl Candidate {
    pub(crate) async fn handle_raft_msg(
        &mut self,
        node: &mut Node,
        msg: RaftMessage,
    ) -> Option<NodeState> {
        match &msg.content {
            RaftMessageContent::AppendEntries(args) => todo!(), // turn to leader
            RaftMessageContent::RequestVote(args) => todo!(), // respond with no
            RaftMessageContent::RequestVoteResponse(args) => {
                let vote = Vote::from_msg(msg.header.source, args.vote_granted);
                if self.ballot_box.add_vote(vote) == VotingResult::Won {
                    Some(Leader::transition_from_canditate(node).await.into())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub(crate) async fn handle_client_req(&mut self, node: &mut Node, req: ClientRequest) {
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

    pub(crate) async fn handle_timeout(
        &mut self,
        node: &mut Node,
        msg: Timeout,
    ) -> Option<NodeState> {
        match msg {
            Timeout::Election => {
                Some(Candidate::loop_from_candidate(node).await)
            }
            _ => None,
        }
    }

    async fn new(node: &mut Node) -> NodeState {
        node.current_term.set(*node.current_term + 1).await;
        // TODO: fix number of servers
        let mut candidate = Candidate {
            ballot_box: BallotBox::new(node.servers.len()),
            election_timer: Timer::new_election_timer(node)
        };

        node.voted_for.set(Some(node.config.self_id)).await;
        if candidate.ballot_box.add_vote(Vote::self_vote(node)) == VotingResult::Won {
            Leader::transition_from_canditate(node).await.into()
        } else {
            candidate.into()
        }
    }

    pub(crate) async fn transition_from_follower(node: &mut Node) -> NodeState {
        Candidate::new(node).await
    }

    pub(crate) async fn loop_from_candidate(node: &mut Node) -> NodeState {
        Candidate::new(node).await
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
    fn self_vote(node: &Node) -> Vote {
        Vote {
            from: node.config.self_id,
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