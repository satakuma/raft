use uuid::Uuid;

use crate::candidate::Candidate;
use crate::time::{Timeout, Timer};
use crate::util::{
    not_leader_add_server, not_leader_command, not_leader_register_client, not_leader_remove_server,
};
use crate::{ClientRequest, ClientRequestContent, Node, NodeState, RaftMessage};

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

    pub(crate) fn reset_timer(&mut self, node: &Node) {
        if let Some(timer) = &mut self.election_timer {
            timer.reset();
        } else {
            self.election_timer = Some(Timer::new_election_timer(node));
        }
    }

    pub(crate) async fn handle_raft_msg(
        &mut self,
        node: &mut Node,
        msg: RaftMessage,
    ) -> Option<NodeState> {
        todo!()
    }

    pub(crate) async fn handle_client_req(&mut self, node: &mut Node, req: ClientRequest) {
        match req.content {
            ClientRequestContent::Command { .. } => {
                let _ = req.reply_to.send(not_leader_command(self.leader)).await;
            }
            ClientRequestContent::Snapshot => todo!(),
            ClientRequestContent::AddServer { .. } => {
                let _ = req.reply_to.send(not_leader_add_server(self.leader)).await;
            }
            ClientRequestContent::RemoveServer { .. } => {
                let _ = req
                    .reply_to
                    .send(not_leader_remove_server(self.leader))
                    .await;
            }
            ClientRequestContent::RegisterClient => {
                let _ = req
                    .reply_to
                    .send(not_leader_register_client(self.leader))
                    .await;
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
                Some(Candidate::transition_from_follower(node).await)
            }
            _ => None,
        }
    }
}
