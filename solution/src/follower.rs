use rand::Rng;
use uuid::Uuid;

use crate::time::{Timeout, Timer};
use crate::util::{
    not_leader_add_server, not_leader_command, not_leader_register_client, not_leader_remove_server,
};
use crate::{ClientRequest, ClientRequestContent, Node, NodeState, RaftMessage};

pub(crate) struct Follower {
    leader: Option<Uuid>,
    timer: Option<Timer>,
}

impl Follower {
    fn new_election_timer(node: &Node) -> Timer {
        let dur = rand::thread_rng().gen_range(node.config.election_timeout_range.clone());
        Timer::new(node.self_ref(), dur, Timeout::Election)
    }

    pub(crate) fn initial() -> Follower {
        Follower {
            leader: None,
            timer: None,
        }
    }

    pub(crate) fn reset_timer(&mut self, node: &Node) {
        if let Some(timer) = &mut self.timer {
            timer.reset();
        } else {
            self.timer = Some(Follower::new_election_timer(node));
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
        todo!()
    }
}
