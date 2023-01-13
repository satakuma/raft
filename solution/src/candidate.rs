use crate::time::Timeout;
use crate::util::{
    not_leader_add_server, not_leader_command, not_leader_register_client, not_leader_remove_server,
};
use crate::{ClientRequest, ClientRequestContent, Node, NodeState, RaftMessage};

pub(crate) struct Candidate {}

impl Candidate {
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
        todo!()
    }
}
