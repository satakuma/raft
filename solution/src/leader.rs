use crate::{time::Timeout, ClientRequest, ClientRequestContent, Node, NodeState, RaftMessage};

pub(crate) struct Leader {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

impl Leader {
    pub(crate) async fn handle_raft_msg(
        &mut self,
        node: &mut Node,
        msg: RaftMessage,
    ) -> Option<NodeState> {
        todo!()
    }

    pub(crate) async fn handle_client_req(&mut self, node: &mut Node, req: ClientRequest) {
        match req.content {
            ClientRequestContent::Command { .. } => todo!(),
            ClientRequestContent::Snapshot => todo!(),
            ClientRequestContent::AddServer { .. } => todo!(),
            ClientRequestContent::RemoveServer { .. } => todo!(),
            ClientRequestContent::RegisterClient => todo!(),
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
