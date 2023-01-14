use std::collections::HashMap;

use uuid::Uuid;

use crate::{Timeout, Timer, ClientRequest, ClientRequestContent, Node, NodeState, RaftMessage};

pub(crate) struct Leader {
    next_index: HashMap<Uuid, usize>,
    match_index: HashMap<Uuid, usize>,
    heartbeat_timer: Timer,
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
        match msg {
            Timeout::Heartbeat => {
                todo!()
            }
            _ => None,
        }
    }

    pub(crate) async fn transition_from_canditate(node: &mut Node) -> NodeState {
        node.voted_for.set(Some(node.config.self_id)).await;
        node.leader_id.set(Some(node.config.self_id)).await;

        let next_index = node.servers.iter().cloned().map(|s| (s, node.log.len())).collect();
        let match_index = node.servers.iter().cloned().map(|s| (s, 0)).collect();

        Leader {
            next_index,
            match_index,
            heartbeat_timer: Timer::new_heartbeat_timer(node),
        }.into()
    }
}
