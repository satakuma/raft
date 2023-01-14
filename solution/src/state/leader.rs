use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    ClientRequest, ClientRequestContent, RaftMessage, RaftState, Server, ServerState, Timeout,
    Timer,
};

pub(crate) struct Leader {
    next_index: HashMap<Uuid, usize>,
    match_index: HashMap<Uuid, usize>,
    heartbeat_timer: Timer,
}

impl Leader {
    async fn heartbeat(&self) {}

    pub(crate) async fn transition_from_canditate(server: &mut Server) -> ServerState {
        server.voted_for.set(Some(server.config.self_id)).await;
        server.leader_id.set(Some(server.config.self_id)).await;

        let next_index = server
            .servers
            .iter()
            .cloned()
            .map(|s| (s, server.log.len()))
            .collect();
        let match_index = server.servers.iter().cloned().map(|s| (s, 0)).collect();

        Leader {
            next_index,
            match_index,
            heartbeat_timer: Timer::new_heartbeat_timer(server),
        }
        .into()
    }
}

#[async_trait::async_trait]
impl RaftState for Leader {
    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        todo!()
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        match req.content {
            ClientRequestContent::Command { .. } => todo!(),
            ClientRequestContent::Snapshot => todo!(),
            ClientRequestContent::AddServer { .. } => todo!(),
            ClientRequestContent::RemoveServer { .. } => todo!(),
            ClientRequestContent::RegisterClient => todo!(),
        }
    }

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState> {
        match msg {
            Timeout::Heartbeat => {
                self.heartbeat().await;
                None
            }
            _ => None,
        }
    }
}
