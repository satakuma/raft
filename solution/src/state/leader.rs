use std::{cmp::min, collections::HashMap};
use uuid::Uuid;

use crate::{
    AppendEntriesArgs, ClientRequest, ClientRequestContent, RaftMessage, RaftState, Server,
    ServerState, Timeout, Timer,
};

pub(crate) struct Leader {
    next_index: HashMap<Uuid, usize>,
    match_index: HashMap<Uuid, usize>,
    heartbeat_timer: Timer,
}

impl Leader {
    async fn replicate_log_with_follower(&self, server: &Server, follower: Uuid) {
        let prev_log_index = self.next_index[&follower] - 1;

        let num_entries = min(
            server.log.last_index() - prev_log_index,
            server.config.append_entries_batch_size,
        );
        let msg = AppendEntriesArgs {
            prev_log_index,
            prev_log_term: server.pstate.current_term,
            entries: server.log[prev_log_index..prev_log_index + num_entries].to_vec(),
            leader_commit: server.commit_index,
        };
        server.send(follower, msg.into()).await;
    }

    async fn replicate_log(&self, server: &mut Server) {
        for server_id in &server.all_servers {
            if *server_id != server.config.self_id {
                self.replicate_log_with_follower(server, *server_id).await;
            }
        }
    }

    async fn heartbeat(&self, server: &mut Server) {
        // We use AppendEntries messages for heartbeat.
        // It is a countermeasure to deal with lost packets / slow followers etc.
        // which ensures that eventually all followers will store all log entries.
        self.replicate_log(server).await;
    }

    pub(crate) async fn transition_from_canditate(server: &mut Server) -> ServerState {
        server
            .pstate
            .update_with(|ps| {
                ps.voted_for = Some(server.config.self_id);
                ps.leader_id = Some(server.config.self_id);
            })
            .await;

        let next_index = server
            .all_servers
            .iter()
            .cloned()
            .map(|s| (s, server.log.last_index() + 1))
            .collect();
        let match_index = server.all_servers.iter().cloned().map(|s| (s, 0)).collect();

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
                self.heartbeat(server).await;
                None
            }
            _ => None,
        }
    }
}
