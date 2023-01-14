use uuid::Uuid;

use crate::time::{Timeout, Timer};
use crate::util::{
    not_leader_add_server, not_leader_command, not_leader_register_client, not_leader_remove_server,
};
use crate::{
    Candidate, ClientRequest, ClientRequestContent, RaftMessage, RaftMessageContent, RaftState,
    Server, ServerState,
};

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

    pub(crate) async fn follow_new_term_leader(
        server: &mut Server,
        term: u64,
        leader: Uuid,
    ) -> ServerState {
        server
            .pstate
            .update_with(|ps| {
                ps.current_term = term;
                ps.voted_for = None;
                ps.leader_id = None;
            })
            .await;

        Follower {
            leader: leader.into(),
            election_timer: Timer::new_election_timer(server).into(),
        }
        .into()
    }

    pub(crate) fn reset_timer(&mut self, server: &Server) {
        if let Some(timer) = &mut self.election_timer {
            timer.reset();
        } else {
            self.election_timer = Some(Timer::new_election_timer(server));
        }
    }
}

#[async_trait::async_trait]
impl RaftState for Follower {
    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        match &msg.content {
            RaftMessageContent::AppendEntries(args) => {
                // append entries
                None
            }
            RaftMessageContent::RequestVote(args) => {
                assert!(server.pstate.current_term == msg.header.term);
                if server.pstate.voted_for.is_none()
                    || server.pstate.voted_for == Some(msg.header.source)
                {
                    // check log and reply
                }
                None
            }
            _ => None,
        }
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
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

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState> {
        match msg {
            Timeout::Election => Some(Candidate::transition_from_follower(server).await),
            _ => None,
        }
    }
}
