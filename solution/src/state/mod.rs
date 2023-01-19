//! Raft server states and transitions between them.
//!
//! This module contains a trait `RaftState`, which is implemented for every
//! server state. It allows to handle messages and compute transitions to
//! other states, which are later performed by the outer `Raft` struct.

mod follower;
pub(crate) use follower::Follower;

mod candidate;
pub(crate) use candidate::Candidate;

mod leader;
pub(crate) use leader::Leader;

use crate::domain::*;
use crate::{Server, Timeout};

#[async_trait::async_trait]
pub(crate) trait RaftState {
    /// Called every time a new Raft message is received, before the actual processing of a message.
    /// Returns true if the message should be ignored.
    fn ignore_raft_msg(&self, _server: &Server, _msg: &RaftMessage) -> bool {
        false
    }

    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState>;

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest);

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState>;
}

pub(crate) enum ServerState {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl ServerState {
    pub(crate) async fn init_recovery(server: &mut Server) -> ServerState {
        Follower::init_recovery(server).await.into()
    }

    pub(crate) fn raft_server_start(&mut self, server: &Server) {
        self.follower_mut().unwrap().raft_server_start(server);
    }
}

#[async_trait::async_trait]
impl RaftState for ServerState {
    fn ignore_raft_msg(&self, server: &Server, msg: &RaftMessage) -> bool {
        // Delegate it to the inner state.
        match self {
            ServerState::Follower(inner) => inner.ignore_raft_msg(server, msg),
            ServerState::Candidate(inner) => inner.ignore_raft_msg(server, msg),
            ServerState::Leader(inner) => inner.ignore_raft_msg(server, msg),
        }
    }

    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        // All RPCs: reply false if term < currentTerm.
        if msg.header.term < server.pstate.current_term {
            server.respond_false(&msg).await;
            return None;
        }

        // All servers: if request or response contains a newer term,
        // set current_term and convert to follower (3.3).
        if msg.header.term > server.pstate.current_term {
            let mut follower = Follower::new_term_discovered(server, msg.header.term).await;
            return follower
                .handle_raft_msg(server, msg)
                .await
                .or(Some(follower));
        }

        // Otherwise delegate message to the inner state assuming term == current_term.
        match self {
            ServerState::Follower(inner) => inner.handle_raft_msg(server, msg).await,
            ServerState::Candidate(inner) => inner.handle_raft_msg(server, msg).await,
            ServerState::Leader(inner) => inner.handle_raft_msg(server, msg).await,
        }
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        if let ClientRequestContent::Snapshot = &req.content {
            server.take_log_snapshot(req.reply_to).await;
        } else {
            match self {
                ServerState::Follower(inner) => inner.handle_client_req(server, req).await,
                ServerState::Candidate(inner) => inner.handle_client_req(server, req).await,
                ServerState::Leader(inner) => inner.handle_client_req(server, req).await,
            };
        }
    }

    async fn handle_timeout(&mut self, server: &mut Server, msg: Timeout) -> Option<ServerState> {
        match self {
            ServerState::Follower(inner) => inner.handle_timeout(server, msg).await,
            ServerState::Candidate(inner) => inner.handle_timeout(server, msg).await,
            ServerState::Leader(inner) => inner.handle_timeout(server, msg).await,
        }
    }
}

macro_rules! impl_server_state_conversion {
    ($state:ident, $getfn:ident, $getfn_mut:ident) => {
        impl From<$state> for ServerState {
            fn from(value: $state) -> ServerState {
                ServerState::$state(value)
            }
        }

        #[allow(dead_code)]
        impl ServerState {
            pub(crate) fn $getfn(&self) -> Option<&$state> {
                match self {
                    ServerState::$state(inner) => Some(inner),
                    _ => None,
                }
            }

            pub(crate) fn $getfn_mut(&mut self) -> Option<&mut $state> {
                match self {
                    ServerState::$state(inner) => Some(inner),
                    _ => None,
                }
            }
        }
    };
}

impl_server_state_conversion!(Follower, follower, follower_mut);
impl_server_state_conversion!(Candidate, candidate, candidate_mut);
impl_server_state_conversion!(Leader, leader, leader_mut);
