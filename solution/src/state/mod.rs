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

use crate::{time::Timeout, ClientRequest, RaftMessage, Server};

#[async_trait::async_trait]
pub(crate) trait RaftState {
    /// Called every time a new Raft message is received, before the actual processing of a message.
    async fn at_new_raft_message(
        &mut self,
        server: &mut Server,
        msg: &RaftMessage,
    ) -> Option<ServerState> {
        if msg.header.term > server.pstate.current_term {
            Some(Follower::follow_new_term_leader(server, msg.header.term, msg.header.source).await)
        } else {
            None
        }
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
    pub(crate) fn initial() -> ServerState {
        Follower::initial().into()
    }
}

#[async_trait::async_trait]
impl RaftState for ServerState {
    async fn at_new_raft_message(
        &mut self,
        server: &mut Server,
        msg: &RaftMessage,
    ) -> Option<ServerState> {
        // Delegate it to the inner state.
        match self {
            ServerState::Follower(inner) => inner.at_new_raft_message(server, msg).await,
            ServerState::Candidate(inner) => inner.at_new_raft_message(server, msg).await,
            ServerState::Leader(inner) => inner.at_new_raft_message(server, msg).await,
        }
    }

    async fn handle_raft_msg(
        &mut self,
        server: &mut Server,
        msg: RaftMessage,
    ) -> Option<ServerState> {
        match self {
            ServerState::Follower(inner) => inner.handle_raft_msg(server, msg).await,
            ServerState::Candidate(inner) => inner.handle_raft_msg(server, msg).await,
            ServerState::Leader(inner) => inner.handle_raft_msg(server, msg).await,
        }
    }

    async fn handle_client_req(&mut self, server: &mut Server, req: ClientRequest) {
        match self {
            ServerState::Follower(inner) => inner.handle_client_req(server, req).await,
            ServerState::Candidate(inner) => inner.handle_client_req(server, req).await,
            ServerState::Leader(inner) => inner.handle_client_req(server, req).await,
        };
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
