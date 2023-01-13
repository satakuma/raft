#![allow(clippy::await_holding_refcell_ref)]

use executor::{Handler, ModuleRef, System};
use std::{cell::RefCell, rc::Rc, time::SystemTime};
use uuid::Uuid;

mod domain;
pub use domain::*;

pub(crate) mod storage;
use storage::{Persistent, PersistentVec};

mod follower;
pub(crate) use follower::Follower;

mod candidate;
pub(crate) use candidate::Candidate;

mod leader;
pub(crate) use leader::Leader;

pub(crate) mod util;

pub(crate) mod time;
pub(crate) use time::{Timeout, Timer};

struct Node {
    config: ServerConfig,
    self_ref: Option<ModuleRef<Raft>>,
    state_machine: Box<dyn StateMachine>,
    sender: Box<dyn RaftSender>,

    current_term: Persistent<u64>,
    voted_for: Persistent<Uuid>,
    log: PersistentVec<LogEntry>,
}

impl Node {
    pub(crate) fn self_ref(&self) -> ModuleRef<Raft> {
        // Unwrapping is safe because this field is set during Raft initialization.
        self.self_ref.as_ref().unwrap().clone()
    }
}

// Safety: Send has to be manually implemented due to the `Rc` inside.
// Fields using Rc are private and are never shared outside of the
// Raft struct. Thus the data pointed by Rc is never shared between threads.
unsafe impl Send for Node {}

pub struct Raft {
    node: Node,
    state: NodeState,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let storage = Rc::new(RefCell::new(stable_storage));
        let current_term = Persistent::new("current_term", 0, &storage).await;
        let voted_for = Persistent::new("voted_for", Uuid::nil(), &storage).await;

        let mut log = PersistentVec::new("log", &storage).await;
        if log.is_empty() {
            log.push(LogEntry {
                content: LogEntryContent::Configuration {
                    servers: config.servers.clone(),
                },
                term: 0,
                timestamp: first_log_entry_timestamp,
            })
            .await;
        }

        let node = Node {
            config,
            self_ref: None,
            state_machine,
            sender: message_sender,
            current_term,
            voted_for,
            log,
        };
        let raft = Raft {
            node,
            state: NodeState::initial(),
        };
        let mref = system.register_module(raft).await;
        mref.send(Start).await;
        mref
    }

    async fn handle_raft_msg(&mut self, msg: RaftMessage) {
        let transition = match &mut self.state {
            NodeState::Follower(inner) => inner.handle_raft_msg(&mut self.node, msg).await,
            NodeState::Candidate(inner) => inner.handle_raft_msg(&mut self.node, msg).await,
            NodeState::Leader(inner) => inner.handle_raft_msg(&mut self.node, msg).await,
        };
        if let Some(next_state) = transition {
            self.state = next_state;
        }
    }

    async fn handle_client_req(&mut self, req: ClientRequest) {
        match &mut self.state {
            NodeState::Follower(inner) => inner.handle_client_req(&mut self.node, req).await,
            NodeState::Candidate(inner) => inner.handle_client_req(&mut self.node, req).await,
            NodeState::Leader(inner) => inner.handle_client_req(&mut self.node, req).await,
        };
    }

    async fn handle_timeout(&mut self, msg: Timeout) {
        let transition = match &mut self.state {
            NodeState::Follower(inner) => inner.handle_timeout(&mut self.node, msg).await,
            NodeState::Candidate(inner) => inner.handle_timeout(&mut self.node, msg).await,
            NodeState::Leader(inner) => inner.handle_timeout(&mut self.node, msg).await,
        };
        if let Some(next_state) = transition {
            self.state = next_state;
        }
    }
}

/// Message sent exactly once at Raft initialization.
/// Allows to set the self ModuleRef and start timeout timers.
struct Start;

#[async_trait::async_trait]
impl Handler<Start> for Raft {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, _msg: Start) {
        self.node.self_ref = Some(self_ref.clone());
        match &mut self.state {
            NodeState::Follower(inner) => inner.reset_timer(&self.node),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        self.handle_raft_msg(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        self.handle_client_req(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: Timeout) {
        self.handle_timeout(msg).await;
    }
}

enum NodeState {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl NodeState {
    fn initial() -> NodeState {
        Follower::initial().into()
    }
}

macro_rules! node_state_impl_conversion {
    ($state:ident, $getfn:ident, $getfn_mut:ident) => {
        impl From<$state> for NodeState {
            fn from(value: $state) -> NodeState {
                NodeState::$state(value)
            }
        }

        impl NodeState {
            pub(crate) fn $getfn(&self) -> Option<&$state> {
                match self {
                    NodeState::$state(inner) => Some(inner),
                    _ => None,
                }
            }

            pub(crate) fn $getfn_mut(&mut self) -> Option<&mut $state> {
                match self {
                    NodeState::$state(inner) => Some(inner),
                    _ => None,
                }
            }
        }
    };
}

node_state_impl_conversion!(Follower, follower, follower_mut);
node_state_impl_conversion!(Candidate, candidate, candidate_mut);
node_state_impl_conversion!(Leader, leader, leader_mut);
