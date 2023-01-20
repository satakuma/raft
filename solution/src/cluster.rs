use std::{collections::HashSet, fmt, ops::RangeInclusive, time::Duration};

use uuid::Uuid;

use crate::domain::*;
use crate::{ClientSender, ServerConfig};

pub(crate) struct DynamicConfig {
    // Static fields
    pub self_id: Uuid,
    pub election_timeout_range: RangeInclusive<Duration>,
    pub heartbeat_timeout: Duration,
    pub append_entries_batch_size: usize,
    pub snapshot_chunk_size: usize,
    pub catch_up_rounds: u64,
    pub session_expiration: Duration,

    servers: HashSet<Uuid>,
    current_change: Option<Change>,
}

impl DynamicConfig {
    pub(crate) fn from_static_config(config: ServerConfig) -> DynamicConfig {
        DynamicConfig {
            self_id: config.self_id,
            election_timeout_range: config.election_timeout_range,
            heartbeat_timeout: config.heartbeat_timeout,
            append_entries_batch_size: config.append_entries_batch_size,
            snapshot_chunk_size: config.snapshot_chunk_size,
            catch_up_rounds: config.catch_up_rounds,
            session_expiration: config.session_expiration,
            servers: config.servers,
            current_change: None,
        }
    }

    pub(crate) fn initialize(&mut self, servers: &HashSet<Uuid>) {
        self.servers = servers.clone();
    }

    pub(crate) fn servers(&self) -> &HashSet<Uuid> {
        &self.servers
    }

    pub(crate) fn servers_updated(&self) -> Option<HashSet<Uuid>> {
        self.current_change.as_ref().map(|chg| {
            let mut new_servers = self.servers.clone();
            chg.delta.apply_to(&mut new_servers);
            new_servers
        })
    }

    pub(crate) fn change_status(&self) -> Option<ChangeStatus> {
        self.current_change.as_ref().map(|chg| chg.status)
    }

    pub(crate) fn new_change(
        &mut self,
        sender: &ClientSender,
        delta: Delta,
    ) -> Result<(), ConfigChangeError> {
        if self.current_change.is_some() {
            return Err(ConfigChangeError::ChangeInProgress);
        }
        self.current_change = Some(Change::new(sender.clone(), delta));
        Ok(())
    }

    pub(crate) fn discard_change(&mut self) {
        self.current_change = None;
    }

    pub(crate) fn change_replicated(&mut self, index: usize) {
        if let Some(chg) = &mut self.current_change {
            chg.status = ChangeStatus::Commit { index };
        }
    }

    pub(crate) async fn config_committed(&mut self, config_index: usize, servers: &HashSet<Uuid>) {
        if let Some(chg) = &mut self.current_change {
            if let ChangeStatus::Commit { index } = chg.status {
                if config_index == index {
                    let chg = self.current_change.take().unwrap();
                    let response = match chg.delta {
                        Delta::Plus(new_server) => {
                            let content = if self.servers.len() + 1 != servers.len() {
                                AddServerResponseContent::AlreadyPresent
                            } else {
                                self.initialize(servers);
                                AddServerResponseContent::ServerAdded
                            };
                            AddServerResponseArgs {
                                new_server,
                                content,
                            }
                            .into()
                        }
                        Delta::Minus(old_server) => {
                            let content = if self.servers.len() - 1 != servers.len() {
                                RemoveServerResponseContent::NotPresent
                            } else if servers.is_empty() {
                                RemoveServerResponseContent::OneServerLeft
                            } else {
                                self.initialize(servers);
                                RemoveServerResponseContent::ServerRemoved
                            };
                            RemoveServerResponseArgs {
                                old_server,
                                content,
                            }
                            .into()
                        }
                    };
                    let _ = chg.sender.send(response).await;
                } else {
                    unreachable!()
                }
            } else {
                panic!("invalid state in config change")
            }
        } else {
            self.initialize(servers);
        }
    }
}

pub(crate) struct Change {
    sender: ClientSender,
    delta: Delta,
    status: ChangeStatus,
}

impl Change {
    fn new(sender: ClientSender, delta: Delta) -> Change {
        let status = match delta {
            Delta::Plus(_) => ChangeStatus::WaitingNop,
            Delta::Minus(_) => ChangeStatus::WaitingNop,
        };
        Change {
            sender,
            delta,
            status,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Delta {
    Plus(Uuid),
    Minus(Uuid),
}

impl Delta {
    fn apply_to(&self, servers: &mut HashSet<Uuid>) {
        match self {
            Delta::Plus(id) => servers.insert(*id),
            Delta::Minus(id) => servers.remove(id),
        };
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(crate) enum ChangeStatus {
    CatchingUp,
    WaitingNop,
    Commit {
        index: usize, // waiting for this entry
    },
}

#[derive(Clone, Debug)]
pub(crate) enum ConfigChangeError {
    ChangeInProgress,
}

impl fmt::Display for ConfigChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <ConfigChangeError as fmt::Debug>::fmt(self, f)
    }
}

impl std::error::Error for ConfigChangeError {}
