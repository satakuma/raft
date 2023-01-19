use std::{collections::HashSet, ops::RangeInclusive, time::Duration};

use uuid::Uuid;

use crate::ServerConfig;

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
        }
    }

    pub(crate) fn initialize(&mut self, servers: &HashSet<Uuid>) {
        self.servers = servers.clone();
    }

    pub(crate) fn servers(&self) -> &HashSet<Uuid> {
        &self.servers
    }
}
