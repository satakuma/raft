use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::domain::*;
use crate::{LogEntryMetadata, Persistent, Server};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Snapshot {
    pub data: Vec<u8>,
    pub last_included: LogEntryMetadata,
}

impl Snapshot {
    pub fn new(data: Vec<u8>, last_included: LogEntryMetadata) -> Snapshot {
        Snapshot {
            data,
            last_included,
        }
    }
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

pub(crate) struct Sender {
    snapshot: Snapshot,
    follower: Uuid,
    max_chunk_size: usize,
    offset: usize,
}

impl Sender {
    pub fn new(snapshot: Snapshot, max_chunk_size: usize, follower: Uuid) -> Sender {
        Sender {
            snapshot,
            follower,
            max_chunk_size,
            offset: 0,
        }
    }

    pub async fn send_chunk(&self, server: &Server) {
        let chunk_size = self.get_chunk_size();
        let chunk = self.snapshot.data[self.offset..self.offset + chunk_size].to_vec();

        let last_log = self.snapshot.last_included;
        let (last_config, client_sessions) = if self.offset == 0 {
            (Some(server.all_servers.clone()), None)
        } else {
            (None, None)
        };

        let new_offset = self.offset + chunk_size;
        let done = new_offset == self.snapshot.size();
        server
            .send(
                self.follower,
                InstallSnapshotArgs {
                    last_included_index: last_log.index,
                    last_included_term: last_log.term,
                    last_config,
                    client_sessions,
                    offset: self.offset,
                    data: chunk,
                    done,
                }
                .into(),
            )
            .await;
    }

    /// Called when `InstallSnapshotResponse` is received.
    /// Returns true if the acknowledged chunk was the last one.
    pub fn chunk_acknowledged(&mut self, ack_offset: usize) -> Status {
        if ack_offset == self.offset {
            let chunk_size = self.get_chunk_size();
            self.offset += chunk_size;

            if self.offset == self.snapshot.size() {
                Status::Done
            } else {
                Status::Pending
            }
        } else {
            Status::Duplicate
        }
    }

    pub fn last_included(&self) -> LogEntryMetadata {
        self.snapshot.last_included
    }

    fn get_chunk_size(&self) -> usize {
        min(self.snapshot.size() - self.offset, self.max_chunk_size)
    }
}

pub(crate) struct Receiver(Persistent<ReceiverInner>);

#[derive(Serialize, Deserialize)]
enum ReceiverInner {
    Pending(PendingState),
    Ready(Snapshot),
    Finished,
}

impl ReceiverInner {
    fn pending(self) -> PendingState {
        match self {
            ReceiverInner::Pending(inner) => inner,
            _ => panic!("tried to use Receiver after Ready"),
        }
    }

    fn pending_mut(&mut self) -> &mut PendingState {
        match self {
            ReceiverInner::Pending(inner) => inner,
            _ => panic!("tried to use Receiver after Ready"),
        }
    }

    fn snapshot(self) -> Snapshot {
        match self {
            ReceiverInner::Ready(inner) => inner,
            _ => panic!("tried to a snapshot from not Ready receiver"),
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self, ReceiverInner::Ready(_))
    }

    fn is_finished(&self) -> bool {
        matches!(self, ReceiverInner::Finished)
    }
}

impl Default for ReceiverInner {
    fn default() -> ReceiverInner {
        ReceiverInner::Pending(Default::default())
    }
}

#[derive(Default, Serialize, Deserialize)]
struct PendingState {
    data: Vec<u8>,
    last_log: Option<LogEntryMetadata>,
    last_config: Option<HashSet<Uuid>>,
    client_sessions: Option<HashMap<Uuid, ClientSession>>,
}

impl PendingState {
    async fn receive_chunk(&mut self, args: InstallSnapshotArgs) -> Status {
        assert_eq!(self.data.len(), args.offset);

        self.data.extend_from_slice(&args.data);
        self.last_log = Some((args.last_included_term, args.last_included_index).into());
        self.last_config = self.last_config.take().or(args.last_config);
        self.client_sessions = self.client_sessions.take().or(args.client_sessions);

        if args.done {
            Status::Done
        } else {
            Status::Pending
        }
    }

    fn into_snapshot(self) -> Snapshot {
        Snapshot {
            data: self.data,
            last_included: self.last_log.unwrap(),
        }
    }
}

impl Receiver {
    pub async fn recovery(server: &Server) -> Option<Receiver> {
        if let Some(inner) = Persistent::recover("snapshot_receiver", server.storage()).await {
            match &*inner {
                ReceiverInner::Finished => None,
                _ => Some(Receiver(inner)),
            }
        } else {
            None
        }
    }

    pub async fn new(server: &Server) -> Receiver {
        let inner =
            Persistent::recover_or("snapshot_receiver", server.storage(), Default::default()).await;
        Receiver(inner)
    }

    pub async fn receive_chunk(&mut self, args: InstallSnapshotArgs) -> Status {
        let mut guard = self.0.mutate();
        let result = guard.pending_mut().receive_chunk(args).await;
        if result == Status::Done {
            // Some trickery to avoid cloning.
            let inner = std::mem::replace(&mut *guard, ReceiverInner::Finished);
            *guard = ReceiverInner::Ready(inner.pending().into_snapshot())
        }
        guard.save().await;
        result
    }

    pub async fn install_snapshot(&mut self, server: &mut Server) {
        let mut guard = self.0.mutate();
        let inner = std::mem::replace(&mut *guard, ReceiverInner::Finished);
        server.install_snapshot(inner.snapshot()).await;
        *guard = ReceiverInner::Finished;
        guard.save().await;
    }

    pub fn is_ready(&self) -> bool {
        self.0.is_ready()
    }

    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Status {
    Pending,
    Done,
    Duplicate,
}
