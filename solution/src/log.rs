use serde::{Deserialize, Serialize};
use std::cmp::min;

use crate::domain::*;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Log {
    snapshot: Option<LogSnapshot>,
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn empty() -> Log {
        Log {
            snapshot: None,
            entries: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.snapshot_size() == 0 && self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.snapshot_len() + self.entries.len()
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.entries.get(index - self.snapshot_len())
    }

    pub fn slice(&self, from: usize, to: usize) -> &[LogEntry] {
        let offset = self.snapshot_len();
        &self.entries[from - offset..to - offset]
    }

    pub fn push(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn truncate(&mut self, len: usize) {
        assert!(len >= self.snapshot_len());
        self.entries.truncate(len - self.snapshot_len());
    }

    pub fn snapshot(&self) -> Option<&LogSnapshot> {
        self.snapshot.as_ref()
    }

    /// Returns number of entries in the snapshot
    pub fn snapshot_len(&self) -> usize {
        self.snapshot_last_included().map_or(0, |md| md.index + 1)
    }

    /// Returns number of bytes of the snapshot
    pub fn snapshot_size(&self) -> usize {
        self.snapshot.as_ref().map_or(0, LogSnapshot::size)
    }

    /// Returns reference to the snapshot data.
    pub fn snapshot_data(&self) -> Option<&[u8]> {
        self.snapshot.as_ref().map(|s| s.data.as_slice())
    }

    /// Returns metadata of the last included entry in the snapshot or `None`,
    /// if the snapshot is empty.
    pub fn snapshot_last_included(&self) -> Option<LogEntryMetadata> {
        self.snapshot.as_ref().map(|s| s.last_included)
    }

    pub fn get_metadata(&self, index: usize) -> Option<LogEntryMetadata> {
        if self.first_not_snapshotted_index() <= index && index < self.len() {
            LogEntryMetadata {
                term: self.entries[index].term,
                index,
            }
            .into()
        } else {
            // Check if the requested index is the last one included in the snapshot,
            // as we hold this information and can retrieve it.
            match self.snapshot_last_included() {
                Some(md) if md.index == index => Some(md),
                _ => None,
            }
        }
    }

    pub fn first_not_snapshotted_index(&self) -> usize {
        self.snapshot_len()
    }

    pub fn last_index(&self) -> usize {
        self.len() - 1
    }

    pub fn last_metadata(&self) -> LogEntryMetadata {
        if self.entries.is_empty() {
            self.snapshot_last_included().unwrap()
        } else {
            (self.entries.last().unwrap().term, self.last_index()).into()
        }
    }

    pub fn is_up_to_date_with(&self, term_index: LogEntryMetadata) -> bool {
        self.last_metadata() <= term_index
    }

    pub fn append_entries(
        &mut self,
        new_entries: Vec<LogEntry>,
        prev_log_md: LogEntryMetadata,
    ) -> bool {
        // This also catches the case where the previous log entry is deeply behind
        // in our snapshot and we cannot append entries to it.
        if self.get_metadata(prev_log_md.index) != Some(prev_log_md) {
            return false;
        }

        for (i, entry) in new_entries.into_iter().enumerate() {
            match self.get_metadata(prev_log_md.index + i + 1) {
                Some(md) if md == prev_log_md => {
                    // This entry is already in the log.
                    continue;
                }
                _ => {
                    // Log entries from this point either conflict or don't exist.
                    // Truncate the log (possibly no-op) and add a new entry.
                    self.truncate(prev_log_md.index + i + 1);
                    self.push(entry);
                }
            }
        }
        true
    }

    pub fn apply_snapshot(&mut self, snapshot: LogSnapshot) -> usize {
        let num_snapshotted_entries = snapshot.last_included.index + 1 - self.snapshot_len();
        let max_index = min(num_snapshotted_entries, self.entries.len());
        self.entries.drain(..max_index);
        self.snapshot = Some(snapshot);

        num_snapshotted_entries
    }

    pub fn take_snapshot(&mut self, data: Vec<u8>, last_index: usize) -> usize {
        assert!(self.snapshot_len() <= last_index);
        if last_index < self.first_not_snapshotted_index() {
            return 0;
        }

        let last_included = match self.get_metadata(last_index) {
            Some(md) => md,
            None => return 0,
        };

        self.apply_snapshot(LogSnapshot {
            data,
            last_included,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct LogSnapshot {
    pub data: Vec<u8>,
    pub last_included: LogEntryMetadata,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub(crate) struct LogEntryMetadata {
    pub term: u64,
    pub index: usize,
}

impl LogSnapshot {
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl From<(u64, usize)> for LogEntryMetadata {
    fn from((term, index): (u64, usize)) -> Self {
        LogEntryMetadata { term, index }
    }
}

impl From<LogEntryMetadata> for (u64, usize) {
    fn from(md: LogEntryMetadata) -> Self {
        (md.term, md.index)
    }
}
