use std::time::Duration;
use std::{collections::HashMap, time::SystemTime};
use uuid::Uuid;

use crate::domain::*;
use crate::ClientSender;

#[derive(Default)]
struct ReplyChannels {
    commands: HashMap<u64, Vec<ClientSender>>,
    register: Option<ClientSender>,
}

pub(crate) struct ClientManager {
    expire_period: Duration,
    sessions: HashMap<Uuid, ClientSession>,
    channels: HashMap<Uuid, ReplyChannels>,
}

impl ClientManager {
    pub(crate) fn new(expire_period: Duration) -> ClientManager {
        ClientManager {
            expire_period,
            sessions: HashMap::new(),
            channels: HashMap::new(),
        }
    }

    pub(crate) fn is_expired(&mut self, client_id: Uuid, now: SystemTime) -> bool {
        if let Some(session) = self.sessions.get(&client_id) {
            if now.duration_since(session.last_activity).unwrap() < self.expire_period {
                false
            } else {
                self.sessions.remove(&client_id);
                true
            }
        } else {
            true
        }
    }

    pub(crate) fn command_status(&self, client_id: Uuid, seq_num: u64) -> CommandStatus {
        if self.channels.get(&client_id).and_then(|c| c.commands.get(&seq_num)).is_some() {
            CommandStatus::InProgress
        } else {
            if let Some(session) = self.sessions.get(&client_id) {
                if let Some(output) = session.responses.get(&seq_num) {
                    CommandStatus::Finished(output.clone())
                } else if seq_num >= session.lowest_sequence_num_without_response {
                    CommandStatus::New
                } else {
                    CommandStatus::Expired
                }
            } else {
                CommandStatus::Expired
            }
        }
    }

    pub(crate) fn prepare_register_client(&mut self, client_id: Uuid, channel: ClientSender) {
        self.channels.insert(client_id, ReplyChannels {
            commands: HashMap::new(),
            register: Some(channel),
        });
    }

    pub(crate) fn prepare_command(&mut self, client_id: Uuid, seq_num: u64, channel: ClientSender) {
        let channels = self.channels.entry(client_id).or_default();
        channels.commands.entry(seq_num).or_default().push(channel);
    }

    pub(crate) async fn register_client(&mut self, client_id: Uuid, timestamp: SystemTime) {
        let session = ClientSession {
            last_activity: timestamp,
            responses: HashMap::new(),
            lowest_sequence_num_without_response: 0,
        };
        self.sessions.insert(client_id, session);

        let channels = self.channels.entry(client_id).or_default();
        if let Some(channel) = channels.register.take() {
            let response = RegisterClientResponseArgs {
                content: RegisterClientResponseContent::ClientRegistered { client_id },
            };
            let _ = channel.send(response.into()).await;
        }
    }

    pub(crate) async fn command(&mut self, client_id: Uuid, seq_num: u64, output: Vec<u8>) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            session.responses.insert(seq_num, output.clone());
        }

        let channels = self.channels.entry(client_id).or_default();
        if let Some(channels) = channels.commands.remove(&seq_num) {
            let content = CommandResponseContent::CommandApplied { output };
            let response = CommandResponseArgs {
                client_id,
                sequence_num: seq_num,
                content,
            };
            for channel in channels {
                let _ = channel.send(response.clone().into()).await;
            }
        }
    }

    pub(crate) async fn expire(&mut self, client_id: Uuid, seq_num: u64) {
        self.sessions.remove(&client_id);

        let channels = self.channels.entry(client_id).or_default();
        if let Some(channels) = channels.commands.remove(&seq_num) {
            let content = CommandResponseContent::SessionExpired;
            let response = CommandResponseArgs {
                client_id,
                sequence_num: seq_num,
                content,
            };
            for channel in channels {
                let _ = channel.send(response.clone().into()).await;
            }
        }
    }

    pub(crate) fn prolong_session(&mut self, client_id: Uuid, now: SystemTime) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            session.last_activity = now;
        }
    }

    pub(crate) fn prune_outputs(&mut self, client_id: Uuid, lowest_seq_num: u64) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            if session.lowest_sequence_num_without_response < lowest_seq_num {
                println!("pruning responses for {:?} lower than {:?}", client_id, lowest_seq_num);
                println!(" before pruning: {:?}", session.responses);
                session.responses.retain(|&seq_num, _| seq_num >= lowest_seq_num);
                session.lowest_sequence_num_without_response = lowest_seq_num;
                println!(" after pruning: {:?}", session.responses);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CommandStatus {
    New,
    InProgress,
    Finished(Vec<u8>),
    Expired,
}