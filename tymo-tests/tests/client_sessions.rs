use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(1000)]
async fn client_session_survives_new_election_another_server() {
    let mut leader_system = System::new().await;
    let mut follower_system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id];
    let leader_storage = SharedRamStorage::default();
    let (apply_sender, apply_receiver) = unbounded();
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();
    let leader = Raft::new(
        &mut leader_system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(leader_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut follower_system,
        make_config(follower_id, Duration::from_millis(200), processes.clone()),
        first_log_entry_timestamp,
        Box::new(SpyMachine { apply_sender }),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client_id = register_client(&leader, &result_sender, &result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            },
        })
    );

    // restart leader, he will turn into a follower
    drop(leader);
    leader_system.shutdown().await;
    drop(leader_system);

    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut leader_system = System::new().await;
    let leader = Raft::new(
        &mut leader_system,
        make_config(leader_id, Duration::from_millis(400), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(leader_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader {
                leader_hint: Some(follower_id),
            },
        })
    );

    follower
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            },
        })
    );

    assert_eq!(apply_receiver.recv().await.unwrap(), vec![1, 2, 3, 4]);
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![5, 6, 7, 8]);
    assert!(apply_receiver.is_empty());

    follower_system.shutdown().await;
    leader_system.shutdown().await;
}

#[tokio::test]
#[timeout(1000)]
async fn client_session_survives_new_election_same_server() {
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id];
    let leader_storage = SharedRamStorage::default();
    let follower_storage = SharedRamStorage::default();
    let (apply_sender, apply_receiver) = unbounded();
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();
    let leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(leader_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes.clone()),
        first_log_entry_timestamp,
        Box::new(SpyMachine {
            apply_sender: apply_sender.clone(),
        }),
        Box::new(follower_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client_id = register_client(&leader, &result_sender, &result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            },
        })
    );

    // restart system
    drop(leader);
    drop(follower);
    system.shutdown().await;
    drop(system);

    let mut system = System::new().await;
    let leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(leader_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes.clone()),
        first_log_entry_timestamp,
        Box::new(SpyMachine { apply_sender }),
        Box::new(follower_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    follower
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader {
                leader_hint: Some(leader_id),
            },
        })
    );

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            },
        })
    );

    assert_eq!(apply_receiver.recv().await.unwrap(), vec![1, 2, 3, 4]);
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![5, 6, 7, 8]);
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}
