use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(500)]
async fn one_server_left_error() {
    // given
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let processes = vec![leader_id];
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();
    let leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;

    let (result_sender, result_receiver) = unbounded();
    tokio::time::sleep(Duration::from_millis(150)).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RemoveServer {
                old_server: leader_id,
            },
        })
        .await;
    let remove_result = result_receiver.recv().await.unwrap();

    assert_eq!(
        remove_result,
        ClientRequestResponse::RemoveServerResponse(RemoveServerResponseArgs {
            old_server: leader_id,
            content: RemoveServerResponseContent::OneServerLeft,
        })
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(800)]
async fn leader_steps_down() {
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id];
    let leader_storage = SharedRamStorage::default();
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
        make_config(follower_id, Duration::from_millis(150), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RemoveServer {
                old_server: leader_id,
            },
        })
        .await;

    let remove_result = result_receiver.recv().await.unwrap();
    assert_eq!(
        remove_result,
        ClientRequestResponse::RemoveServerResponse(RemoveServerResponseArgs {
            old_server: leader_id,
            content: RemoveServerResponseContent::ServerRemoved,
        })
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    follower
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;

    let register_result = result_receiver.recv().await.unwrap();
    assert!(matches!(
        register_result,
        ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
            content: RegisterClientResponseContent::ClientRegistered { .. },
        })
    ));

    system.shutdown().await;
}
