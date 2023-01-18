use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(500)]
async fn install_snapshot_with_failing_follower() {
    // given
    let mut system = System::new().await;
    let follower_id = Uuid::new_v4();
    let spy_id = Uuid::new_v4();
    let processes = vec![follower_id, spy_id];
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();

    let follower_storage = SharedRamStorage::default();
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(500), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(follower_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 3,
            },
            content: RaftMessageContent::InstallSnapshot(InstallSnapshotArgs {
                offset: 0,
                data: vec![0, 1, 2, 3],
                client_sessions: None,
                last_config: Some(processes.clone().into_iter().collect()),
                last_included_index: 9,
                last_included_term: 2,
                done: false,
            }),
        })
        .await;

    let (spy_tx, spy_rx) = unbounded();
    let spy = {
        let inner_raft = Raft::new(
            &mut system,
            make_config(spy_id, Duration::from_millis(800), processes.clone()),
            first_log_entry_timestamp,
            Box::new(LogMachine::default()),
            Box::new(RamStorage::default()),
            Box::new(sender.clone()),
        )
        .await;
        RaftSpy::new(&mut system, Some(inner_raft), spy_tx).await
    };
    sender.insert(spy_id, Box::new(spy.clone())).await;

    let msg = spy_rx.recv().await.unwrap();
    assert_eq!(
        msg,
        RaftMessage {
            header: RaftMessageHeader {
                source: follower_id,
                term: 3,
            },
            content: RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs {
                offset: 0,
                last_included_index: 9,
            })
        }
    );

    // follower crash
    drop(follower);

    let (init_sender, init_receiver) = unbounded();
    let follower_machine = InitDetectorMachine { init_sender };
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(500), processes),
        first_log_entry_timestamp,
        Box::new(follower_machine),
        Box::new(follower_storage.clone()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    // send next chunk
    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 3,
            },
            content: RaftMessageContent::InstallSnapshot(InstallSnapshotArgs {
                offset: 4,
                data: vec![4, 5, 6, 7],
                client_sessions: None,
                last_config: None,
                last_included_index: 9,
                last_included_term: 2,
                done: true,
            }),
        })
        .await;

    let msg = spy_rx.recv().await.unwrap();
    assert_eq!(
        msg,
        RaftMessage {
            header: RaftMessageHeader {
                source: follower_id,
                term: 3,
            },
            content: RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs {
                offset: 4,
                last_included_index: 9,
            })
        }
    );

    assert_eq!(
        init_receiver.recv().await.unwrap(),
        vec![0, 1, 2, 3, 4, 5, 6, 7],
    );

    system.shutdown().await;
}
