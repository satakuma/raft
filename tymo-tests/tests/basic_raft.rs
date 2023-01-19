use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(1000)]
async fn append_entries_limits_chunk() {
    // chunk size is set to 3
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let spy_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id, spy_id];
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
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;

    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    tokio::time::sleep(Duration::from_millis(150)).await;

    let (result_sender, result_receiver) = unbounded();
    let client_id = register_client(&leader, &result_sender, &result_receiver).await;

    const BATCHES: usize = 4;

    for i in 0..BATCH_SIZE * BATCHES {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![(i + 1) as _],
                    client_id,
                    sequence_num: i as _,
                    lowest_sequence_num_without_response: i as _,
                },
            })
            .await;
        assert_eq!(
            result_receiver.recv().await.unwrap(),
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: i as _,
                content: CommandResponseContent::CommandApplied {
                    output: vec![(i + 1) as _],
                },
            })
        );
    }

    let (spy_sender, spy_receiver) = unbounded();
    sender
        .insert(
            spy_id,
            Box::new(RaftSpy::new(&mut system, None, spy_sender).await),
        )
        .await;
    for i in 0..BATCHES {
        let msg = spy_receiver.recv().await.unwrap();
        assert!(matches!(
            msg,
            RaftMessage {
                header: RaftMessageHeader {
                    source,
                    term: 1,
                },
                content: RaftMessageContent::AppendEntries(_)
            } if source == leader_id
        ));
        if let RaftMessageContent::AppendEntries(AppendEntriesArgs {
            entries,
            prev_log_index,
            ..
        }) = msg.content
        {
            assert_eq!(entries.len(), BATCH_SIZE);
            assert_eq!(prev_log_index, i * BATCH_SIZE);
            leader
                .send(RaftMessage {
                    header: RaftMessageHeader {
                        source: spy_id,
                        term: 1,
                    },
                    content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                        success: true,
                        last_verified_log_index: prev_log_index + entries.len(),
                    }),
                })
                .await;
        }
    }

    system.shutdown().await;
}
