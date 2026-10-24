use history_sync::sim::Cluster;
use history_sync::{Candidate, Entry, Leader, Node, Replica, Role, SyncMessage, elect};

fn run_tasks(cluster: &mut Cluster, leader: u64, tasks: std::ops::Range<u64>) {
    for t in tasks {
        cluster.complete_task(leader, t, &format!("prompt {t}"), &format!("response {t}"));
        cluster.run();
    }
}

#[test]
fn syncs_any_number_of_nodes() {
    for n in [1, 2, 3, 8, 50] {
        let mut cluster = Cluster::new(n);
        run_tasks(&mut cluster, 0, 0..5);
        assert!(cluster.converged(), "{n} nodes did not converge");
        assert_eq!(cluster.node(0).history().len(), 10);
    }
}

#[test]
fn sends_deltas_after_first_snapshot() {
    let mut leader = Leader::new(0, 1, Default::default(), [1]);
    let mut replica = Replica::new(1);

    let out = leader.complete_task(0, "a", "b");
    assert!(matches!(out[0].1, SyncMessage::Snapshot { .. }));
    let ack = replica.handle(out[0].1.clone()).unwrap();
    leader.handle(1, ack);
    assert!(leader.sync().is_empty(), "up-to-date peer should get nothing");

    let out = leader.complete_task(1, "c", "d");
    match &out[0].1 {
        SyncMessage::Append { start, entries, .. } => {
            assert_eq!(*start, 2);
            assert_eq!(entries.len(), 2);
        }
        other => panic!("expected append, got {other:?}"),
    }
}

#[test]
fn duplicate_appends_are_idempotent() {
    let mut leader = Leader::new(0, 1, Default::default(), [1]);
    let mut replica = Replica::new(1);
    let snap = leader.sync().remove(0).1;
    leader.handle(1, replica.handle(snap).unwrap());

    let append = leader.complete_task(0, "a", "b").remove(0).1;
    replica.handle(append.clone());
    replica.handle(append);
    assert_eq!(replica.history(), leader.history());
}

#[test]
fn dropped_messages_are_recovered_by_resync() {
    let mut cluster = Cluster::new(4);
    run_tasks(&mut cluster, 0, 0..2);
    cluster.drop_next_to(2, 2);
    run_tasks(&mut cluster, 0, 2..4);
    assert!(!cluster.converged());
    assert!(cluster.dropped >= 2);

    cluster.resync(0);
    cluster.run();
    assert!(cluster.converged());
}

#[test]
fn gap_triggers_snapshot() {
    let mut leader = Leader::new(0, 1, Default::default(), [1]);
    let mut replica = Replica::new(1);
    leader.handle(1, replica.handle(leader.sync().remove(0).1).unwrap());

    leader.complete_task(0, "lost", "lost"); // never delivered
    // Pretend the peer acked further than it really did, so the next append has a gap.
    leader.handle(1, SyncMessage::Ack { epoch: 1, len: 2 });
    leader.append([Entry::new(1, Role::User, "next")]);
    let append = leader.sync().remove(0).1;
    let reply = replica.handle(append).unwrap();
    assert!(matches!(reply, SyncMessage::Resync { .. }));

    let mut out = leader.handle(1, reply);
    assert!(matches!(out[0].1, SyncMessage::Snapshot { .. }));
    replica.handle(out.remove(0).1);
    assert_eq!(replica.history(), leader.history());
}

#[test]
fn failover_continues_session_from_replica_history() {
    let mut cluster = Cluster::new(5);
    run_tasks(&mut cluster, 0, 0..3);

    cluster.set_offline(0, true);
    let elected = cluster.failover().unwrap();
    assert_eq!(elected.epoch, 2);
    assert_ne!(elected.id, 0);
    cluster.run();
    assert!(cluster.converged());
    assert_eq!(cluster.node(elected.id).history().len(), 6);

    run_tasks(&mut cluster, elected.id, 3..5);
    assert!(cluster.converged());
    assert_eq!(cluster.node(elected.id).history().len(), 10);
}

#[test]
fn election_prefers_most_complete_history() {
    let mut cluster = Cluster::new(4);
    run_tasks(&mut cluster, 0, 0..2);
    // Nodes 1 and 2 miss the last task; node 3 has it.
    cluster.drop_next_to(1, 1);
    cluster.drop_next_to(2, 1);
    run_tasks(&mut cluster, 0, 2..3);

    cluster.set_offline(0, true);
    let elected = cluster.failover().unwrap();
    assert_eq!(elected.id, 3);
    cluster.run();
    assert!(cluster.converged());
    assert_eq!(cluster.node(1).history().len(), 6);
}

#[test]
fn returning_stale_leader_is_rejected_and_resynced() {
    let mut cluster = Cluster::new(3);
    run_tasks(&mut cluster, 0, 0..2);
    cluster.set_offline(0, true);
    let elected = cluster.failover().unwrap();
    cluster.run();
    run_tasks(&mut cluster, elected.id, 2..3);

    // Old main process comes back unaware and runs a task of its own.
    cluster.set_offline(0, false);
    cluster.complete_task(0, 99, "stale", "stale");
    cluster.run();
    assert!(matches!(cluster.node(0), Node::Replica(_)), "stale leader should step down");
    assert_eq!(cluster.leader_id(), Some(elected.id));

    // Its divergent entry is discarded once the new leader snapshots it.
    cluster.rejoin(0);
    cluster.run();
    assert!(cluster.converged());
    assert!(cluster.node(0).history().entries().iter().all(|e| e.task_id != 99));
}

#[test]
fn election_is_deterministic() {
    let c = |id, epoch, len| Candidate { id, epoch, len };
    assert_eq!(elect(&[]), None);
    assert_eq!(elect(&[c(4, 1, 6), c(2, 1, 6), c(3, 1, 4)]).unwrap().id, 2);
    assert_eq!(elect(&[c(1, 1, 9), c(5, 2, 2)]).unwrap().id, 5);
    assert_eq!(elect(&[c(1, 1, 9), c(5, 2, 2)]).unwrap().epoch, 3);
}

#[test]
fn messages_round_trip_through_wire_format() {
    let mut leader = Leader::new(7, 3, Default::default(), [1, 2]);
    for (_, msg) in leader.complete_task(0, "hello", "world") {
        assert_eq!(SyncMessage::from_bytes(&msg.to_bytes()).unwrap(), msg);
    }
}
