use llm_cluster::history_sync::election::{Candidate, elect};
use llm_cluster::history_sync::history::{Entry, NodeId};
use llm_cluster::history_sync::leader::Leader;
use llm_cluster::history_sync::message::SyncMessage;
use llm_cluster::history_sync::node::Node;
use llm_cluster::history_sync::replica::Replica;
use llm_cluster::history_sync::sim::{Cluster, node_id as id};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{Hello, PROTOCOL_VERSION};

fn run_tasks(cluster: &mut Cluster, leader: NodeId, tasks: std::ops::Range<u64>) {
    for t in tasks {
        cluster.complete_task(
            leader,
            &format!("job-{t}"),
            &format!("prompt {t}"),
            &format!("response {t}"),
        );
        cluster.run();
    }
}

fn hello(node_id: NodeId) -> Hello {
    Hello {
        node_id: node_id.to_string(),
        protocol_version: PROTOCOL_VERSION,
        cluster_id: "test".to_owned(),
        last_log_index: 0,
    }
}

#[test]
fn syncs_any_number_of_nodes() {
    for n in [1, 2, 3, 8, 50] {
        let mut cluster = Cluster::new(n);
        run_tasks(&mut cluster, id(0), 0..5);
        assert!(cluster.converged(), "{n} nodes did not converge");
        // Three events per job: started, output, completed.
        assert_eq!(cluster.node(id(0)).history().len(), 15);
    }
}

#[test]
fn sends_deltas_after_first_snapshot() {
    let mut leader = Leader::new(id(0), 1, Default::default(), [id(1)]);
    let mut replica = Replica::new(id(1));

    let out = leader.complete_task("job-0", "a", "b");
    assert!(matches!(out[0].1, SyncMessage::Snapshot { .. }));
    let ack = replica.handle(out[0].1.clone()).unwrap();
    leader.handle(id(1), ack);
    assert!(
        leader.sync().is_empty(),
        "up-to-date peer should get nothing"
    );

    let out = leader.complete_task("job-1", "c", "d");
    match &out[0].1 {
        SyncMessage::Append { start, entries, .. } => {
            assert_eq!(*start, 3);
            assert_eq!(entries.len(), 3);
        }
        other => panic!("expected append, got {other:?}"),
    }
}

#[test]
fn duplicate_appends_are_idempotent() {
    let mut leader = Leader::new(id(0), 1, Default::default(), [id(1)]);
    let mut replica = Replica::new(id(1));
    let snap = leader.sync().remove(0).1;
    leader.handle(id(1), replica.handle(snap).unwrap());

    let append = leader.complete_task("job-0", "a", "b").remove(0).1;
    replica.handle(append.clone());
    replica.handle(append);
    assert_eq!(replica.history(), leader.history());
}

#[test]
fn dropped_messages_are_recovered_by_resync() {
    let mut cluster = Cluster::new(4);
    run_tasks(&mut cluster, id(0), 0..2);
    cluster.drop_next_to(id(2), 2);
    run_tasks(&mut cluster, id(0), 2..4);
    assert!(!cluster.converged());
    assert!(cluster.dropped >= 2);

    cluster.resync(id(0));
    cluster.run();
    assert!(cluster.converged());
}

#[test]
fn gap_triggers_snapshot() {
    let mut leader = Leader::new(id(0), 1, Default::default(), [id(1)]);
    let mut replica = Replica::new(id(1));
    leader.handle(id(1), replica.handle(leader.sync().remove(0).1).unwrap());

    leader.complete_task("lost", "lost", "lost"); // never delivered
    // Pretend the peer acked further than it really did, so the next append has a gap.
    leader.handle(id(1), SyncMessage::Ack { epoch: 1, len: 3 });
    leader.append([Entry::JobStarted {
        request_id: "next".to_owned(),
        prompt: "next".to_owned(),
    }]);
    let append = leader.sync().remove(0).1;
    let reply = replica.handle(append).unwrap();
    assert!(matches!(reply, SyncMessage::Resync { .. }));

    let mut out = leader.handle(id(1), reply);
    assert!(matches!(out[0].1, SyncMessage::Snapshot { .. }));
    replica.handle(out.remove(0).1);
    assert_eq!(replica.history(), leader.history());
}

#[test]
fn failover_continues_session_from_replica_history() {
    let mut cluster = Cluster::new(5);
    run_tasks(&mut cluster, id(0), 0..3);

    cluster.disconnect(id(0));
    let elected = cluster.failover().unwrap();
    assert_eq!(elected.epoch, 2);
    // All replicas hold the same history, so the longest-connected one (node 1) wins.
    assert_eq!(elected.id, id(1));
    cluster.run();
    assert!(cluster.converged());
    // Nine job events plus the EpochAdvanced the new leader records.
    assert_eq!(cluster.node(elected.id).history().len(), 10);
    assert!(matches!(
        cluster.node(elected.id).history().entries().last(),
        Some(Entry::EpochAdvanced { epoch: 2, leader }) if *leader == id(1)
    ));

    run_tasks(&mut cluster, elected.id, 3..5);
    assert!(cluster.converged());
    assert_eq!(cluster.node(elected.id).history().len(), 16);
}

#[test]
fn election_prefers_most_complete_history() {
    let mut cluster = Cluster::new(4);
    run_tasks(&mut cluster, id(0), 0..2);
    // Nodes 1 and 2 miss the last task; node 3 has it.
    cluster.drop_next_to(id(1), 1);
    cluster.drop_next_to(id(2), 1);
    run_tasks(&mut cluster, id(0), 2..3);

    cluster.disconnect(id(0));
    let elected = cluster.failover().unwrap();
    assert_eq!(elected.id, id(3));
    cluster.run();
    assert!(cluster.converged());
    assert_eq!(cluster.node(id(1)).history().len(), 10);
}

#[test]
fn returning_stale_leader_is_rejected_and_resynced() {
    let mut cluster = Cluster::new(3);
    run_tasks(&mut cluster, id(0), 0..2);
    cluster.disconnect(id(0));
    let elected = cluster.failover().unwrap();
    cluster.run();
    run_tasks(&mut cluster, elected.id, 2..3);

    // Old main process comes back unaware and runs a task of its own.
    cluster.reconnect(id(0));
    cluster.complete_task(id(0), "stale", "stale", "stale");
    cluster.run();
    assert!(
        matches!(cluster.node(id(0)), Node::Replica(_)),
        "stale leader should step down"
    );
    assert_eq!(cluster.leader_id(), Some(elected.id));

    // Its divergent entry is discarded once the new leader snapshots it.
    cluster.rejoin(id(0));
    cluster.run();
    assert!(cluster.converged());
    assert!(
        cluster
            .node(id(0))
            .history()
            .entries()
            .iter()
            .all(|e| e.request_id() != Some("stale"))
    );
}

#[test]
fn disconnected_peer_stops_syncing_and_catches_up_on_rejoin() {
    let mut cluster = Cluster::new(3);
    run_tasks(&mut cluster, id(0), 0..1);

    cluster.disconnect(id(2));
    let peers: Vec<NodeId> = cluster.leader_mut(id(0)).peers().collect();
    assert_eq!(peers, vec![id(1)]);
    run_tasks(&mut cluster, id(0), 1..3);
    assert_eq!(cluster.node(id(2)).history().len(), 3);

    cluster.rejoin(id(2));
    cluster.run();
    assert!(cluster.converged());
    assert_eq!(cluster.node(id(2)).history().len(), 9);
}

#[test]
fn leader_peer_list_follows_the_peer_table() {
    let (me, a, b, c) = (id(0), id(1), id(2), id(3));
    let mut table = PeerTable::new();
    table.record_connection_at(me, &hello(me), 1);
    table.record_connection_at(a, &hello(a), 2);
    table.record_connection_at(b, &hello(b), 3);

    let mut node = Node::Leader(Leader::new(me, 1, Default::default(), []));
    node.set_peers(&table);
    let leader = node.as_leader_mut().unwrap();
    let mut peers: Vec<NodeId> = leader.peers().collect();
    peers.sort();
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(peers, expected, "self is never a peer");

    // a has acked everything; it must keep that progress when the list changes.
    let snapshot = leader
        .sync()
        .into_iter()
        .find(|(to, _)| *to == a)
        .unwrap()
        .1;
    let mut replica = Replica::new(a);
    leader.handle(a, replica.handle(snapshot).unwrap());

    table.remove_connection(&b, 3);
    table.record_connection_at(c, &hello(c), 4);
    node.set_peers(&table);
    let leader = node.as_leader_mut().unwrap();
    let pending: Vec<NodeId> = leader.sync().into_iter().map(|(to, _)| to).collect();
    assert_eq!(pending, vec![c], "only the new peer needs a snapshot");
}

#[test]
fn election_is_deterministic() {
    let c = |n, epoch, len| Candidate {
        id: id(n),
        epoch,
        len,
        connected_at_unix_ms: 0,
    };
    let lower = id(4).min(id(2));
    assert_eq!(elect(&[]), None);
    assert_eq!(
        elect(&[c(4, 1, 6), c(2, 1, 6), c(3, 1, 4)]).unwrap().id,
        lower
    );
    assert_eq!(elect(&[c(1, 1, 9), c(5, 2, 2)]).unwrap().id, id(5));
    assert_eq!(elect(&[c(1, 1, 9), c(5, 2, 2)]).unwrap().epoch, 3);
}

#[test]
fn messages_round_trip_through_wire_format() {
    let mut leader = Leader::new(id(7), 3, Default::default(), [id(1), id(2)]);
    for (_, msg) in leader.complete_task("job-0", "hello", "world") {
        assert_eq!(SyncMessage::from_bytes(&msg.to_bytes()).unwrap(), msg);
    }
}

#[test]
fn election_prefers_longest_connected_among_equal_histories() {
    let c = |n, len, connected_at_unix_ms| Candidate {
        id: id(n),
        epoch: 1,
        len,
        connected_at_unix_ms,
    };
    // Equal histories: the earliest connection wins, whatever the IDs.
    assert_eq!(elect(&[c(1, 6, 2_000), c(3, 6, 1_000)]).unwrap().id, id(3));
    assert_eq!(elect(&[c(3, 6, 2_000), c(1, 6, 1_000)]).unwrap().id, id(1));
    // More history still beats a longer connection.
    assert_eq!(elect(&[c(1, 8, 2_000), c(3, 6, 1_000)]).unwrap().id, id(1));
    // Same history and connection time: lowest ID wins.
    let lower = id(4).min(id(2));
    assert_eq!(elect(&[c(4, 6, 1_000), c(2, 6, 1_000)]).unwrap().id, lower);
}
