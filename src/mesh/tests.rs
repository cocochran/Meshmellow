use super::*;

#[tokio::test]
async fn oversized_membership_file_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("membership.json"), vec![b'x'; MAX_WIRE + 1]).unwrap();

    assert!(Node::open_local(dir.path()).await.is_err());
}

async fn peer(node: &Node) -> Peer {
    let snapshot = node.snapshot().await.unwrap();
    Peer {
        id: node.id(),
        relay: None,
        addresses: vec![format!("127.0.0.1:{}", snapshot["port"]).parse().unwrap()],
        invite: snapshot["invite"]["id"].as_str().unwrap_or("").to_owned(),
    }
}

#[tokio::test]
async fn three_real_peers_create_join_any_trusted_approval_and_restart() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = Node::open_local(dirs[0].path()).await.unwrap();
    let b = Node::open_local(dirs[1].path()).await.unwrap();
    let c = Node::open_local(dirs[2].path()).await.unwrap();
    a.create("Our mesh", "iPad").await.unwrap();
    let ticket = a.invite().await.unwrap();
    b.discover(vec![peer(&a).await]).await.unwrap();
    b.command(json!({"op": "join", "invitation": ticket, "device_name": "Android"}))
        .await
        .unwrap();
    assert!(b.snapshot().await.unwrap()["mesh"].is_null());
    let request = a.snapshot().await.unwrap()["pending"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    a.decide(&a.id(), &request, true).await.unwrap();
    b.refresh().await.unwrap();
    assert_eq!(
        b.snapshot().await.unwrap()["members"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let next_ticket = a.invite().await.unwrap();
    c.discover(vec![peer(&a).await]).await.unwrap();
    c.join_invitation(&next_ticket, "Phone").await.unwrap();
    b.discover(vec![peer(&a).await]).await.unwrap();
    b.refresh().await.unwrap();
    let request = b.snapshot().await.unwrap()["pending"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.decide(&a.id(), &request, true).await.unwrap();
    b.decide(&a.id(), &request, true).await.unwrap();
    assert!(b.decide(&a.id(), &request, false).await.is_err());
    // An approved device returning after the invitation deadline still recovers
    // its committed membership; invitation cleanup must not discard approval.
    c.record.lock().await.joining.as_mut().unwrap().expires = now() - 1;
    a.record
        .lock()
        .await
        .requests
        .iter_mut()
        .find(|p| p.id == request)
        .unwrap()
        .expires = now() - 1;
    a.invite().await.unwrap();
    c.refresh().await.unwrap();
    assert_eq!(
        c.snapshot().await.unwrap()["members"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let id = c.id();
    c.close().await;
    drop(c);
    let restored = Node::open_local(dirs[2].path()).await.unwrap();
    assert_eq!(restored.id(), id);
    assert_eq!(
        restored.snapshot().await.unwrap()["mesh"]["name"],
        "Our mesh"
    );
    assert!(restored.create("Other mesh", "Phone").await.is_err());
    a.close().await;
    b.close().await;
    restored.close().await;
}

#[tokio::test]
async fn strangers_cannot_approve_poll_others_or_supply_another_mesh() {
    let ad = tempfile::tempdir().unwrap();
    let bd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let a = Node::open_local(ad.path()).await.unwrap();
    let b = Node::open_local(bd.path()).await.unwrap();
    let c = Node::open_local(cd.path()).await.unwrap();
    a.create("A", "A").await.unwrap();
    a.invite().await.unwrap();
    b.join(peer(&a).await, "B").await.unwrap();
    let request = a.snapshot().await.unwrap()["pending"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let ledger = a.record.lock().await.ledger.clone().unwrap();
    assert!(
        c.rpc(
            &peer(&a).await,
            Wire::Decide {
                ledger,
                request: request.clone(),
                approve: true
            }
        )
        .await
        .is_err()
    );
    assert!(
        c.rpc(
            &peer(&a).await,
            Wire::Poll {
                request: request.clone()
            }
        )
        .await
        .is_err()
    );
    assert!(
        c.rpc(
            &peer(&a).await,
            Wire::Cancel {
                request: request.clone()
            }
        )
        .await
        .is_err()
    );
    c.create("Other", "C").await.unwrap();
    let foreign = c.record.lock().await.ledger.clone().unwrap();
    assert!(
        c.rpc(
            &peer(&a).await,
            Wire::Decide {
                ledger: foreign,
                request: request.clone(),
                approve: true
            }
        )
        .await
        .is_err()
    );
    assert_eq!(
        a.snapshot().await.unwrap()["pending"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    a.decide(&a.id(), &request, false).await.unwrap();
    b.refresh().await.unwrap();
    assert!(b.snapshot().await.unwrap()["joining"].is_null());
    assert!(b.snapshot().await.unwrap()["mesh"].is_null());
    a.close().await;
    b.close().await;
    c.close().await;
}

#[tokio::test]
async fn invitations_expire_are_single_use_and_cancellation_withdraws_request() {
    let ad = tempfile::tempdir().unwrap();
    let bd = tempfile::tempdir().unwrap();
    let cd = tempfile::tempdir().unwrap();
    let a = Node::open_local(ad.path()).await.unwrap();
    let b = Node::open_local(bd.path()).await.unwrap();
    let c = Node::open_local(cd.path()).await.unwrap();
    a.create("A", "A").await.unwrap();
    a.invite().await.unwrap();
    let old = peer(&a).await;
    a.record.lock().await.invite.as_mut().unwrap().expires = now() - 1;
    assert!(b.join(old, "B").await.is_err());
    a.invite().await.unwrap();
    let invitation = peer(&a).await;
    b.join(invitation.clone(), "B").await.unwrap();
    assert!(c.join(invitation.clone(), "C").await.is_err());
    assert!(a.invite().await.is_err());
    b.command(json!({"op":"cancel_join"})).await.unwrap();
    assert!(
        a.snapshot().await.unwrap()["pending"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(b.snapshot().await.unwrap()["joining"].is_null());
    assert!(c.join(invitation, "C").await.is_err());
    a.close().await;
    b.close().await;
    c.close().await;
}

#[test]
fn membership_rejects_forgery_cycles_tampering_and_cross_mesh_grants() {
    let a = SecretKey::generate();
    let b = SecretKey::generate();
    let c = SecretKey::generate();
    let mut ledger = Ledger::create(&a, "Mesh", "A").unwrap();
    ledger
        .admit(
            &a,
            &b.public().to_string(),
            "B",
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
    ledger
        .admit(
            &b,
            &c.public().to_string(),
            "C",
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
    assert_eq!(ledger.members().unwrap().len(), 3);
    let mut forged = ledger.clone();
    forged.grants[1].name = "Changed".into();
    assert!(forged.members().is_err());
    let mut cycle = ledger.clone();
    cycle.grants[0].issuer = c.public().to_string();
    assert!(cycle.members().is_err());
    let mut wrong_mesh = Ledger::create(&a, "Another", "A").unwrap();
    wrong_mesh.grants = ledger.grants.clone();
    assert!(wrong_mesh.members().is_err());
    let mut tampered = ledger.clone();
    tampered.genesis.name = "Impersonated".into();
    assert!(tampered.members().is_err());
    let stranger = SecretKey::generate();
    assert!(
        ledger
            .admit(
                &stranger,
                &stranger.public().to_string(),
                "Stranger",
                &uuid::Uuid::new_v4().to_string()
            )
            .is_err()
    );
}

/// Requires public Iroh relay/DNS access; run explicitly, never in offline CI.
#[tokio::test]
#[ignore = "uses public Iroh relays and DNS"]
async fn internet_invitations_without_ip_transport_and_remote_trust() {
    tokio::time::timeout(Duration::from_secs(150), async {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let a = Node::open_with(dirs[0].path(), true).await.unwrap();
        let b = Node::open_with(dirs[1].path(), true).await.unwrap();
        let c = Node::open_with(dirs[2].path(), true).await.unwrap();
        for node in [&a, &b, &c] {
            node.endpoint.online().await;
            assert!(node.endpoint.bound_sockets().is_empty());
        }
        a.create("Internet mesh", "Founder").await.unwrap();
        let ticket = a.invite().await.unwrap();
        b.join_invitation(&ticket, "Remote member").await.unwrap();
        let request = a.snapshot().await.unwrap()["pending"][0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        a.decide(&a.id(), &request, true).await.unwrap();
        b.refresh().await.unwrap();
        let ticket = a.invite().await.unwrap();
        c.join_invitation(&ticket, "Third device").await.unwrap();
        // B knows A only from its persisted membership; no discovery records.
        for _ in 0..12 {
            b.refresh().await.unwrap();
            if !b.snapshot().await.unwrap()["pending"]
                .as_array()
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let request = b.snapshot().await.unwrap()["pending"][0]["id"]
            .as_str()
            .expect("remote trusted member sees request by node ID")
            .to_owned();
        b.decide(&a.id(), &request, true).await.unwrap();
        c.refresh().await.unwrap();
        assert_eq!(
            c.snapshot().await.unwrap()["members"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        let original = c.id();
        c.close().await;
        let c = Node::open_with(dirs[2].path(), true).await.unwrap();
        assert_eq!(c.id(), original);
        assert_eq!(
            c.snapshot().await.unwrap()["members"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .expect("relay pairing completed within 150 seconds");
}

#[tokio::test]
#[ignore = "uses public Iroh relays and DNS"]
async fn core_ticket_join_establishes_trusted_heartbeats_without_lookup() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let ad = tempfile::tempdir().unwrap();
        let bd = tempfile::tempdir().unwrap();
        let a = Node::open_with(ad.path(), true).await.unwrap();
        let b = Node::open_with(bd.path(), true).await.unwrap();
        a.endpoint.online().await;
        b.endpoint.online().await;
        a.create("Core mesh", "Founder").await.unwrap();
        a.invite().await.unwrap();
        let state = a.snapshot().await.unwrap();
        assert!(state["invite"].get("code").is_none());
        let ticket = state["invite"]["ticket"].as_str().unwrap();
        let locator = invitation::Invitation::from_ticket(ticket).unwrap();
        // Knowing the cluster ID is not admission to the core control plane.
        let hello =
            crate::net::network::local_hello(&b.endpoint, state["mesh"]["id"].as_str().unwrap(), 0);
        assert!(
            crate::net::network::dial_peer(
                &b.endpoint,
                locator.peer().addr().unwrap(),
                &hello,
                &b.control_peers
            )
            .await
            .is_err()
        );
        b.join_invitation(ticket, "Joining device").await.unwrap();
        assert!(
            b.snapshot().await.unwrap()["connected_peers"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let request = a.snapshot().await.unwrap()["pending"][0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        a.decide(&a.id(), &request, true).await.unwrap();
        b.refresh().await.unwrap();
        async fn wait_for_heartbeats(a: &Node, b: &Node) {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let a_live = a
                        .control_peers
                        .lock()
                        .unwrap()
                        .get(&b.endpoint.id())
                        .is_some_and(|p| p.last_heartbeat_sequence.is_some());
                    let b_live = b
                        .control_peers
                        .lock()
                        .unwrap()
                        .get(&a.endpoint.id())
                        .is_some_and(|p| p.last_heartbeat_sequence.is_some());
                    if a_live && b_live {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("signed members establish bidirectional core heartbeats");
        }
        wait_for_heartbeats(&a, &b).await;
        let id = b.id();
        b.close().await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while a
                .control_peers
                .lock()
                .unwrap()
                .get(&id.parse().unwrap())
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("closed member leaves live peer table");
        let b = Node::open_with(bd.path(), true).await.unwrap();
        assert_eq!(b.id(), id);
        b.endpoint.online().await;
        wait_for_heartbeats(&a, &b).await;
        assert_eq!(
            b.snapshot().await.unwrap()["members"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        a.close().await;
        b.close().await;
    })
    .await
    .expect("core mesh integration completed");
}

#[test]
fn self_contained_invitation_rejects_tampering_and_oversize_input() {
    let key = SecretKey::generate();
    let mut locator =
        invitation::Invitation::signed(&key, uuid::Uuid::new_v4().to_string(), now() + 300, None);
    let ticket = locator.ticket();
    assert_eq!(
        invitation::Invitation::from_ticket(&ticket).unwrap().issuer,
        key.public().to_string()
    );
    for expires in [now() - 1, now() + INVITE_SECONDS + 60] {
        let invalid =
            invitation::Invitation::signed(&key, uuid::Uuid::new_v4().to_string(), expires, None);
        assert!(invitation::Invitation::from_ticket(&invalid.ticket()).is_err());
    }
    assert!(invitation::Invitation::from_ticket("😀 😎 🥳 🤖 👻 👽 🐶 🐱").is_err());
    assert!(
        invitation::Invitation::from_ticket(&ticket.replace("layercake://", "https://")).is_err()
    );
    locator.invite = uuid::Uuid::new_v4().to_string();
    assert!(invitation::Invitation::from_ticket(&locator.ticket()).is_err());
    assert!(invitation::Invitation::from_ticket(&"x".repeat(4097)).is_err());
}

#[tokio::test]
async fn leaving_disconnects_persists_and_rejoining_requires_approval() {
    let ad = tempfile::tempdir().unwrap();
    let bd = tempfile::tempdir().unwrap();
    let a = Node::open_local(ad.path()).await.unwrap();
    let b = Node::open_local(bd.path()).await.unwrap();
    a.create("Original mesh", "A").await.unwrap();
    a.discover(vec![peer(&b).await]).await.unwrap();
    a.invite().await.unwrap();
    b.join(peer(&a).await, "B").await.unwrap();
    b.discover(vec![peer(&a).await]).await.unwrap();
    let request = a.snapshot().await.unwrap()["pending"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    a.decide(&a.id(), &request, true).await.unwrap();
    b.refresh().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while a.snapshot().await.unwrap()["connected_peers"]
            .as_array()
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let original_id = b.id();
    let left = b.command(json!({"op":"leave"})).await.unwrap();
    assert!(left["mesh"].is_null());
    assert!(left["invite"].is_null());
    assert!(left["members"].as_array().unwrap().is_empty());
    tokio::time::timeout(Duration::from_secs(10), async {
        while !a.snapshot().await.unwrap()["connected_peers"]
            .as_array()
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    b.close().await;
    let b = Node::open_local(bd.path()).await.unwrap();
    assert_eq!(b.id(), original_id);
    assert!(b.snapshot().await.unwrap()["mesh"].is_null());
    a.invite().await.unwrap();
    b.join(peer(&a).await, "B").await.unwrap();
    b.refresh().await.unwrap();
    assert!(b.snapshot().await.unwrap()["mesh"].is_null());
    let request = a.snapshot().await.unwrap()["pending"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    a.decide(&a.id(), &request, true).await.unwrap();
    b.refresh().await.unwrap();
    assert_eq!(
        b.snapshot().await.unwrap()["members"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    b.leave().await.unwrap();
    b.create("New mesh", "B").await.unwrap();
    assert_eq!(b.snapshot().await.unwrap()["mesh"]["name"], "New mesh");
    a.close().await;
    b.close().await;
}

/// Membership maintenance prunes per-member control tasks. Model verification
/// must survive that pruning and report its outcome rather than stay "verifying".
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compute_verification_outlives_membership_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::open_local(dir.path()).await.unwrap();
    node.create("Our mesh", "Laptop").await.unwrap();
    node.close().await;
    // A FIFO holds the hash open until written, so maintenance runs meanwhile.
    let model = dir.path().join("model.gguf");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&model)
            .status()
            .unwrap()
            .success()
    );
    let config = json!({"runner": "/nonexistent", "model": model, "model_sha256": "0".repeat(64),
        "layers": 16, "context": 128, "gpu_layers": 0, "budget_bytes": 1});
    std::fs::write(dir.path().join("compute.json"), config.to_string()).unwrap();
    let node = Node::open_local(dir.path()).await.unwrap();
    assert_eq!(node.compute_status().await.unwrap().phase, "verifying");
    tokio::time::sleep(Duration::from_secs(3)).await;
    std::fs::write(&model, b"not the pinned model").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let status = loop {
        let status = node.compute_status().await.unwrap();
        if status.phase != "verifying" || tokio::time::Instant::now() > deadline {
            break status;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(status.phase, "failed", "{status:?}");
    node.close().await;
}
