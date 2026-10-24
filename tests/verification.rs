use llm_cluster::{placement::*, verification::*};

fn fixture() -> (Placement, Vec<PeerProfile>, RunEvidence) {
    let profiles: Vec<_> = ["remote", "local"]
        .into_iter()
        .map(|id| PeerProfile {
            id: id.into(),
            model_hash: "hash".into(),
            runtime_id: "pin".into(),
            context_tokens: 8192,
            authorized: true,
            healthy: true,
            memory_budget_bytes: 1000,
            reserved_bytes: 100,
            decode_us_per_layer: vec![1; 4],
        })
        .collect();
    let placement = Placement {
        model_hash: "hash".into(),
        runtime_id: "pin".into(),
        context_tokens: 8192,
        assignments: profiles
            .iter()
            .enumerate()
            .map(|(i, p)| Assignment {
                peer_id: p.id.clone(),
                layers: i * 2..i * 2 + 2,
                estimated_bytes: 500,
            })
            .collect(),
        predicted_decode_us: 4,
    };
    let evidence = RunEvidence {
        model_hash: "hash".into(),
        runtime_id: "pin".into(),
        context_tokens: 8192,
        devices: profiles
            .iter()
            .map(|p| DeviceEvidence {
                peer_id: p.id.clone(),
                peak_bytes: 1000,
                compute_events: 10,
            })
            .collect(),
    };
    (placement, profiles, evidence)
}

#[test]
fn accepts_two_contributors_at_budget_in_any_evidence_order() {
    let (p, profiles, mut e) = fixture();
    e.devices.reverse();
    assert_eq!(verify_run(&p, &profiles, &e, 2), Ok(()));
}

#[test]
fn rejects_realistic_runtime_failures() {
    let (p, profiles, evidence) = fixture();
    let mut e = evidence.clone();
    e.devices[0].compute_events = 0;
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::NoCompute("remote".into()))
    );
    e = evidence.clone();
    e.devices[0].peak_bytes = 1001;
    assert!(matches!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::BudgetExceeded { .. })
    ));
    e = evidence.clone();
    e.devices[0].peak_bytes = 0;
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::MissingTelemetry("remote".into()))
    );
    e = evidence.clone();
    e.devices.pop();
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::MissingPeer("local".into()))
    );
    e = evidence.clone();
    e.runtime_id = "different".into();
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::IdentityMismatch)
    );
    let mut profiles = profiles;
    profiles[0].authorized = false;
    assert_eq!(
        verify_run(&p, &profiles, &evidence, 2),
        Err(VerificationError::IneligiblePeer("remote".into()))
    );
}

#[test]
fn rejects_duplicate_unplanned_and_insufficient_contributors() {
    let (mut p, profiles, mut e) = fixture();
    e.devices.push(e.devices[0].clone());
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::DuplicatePeer("remote".into()))
    );
    e.devices.pop();
    e.devices[0].peer_id = "other".into();
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::UnexpectedPeer("other".into()))
    );
    p.assignments.pop();
    assert_eq!(
        verify_run(&p, &profiles, &e, 2),
        Err(VerificationError::InvalidPlan)
    );
}
