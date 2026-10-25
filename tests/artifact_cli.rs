use llm_cluster::artifact::{ArtifactCache, CancellationToken, Location, Manifest, ModelPin, Role};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn cached_iroh_artifact_is_available_offline_without_opening_an_identity() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"synthetic exported artifact";
    let manifest = Manifest {
        version: 1,
        model: ModelPin {
            revision: "a".repeat(40),
            source_sha256: "b".repeat(64),
            source_bytes: 1024,
            architecture: "qwen35".into(),
            layers: 4,
        },
        begin: 1,
        end: 2,
        role: Role::Middle,
        bytes: bytes.len() as u64,
        artifact_sha256: hex::encode(Sha256::digest(bytes)),
        export_report_sha256: "c".repeat(64),
        location: Location::Iroh {
            peer: llm_cluster::mesh::Peer {
                id: "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a".into(),
                addresses: vec![],
                invite: String::new(),
                relay: None,
            },
        },
    };
    let source = dir.path().join("source");
    tokio::fs::write(&source, bytes).await.unwrap();
    let root = dir.path().join("cache");
    let cache = ArtifactCache::open(&root, manifest.model.clone(), 1024, None)
        .await
        .unwrap();
    let verified = cache
        .import_artifact(&manifest, &source, &CancellationToken::new())
        .await
        .unwrap();
    tokio::fs::remove_file(source).await.unwrap();
    let manifest_path = dir.path().join("manifest.json");
    tokio::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap())
        .await
        .unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_llm-cluster"))
        .args(["artifacts", "fetch", "--manifest"])
        .arg(manifest_path)
        .arg("--cache")
        .arg(root)
        .args(["--quota-bytes", "1024"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "staged");
    assert_eq!(result["inference_ready"], false);
    assert_eq!(result["path"], verified.to_str().unwrap());
}
