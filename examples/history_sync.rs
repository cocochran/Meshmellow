//! Demo: `cargo run --example history_sync -- <nodes>` simulates history sync, packet loss, and failover.

use llm_cluster::history_sync::sim::Cluster;

fn main() {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
        .max(2);
    let mut cluster = Cluster::new(n);
    let (first, last) = (cluster.id(0), cluster.id(n - 1));
    println!("Mesh of {n} nodes; node 0 is the main process (epoch 1)\n");

    for t in 0..3 {
        if t == 1 {
            cluster.drop_next_to(last, 1);
            println!("  (dropping the next message to node {})", n - 1);
        }
        cluster.complete_task(
            first,
            &format!("job-{t}"),
            &format!("prompt {t}"),
            &format!("response {t}"),
        );
        cluster.run();
        report(&cluster, &format!("after task {t}"));
    }

    cluster.resync(first);
    cluster.run();
    report(&cluster, "after retransmit");

    println!("\nNode 0 drops out.");
    cluster.disconnect(first);
    let elected = cluster.failover().expect("someone survives");
    cluster.run();
    println!(
        "Node {} ({}) elected as main process (epoch {})",
        cluster.index(elected.id),
        elected.id.fmt_short(),
        elected.epoch
    );
    report(&cluster, "after failover");

    cluster.complete_task(elected.id, "job-3", "prompt 3", "response 3");
    cluster.run();
    report(&cluster, "after task 3");

    println!("\nNode 0 returns with a stale view and tries to run a task.");
    cluster.reconnect(first);
    cluster.complete_task(first, "stale", "stale", "stale");
    cluster.run();
    cluster.rejoin(first);
    cluster.run();
    report(&cluster, "after node 0 rejoins");

    println!(
        "\n{} messages delivered, {} dropped",
        cluster.delivered, cluster.dropped
    );
}

fn report(cluster: &Cluster, label: &str) {
    let lens: Vec<String> = cluster
        .node_ids()
        .map(|id| format!("{}:{}", cluster.index(id), cluster.node(id).history().len()))
        .collect();
    let status = if cluster.converged() {
        "converged"
    } else {
        "NOT converged"
    };
    println!(
        "{label:>22}: entries per node [{}] -> {status}",
        lens.join(" ")
    );
}
