//! Local JSON-lines control interface. Network enrollment uses authenticated Iroh,
//! never this stdin channel. One private state directory per running process.
use super::Node;
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    io::{BufRead, Write},
    time::Duration,
};

pub async fn run(directory: std::path::PathBuf) -> Result<()> {
    let node = Node::open(directory).await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    std::thread::spawn(move || {
        // Bounded reader; a local caller cannot cause unbounded line allocation.
        let mut input = std::io::stdin().lock();
        loop {
            let mut bytes = Vec::new();
            let result = std::io::Read::take(&mut input, 131073).read_until(b'\n', &mut bytes);
            if !matches!(result, Ok(n) if n > 0 && n <= 131072) {
                break;
            }
            if tx.blocking_send(bytes).is_err() {
                break;
            }
        }
    });
    // Remote polling runs independently; a slow/offline peer cannot starve stdin
    // commands such as snapshot, approve or shutdown.
    let refresh_node = node.clone();
    let refresh_task = tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(2));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            timer.tick().await;
            let _ = tokio::time::timeout(Duration::from_secs(6), refresh_node.refresh()).await;
        }
    });
    while let Some(line) = rx.recv().await {
        let result = match serde_json::from_slice::<Value>(&line) {
            Ok(command) => node.command(command).await,
            Err(error) => Err(error.into()),
        };
        let output = match result {
            Ok(state) => json!({"ok": true, "state": state}),
            Err(error) => json!({"ok": false, "error": error.to_string()}),
        };
        println!("{output}");
        std::io::stdout().flush()?;
    }
    refresh_task.abort();
    node.close().await;
    Ok(())
}
