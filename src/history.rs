//! Hook for completed conversation turns, consumed by history storage and sync.
//!
//! The API hands every successfully completed job to a [`HistorySink`]. Only a logging sink
//! ships here; persistent storage and replication plug in behind the same trait (M-9, M-10).

use tracing::info;

/// One finished request/reply pair, handed to a [`HistorySink`] after its job completes.
#[derive(Clone, Debug)]
pub struct CompletedTurn {
    /// The job's UUID, matching `JobInfo::request_id`.
    pub request_id: String,
    /// Conversation the client named in its request, if any.
    pub conversation_id: Option<String>,
    /// Chat-completions request body as forwarded to llama-server (`conversation_id` removed).
    pub request: serde_json::Value,
    /// Assistant reply text; assembled from the delta chunks for streamed replies.
    pub response_text: String,
}

/// Receives completed turns. Called on the request's task, so implementations should return
/// quickly and move slow work (disk, network) onto their own task.
pub trait HistorySink: Send + Sync {
    /// Takes ownership of one completed turn.
    fn record(&self, turn: CompletedTurn);
}

/// Sink that only logs each turn via `tracing` (target `history`); nothing is stored.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoggingHistorySink;

impl HistorySink for LoggingHistorySink {
    fn record(&self, turn: CompletedTurn) {
        info!(
            target: "history",
            request_id = %turn.request_id,
            conversation_id = turn.conversation_id.as_deref().unwrap_or("-"),
            response_chars = turn.response_text.chars().count(),
            "completed turn"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_sink_accepts_turns_behind_a_trait_object() {
        let sink: Box<dyn HistorySink> = Box::new(LoggingHistorySink);
        sink.record(CompletedTurn {
            request_id: "id".to_owned(),
            conversation_id: None,
            request: serde_json::json!({"messages": []}),
            response_text: "hello".to_owned(),
        });
    }
}
