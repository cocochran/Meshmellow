//! FIFO inference job queue with one active job and tracked job states.
//!
//! llama-server runs one distributed generation at a time, so every request takes a
//! [`JobPermit`] from the shared [`JobQueue`] first. Permits are handed out strictly in arrival
//! order and only one exists at a time. A job moves `queued → running → completed | failed |
//! canceled`; dropping a permit (or the `acquire` future) without an outcome marks it canceled,
//! which is what happens when an HTTP client goes away mid-request.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

/// Finished jobs kept for `GET /jobs`; older ones are forgotten so memory stays bounded.
pub const MAX_RETAINED_FINISHED_JOBS: usize = 256;

/// Active plus queued inference jobs admitted at once.
pub const MAX_OUTSTANDING_JOBS: usize = 16;

/// Jobs allowed to run at once: llama-server serves one generation at a time.
const RUNNING_SLOTS: usize = 1;

/// Lifecycle state of one inference request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// Waiting for the running job (and any earlier queued ones) to finish.
    Queued,
    /// The single job currently forwarded to llama-server.
    Running,
    /// Finished and the reply was delivered.
    Completed,
    /// Upstream error or non-success reply; see [`JobInfo::error`].
    Failed,
    /// Client went away before the job finished.
    Canceled,
}

impl JobStatus {
    /// True once the job can no longer change state.
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Canceled)
    }
}

/// Snapshot of one job, as listed by [`JobQueue::jobs`] and `GET /jobs`.
#[derive(Clone, Debug, Serialize)]
pub struct JobInfo {
    /// Random UUID v4 assigned when the job was queued.
    pub request_id: String,
    /// Conversation the request belongs to, if the client named one.
    pub conversation_id: Option<String>,
    /// Current lifecycle state.
    pub status: JobStatus,
    /// When the job was queued (Unix milliseconds).
    pub created_at_unix_ms: u64,
    /// When the job became the running job (Unix milliseconds).
    pub started_at_unix_ms: Option<u64>,
    /// When the job reached a finished state (Unix milliseconds).
    pub finished_at_unix_ms: Option<u64>,
    /// Why the job failed; `None` unless the status is [`JobStatus::Failed`].
    pub error: Option<String>,
}

/// Shared state behind a [`JobQueue`] and its permits.
struct QueueInner {
    /// Bounds parsed request bodies and active job records waiting for inference.
    admission_slots: Arc<Semaphore>,
    /// Hands out the running slot; tokio's semaphore is fair, which gives FIFO order.
    slot: Arc<Semaphore>,
    /// Every active job plus the most recent finished ones, oldest first.
    jobs: Mutex<VecDeque<JobInfo>>,
}

impl QueueInner {
    fn lock_jobs(&self) -> MutexGuard<'_, VecDeque<JobInfo>> {
        // A panic while holding the lock cannot leave a half-written JobInfo behind (every
        // update is a single field assignment), so the data is still usable after poisoning.
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn register(&self, conversation_id: Option<String>) -> String {
        let request_id = Uuid::new_v4().to_string();
        self.lock_jobs().push_back(JobInfo {
            request_id: request_id.clone(),
            conversation_id,
            status: JobStatus::Queued,
            created_at_unix_ms: unix_ms_now(),
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            error: None,
        });
        request_id
    }

    fn mark_running(&self, request_id: &str) {
        let mut jobs = self.lock_jobs();
        if let Some(job) = jobs.iter_mut().find(|job| job.request_id == request_id) {
            job.status = JobStatus::Running;
            job.started_at_unix_ms = Some(unix_ms_now());
        }
    }

    fn finish(&self, request_id: &str, status: JobStatus, error: Option<String>) {
        let mut jobs = self.lock_jobs();
        if let Some(job) = jobs.iter_mut().find(|job| job.request_id == request_id) {
            job.status = status;
            job.finished_at_unix_ms = Some(unix_ms_now());
            job.error = error;
        }
        prune_finished(&mut jobs);
    }
}

/// Drops the oldest finished jobs beyond [`MAX_RETAINED_FINISHED_JOBS`]; active jobs always stay.
fn prune_finished(jobs: &mut VecDeque<JobInfo>) {
    let mut finished = jobs.iter().filter(|job| job.status.is_finished()).count();
    while finished > MAX_RETAINED_FINISHED_JOBS {
        let Some(oldest) = jobs.iter().position(|job| job.status.is_finished()) else {
            return;
        };
        jobs.remove(oldest);
        finished -= 1;
    }
}

/// Milliseconds since the Unix epoch; 0 if the system clock is set before 1970.
fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// FIFO queue that lets exactly one job run at a time and remembers every job's state.
pub struct JobQueue {
    inner: Arc<QueueInner>,
}

impl JobQueue {
    /// Creates an empty queue with a free running slot.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(QueueInner {
                admission_slots: Arc::new(Semaphore::new(MAX_OUTSTANDING_JOBS)),
                slot: Arc::new(Semaphore::new(RUNNING_SLOTS)),
                jobs: Mutex::new(VecDeque::new()),
            }),
        }
    }

    /// Registers a job and waits until it has both an outstanding-job slot and the running slot.
    ///
    /// Cancel-safe: dropping the returned future while waiting marks a registered job Canceled
    /// and gives up its place in line.
    pub async fn acquire(&self, conversation_id: Option<String>) -> JobPermit {
        let admission = Arc::clone(&self.inner.admission_slots)
            .acquire_owned()
            .await
            .expect("job queue admission semaphore is never closed");
        self.acquire_admitted(conversation_id, admission).await
    }

    /// Tries to admit an active or queued job, returning `None` immediately when the capacity is
    /// full. Once admitted, waits FIFO for the single running slot. Cancel-safe after admission.
    pub async fn try_acquire(&self, conversation_id: Option<String>) -> Option<JobPermit> {
        let admission = Arc::clone(&self.inner.admission_slots)
            .try_acquire_owned()
            .ok()?;
        Some(self.acquire_admitted(conversation_id, admission).await)
    }

    async fn acquire_admitted(
        &self,
        conversation_id: Option<String>,
        admission: OwnedSemaphorePermit,
    ) -> JobPermit {
        let waiting = QueuedGuard {
            inner: Arc::clone(&self.inner),
            request_id: self.inner.register(conversation_id),
            admission: Some(admission),
            is_armed: true,
        };
        let slot = Arc::clone(&self.inner.slot).acquire_owned().await;
        let (request_id, admission) = waiting.disarm();
        let Ok(slot) = slot else {
            // The semaphore is never closed, so this is unreachable; record it rather than panic.
            self.inner.finish(
                &request_id,
                JobStatus::Failed,
                Some("job queue closed".to_owned()),
            );
            drop(admission);
            return JobPermit::detached(Arc::clone(&self.inner), request_id);
        };
        self.inner.mark_running(&request_id);
        JobPermit {
            inner: Arc::clone(&self.inner),
            request_id,
            slot: Some(slot),
            admission: Some(admission),
            is_finished: false,
        }
    }

    /// Lists active jobs and the most recent finished ones, newest last.
    pub fn jobs(&self) -> Vec<JobInfo> {
        self.inner.lock_jobs().iter().cloned().collect()
    }
}

impl Default for JobQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Marks a still-queued job Canceled if its `acquire` future is dropped before it runs.
struct QueuedGuard {
    inner: Arc<QueueInner>,
    request_id: String,
    admission: Option<OwnedSemaphorePermit>,
    is_armed: bool,
}

impl QueuedGuard {
    /// The job got the slot: stop guarding and hand back its request ID.
    fn disarm(mut self) -> (String, OwnedSemaphorePermit) {
        self.is_armed = false;
        (
            std::mem::take(&mut self.request_id),
            self.admission
                .take()
                .expect("queued job holds an admission slot"),
        )
    }
}

impl Drop for QueuedGuard {
    fn drop(&mut self) {
        if self.is_armed {
            self.inner
                .finish(&self.request_id, JobStatus::Canceled, None);
        }
    }
}

/// The right to run one job. Only one permit exists at a time.
///
/// RAII: dropping it without [`complete`](Self::complete) or [`fail`](Self::fail) marks the job
/// Canceled (the client went away) and frees the slot for the next queued job.
pub struct JobPermit {
    inner: Arc<QueueInner>,
    request_id: String,
    /// Released after the outcome is recorded, so the next job starts after this one finished.
    slot: Option<OwnedSemaphorePermit>,
    /// Bounds total active and queued jobs until this outcome is recorded.
    admission: Option<OwnedSemaphorePermit>,
    is_finished: bool,
}

impl JobPermit {
    /// Permit for a job whose outcome is already recorded; holds no slot.
    fn detached(inner: Arc<QueueInner>, request_id: String) -> Self {
        Self {
            inner,
            request_id,
            slot: None,
            admission: None,
            is_finished: true,
        }
    }

    /// The job's UUID, as shown in [`JobInfo::request_id`].
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Marks the job Completed and lets the next queued job run.
    pub fn complete(mut self) {
        self.finish(JobStatus::Completed, None);
    }

    /// Marks the job Failed with `error` and lets the next queued job run.
    pub fn fail(mut self, error: String) {
        self.finish(JobStatus::Failed, Some(error));
    }

    fn finish(&mut self, status: JobStatus, error: Option<String>) {
        if self.is_finished {
            return;
        }
        self.is_finished = true;
        self.inner.finish(&self.request_id, status, error);
        self.slot.take();
        self.admission.take();
    }
}

impl std::fmt::Debug for JobPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobPermit")
            .field("request_id", &self.request_id)
            .field("is_finished", &self.is_finished)
            .finish()
    }
}

impl Drop for JobPermit {
    fn drop(&mut self) {
        self.finish(JobStatus::Canceled, None);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Long enough for a spawned task to reach its await point on any CI machine.
    const SETTLE: Duration = Duration::from_millis(50);

    fn status_of(queue: &JobQueue, request_id: &str) -> JobStatus {
        queue
            .jobs()
            .into_iter()
            .find(|job| job.request_id == request_id)
            .map(|job| job.status)
            .expect("job is listed")
    }

    fn statuses(queue: &JobQueue) -> Vec<JobStatus> {
        queue.jobs().into_iter().map(|job| job.status).collect()
    }

    fn spawn_acquire(queue: &Arc<JobQueue>, label: &str) -> tokio::task::JoinHandle<JobPermit> {
        let queue = Arc::clone(queue);
        let label = label.to_owned();
        tokio::spawn(async move { queue.acquire(Some(label)).await })
    }

    #[tokio::test]
    async fn first_job_runs_immediately_with_uuid_and_timestamps() {
        let queue = JobQueue::new();
        let permit = queue.acquire(Some("conv".to_owned())).await;
        let jobs = queue.jobs();
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.request_id, permit.request_id());
        assert_eq!(
            Uuid::parse_str(&job.request_id).unwrap().get_version_num(),
            4
        );
        assert_eq!(job.conversation_id.as_deref(), Some("conv"));
        assert!(job.created_at_unix_ms > 0);
        assert!(job.started_at_unix_ms.unwrap() >= job.created_at_unix_ms);
        assert!(job.finished_at_unix_ms.is_none());
        permit.complete();
        let job = &queue.jobs()[0];
        assert_eq!(job.status, JobStatus::Completed);
        assert!(job.finished_at_unix_ms.unwrap() >= job.started_at_unix_ms.unwrap());
        assert!(job.error.is_none());
    }

    #[tokio::test]
    async fn only_one_job_runs_and_waiters_start_in_fifo_order() {
        let queue = Arc::new(JobQueue::new());
        let first = queue.acquire(None).await;
        let second = spawn_acquire(&queue, "second");
        tokio::time::sleep(SETTLE).await;
        let third = spawn_acquire(&queue, "third");
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            statuses(&queue),
            [JobStatus::Running, JobStatus::Queued, JobStatus::Queued]
        );

        first.complete();
        let second = second.await.unwrap();
        assert_eq!(
            statuses(&queue),
            [JobStatus::Completed, JobStatus::Running, JobStatus::Queued]
        );
        assert!(!third.is_finished());

        second.complete();
        let third = third.await.unwrap();
        assert_eq!(status_of(&queue, third.request_id()), JobStatus::Running);
        let jobs = queue.jobs();
        assert_eq!(jobs[2].conversation_id.as_deref(), Some("third"));
        assert!(jobs[2].started_at_unix_ms.unwrap() >= jobs[1].finished_at_unix_ms.unwrap());
        third.complete();
    }

    #[tokio::test]
    async fn outstanding_job_limit_rejects_without_registering_another_job() {
        let queue = Arc::new(JobQueue::new());
        let first = queue.acquire(None).await;
        let waiters: Vec<_> = (0..MAX_OUTSTANDING_JOBS - 1)
            .map(|index| spawn_acquire(&queue, &format!("queued-{index}")))
            .collect();
        tokio::time::sleep(SETTLE).await;

        assert_eq!(queue.jobs().len(), MAX_OUTSTANDING_JOBS);
        assert!(
            queue
                .try_acquire(Some("overflow".to_owned()))
                .await
                .is_none()
        );
        assert_eq!(queue.jobs().len(), MAX_OUTSTANDING_JOBS);

        first.complete();
        for waiter in waiters {
            waiter.await.unwrap().complete();
        }
        assert_eq!(
            queue.jobs().len(),
            MAX_RETAINED_FINISHED_JOBS.min(MAX_OUTSTANDING_JOBS)
        );
        let final_permit = queue.acquire(None).await;
        final_permit.complete();
    }

    #[tokio::test]
    async fn dropping_a_permit_cancels_the_job_and_frees_the_slot() {
        let queue = Arc::new(JobQueue::new());
        let first = queue.acquire(None).await;
        let first_id = first.request_id().to_owned();
        let second = spawn_acquire(&queue, "second");
        tokio::time::sleep(SETTLE).await;

        drop(first);
        let second = second.await.unwrap();
        assert_eq!(status_of(&queue, &first_id), JobStatus::Canceled);
        assert_eq!(status_of(&queue, second.request_id()), JobStatus::Running);
        second.complete();
    }

    #[tokio::test]
    async fn dropping_a_queued_acquire_cancels_it_without_blocking_others() {
        let queue = Arc::new(JobQueue::new());
        let first = queue.acquire(None).await;
        let abandoned = spawn_acquire(&queue, "abandoned");
        tokio::time::sleep(SETTLE).await;
        let third = spawn_acquire(&queue, "third");
        tokio::time::sleep(SETTLE).await;

        abandoned.abort();
        assert!(abandoned.await.unwrap_err().is_cancelled());
        assert_eq!(queue.jobs()[1].status, JobStatus::Canceled);

        first.complete();
        let third = third.await.unwrap();
        assert_eq!(status_of(&queue, third.request_id()), JobStatus::Running);
        third.complete();
    }

    #[tokio::test]
    async fn fail_records_the_error_and_frees_the_slot() {
        let queue = JobQueue::new();
        let permit = queue.acquire(None).await;
        permit.fail("upstream returned 500".to_owned());
        let job = &queue.jobs()[0];
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.error.as_deref(), Some("upstream returned 500"));
        assert!(job.finished_at_unix_ms.is_some());

        let next = tokio::time::timeout(SETTLE, queue.acquire(None))
            .await
            .expect("slot was freed");
        next.complete();
    }

    #[tokio::test]
    async fn retained_history_is_bounded_but_keeps_active_jobs() {
        let queue = JobQueue::new();
        for _ in 0..MAX_RETAINED_FINISHED_JOBS + 10 {
            queue.acquire(None).await.complete();
        }
        let running = queue.acquire(None).await;
        let jobs = queue.jobs();
        assert_eq!(jobs.len(), MAX_RETAINED_FINISHED_JOBS + 1);
        assert_eq!(jobs.last().unwrap().request_id, running.request_id());
        assert_eq!(jobs.last().unwrap().status, JobStatus::Running);
        running.complete();
        assert_eq!(queue.jobs().len(), MAX_RETAINED_FINISHED_JOBS);
    }

    #[test]
    fn statuses_serialize_lowercase() {
        let json = serde_json::to_string(&JobStatus::Canceled).unwrap();
        assert_eq!(json, "\"canceled\"");
    }
}
