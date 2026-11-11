//! Which worker processes this engine sees polling.
//!
//! A worker process that dies while it holds an activation never answers it,
//! and the workflow used to wait for the claim to time out — five minutes in
//! production — before anyone else could carry it on. The engine is the one
//! party that sees, without being told, whether a worker process is still
//! there: a live worker keeps a poll open on it, workflow or task, nearly all
//! the time, and every poll from a current SDK names its process
//! (`worker_instance_id`). A process that is killed drops its connections,
//! and the polls it had open end with them.
//!
//! So every poll is counted here, under its organization, namespace and
//! process, for as long as it is open, and the engine writes the processes it
//! has seen to the database every few seconds; see
//! [`LapsedWorkerClaims`](crate::engine::dispatch::LapsedWorkerClaims), which
//! also releases what a process held once nobody has seen it for long enough.
//!
//! The registration heartbeat (`WorkerService`) is not used for this. It names
//! a service, which the SDKs fill in with the worker's identity, and an
//! identity is often reused by the next process (a fixed `service@hostname`
//! restarted in the same pod), so a heartbeat cannot tell the dead process
//! from its successor; and not every SDK sends one. A poll names the process.
//!
//! Counting a poll is a map entry under a short lock, on the hottest path in
//! the engine, so nothing here touches the database. A poll that names no
//! process, or names one longer than [`MAX_INSTANCE_ID_BYTES`], is not
//! counted, and neither is what it claims: such a claim is never released on
//! its process's account and waits for its claim timeout, as every claim did.

use dashmap::DashMap;
use orcher_storage::repositories::domains::worker::SeenInstance;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// The longest process id that is tracked, in bytes, as for shutdowns.
pub const MAX_INSTANCE_ID_BYTES: usize = crate::worker::shutdowns::MAX_FIELD_BYTES;

/// The process a poll names, if it names one this engine tracks.
pub fn tracked_instance(worker_instance_id: &str) -> Option<&str> {
    Some(worker_instance_id).filter(|id| !id.is_empty() && id.len() <= MAX_INSTANCE_ID_BYTES)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    organization_id: Uuid,
    namespace: String,
    instance: String,
}

#[derive(Debug)]
struct Sighting {
    /// Polls from the process open on this engine now.
    open: usize,
    /// When one of its polls last began or ended here.
    last: Instant,
    /// The identity it last polled under.
    identity: String,
}

/// The worker processes polling this engine, and when each was last seen.
#[derive(Debug, Default)]
pub struct WorkerLiveness {
    seen: DashMap<Key, Sighting>,
}

/// A poll being counted. The process counts as seen for as long as this
/// lives, and once more when it is dropped — however the poll ends, the
/// worker hanging up included, which drops the poll's future without running
/// anything after the await it was parked on.
#[derive(Debug)]
pub struct PollPresence {
    liveness: Arc<WorkerLiveness>,
    key: Key,
}

impl Drop for PollPresence {
    fn drop(&mut self) {
        if let Some(mut sighting) = self.liveness.seen.get_mut(&self.key) {
            sighting.open = sighting.open.saturating_sub(1);
            sighting.last = Instant::now();
        }
    }
}

impl WorkerLiveness {
    /// Nobody seen yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Count a poll from `worker_instance_id`, made for `organization_id` in
    /// `namespace` under `identity`, until the returned presence is dropped.
    /// `None` for a poll that names no process this engine tracks.
    pub fn polling(
        self: &Arc<Self>,
        organization_id: Option<Uuid>,
        namespace: &str,
        worker_instance_id: &str,
        identity: &str,
    ) -> Option<PollPresence> {
        let instance = tracked_instance(worker_instance_id)?;
        let key = Key {
            organization_id: organization_id.unwrap_or_else(Uuid::nil),
            namespace: namespace.to_string(),
            instance: instance.to_string(),
        };
        {
            let mut sighting = self.seen.entry(key.clone()).or_insert_with(|| Sighting {
                open: 0,
                last: Instant::now(),
                identity: String::new(),
            });
            sighting.open += 1;
            sighting.last = Instant::now();
            if sighting.identity != identity {
                sighting.identity = identity.to_string();
            }
        }
        Some(PollPresence {
            liveness: Arc::clone(self),
            key,
        })
    }

    /// The processes seen since `since`: with a poll open now, or with one
    /// that began or ended since. A process seen only before `since` was
    /// reported by the call that took it then, and is forgotten here.
    pub fn take_seen(&self, since: Instant) -> Vec<SeenInstance> {
        let mut seen = Vec::new();
        self.seen.retain(|key, sighting| {
            let current = sighting.open > 0 || sighting.last >= since;
            if current {
                seen.push(SeenInstance {
                    organization_id: key.organization_id,
                    namespace: key.namespace.clone(),
                    worker_instance_id: key.instance.clone(),
                    identity: sighting.identity.clone(),
                });
            }
            current
        });
        seen
    }

    /// How many processes are tracked, for tests and metrics.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether no process is tracked.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_poll_counts_while_open_and_once_after() {
        let liveness = Arc::new(WorkerLiveness::new());
        let start = Instant::now();
        let presence = liveness
            .polling(None, "default", "process-a", "worker-a")
            .expect("a named process is tracked");
        let later = Instant::now();
        // Open, so seen whatever the window.
        assert_eq!(liveness.take_seen(later).len(), 1);
        drop(presence);
        // Ended inside the window: seen once more.
        assert_eq!(liveness.take_seen(start).len(), 1);
        // Ended before the window: forgotten.
        let after = Instant::now() + std::time::Duration::from_millis(1);
        assert!(liveness.take_seen(after).is_empty());
        assert!(liveness.is_empty());
    }

    #[test]
    fn processes_are_told_apart_by_organization_and_namespace() {
        let liveness = Arc::new(WorkerLiveness::new());
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let _p1 = liveness.polling(Some(a), "default", "same", "w");
        let _p2 = liveness.polling(Some(b), "default", "same", "w");
        let _p3 = liveness.polling(Some(a), "other", "same", "w");
        let _p4 = liveness.polling(Some(a), "default", "same", "w");
        let mut seen = liveness.take_seen(Instant::now());
        seen.sort();
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn a_poll_naming_no_process_or_an_oversized_one_is_not_tracked() {
        let liveness = Arc::new(WorkerLiveness::new());
        assert!(liveness.polling(None, "default", "", "w").is_none());
        let long = "x".repeat(MAX_INSTANCE_ID_BYTES + 1);
        assert!(liveness.polling(None, "default", &long, "w").is_none());
        assert!(liveness.is_empty());
    }
}
