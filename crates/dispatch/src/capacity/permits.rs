//! Task permit implementation for worker capacity tracking

use orcher_common::types::dispatch::TaskPermitStats;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Atomic permit counter for tracking a single worker's concurrent task capacity.
#[derive(Debug)]
pub struct TaskPermits {
    max_permits: usize,
    acquired_permits: AtomicUsize,
    identity: String,
    task_queue: String,
    namespace: String,
    registered_at: Instant,
    last_acquire_at: std::sync::RwLock<Option<Instant>>,
    last_release_at: std::sync::RwLock<Option<Instant>>,
    total_acquired: AtomicUsize,
    total_released: AtomicUsize,
    /// The executions this replica's permits were handed out for, and when.
    ///
    /// An activation's permit is returned by its completion, and with more
    /// than one replica the completion can reach a replica other than the one
    /// that handed the activation out. That replica has nothing to return, and
    /// the counter here would have stayed taken for good: permits are never
    /// reclaimed while in use, so a worker's capacity drained away one
    /// cross-replica completion at a time until it was offered no work.
    /// Recording what each permit is for lets a completion return exactly its
    /// own, and lets [`TaskPermits::release_expired`] return the ones whose
    /// completion went elsewhere.
    ///
    /// With each is the activation it was handed out for, when known, so a
    /// completion can return the permit of exactly the activation it answers
    /// and never the one handed out after it for the same execution.
    held: std::sync::Mutex<std::collections::HashMap<String, (Instant, Option<i64>)>>,
}

impl TaskPermits {
    pub fn new(
        identity: String,
        task_queue: String,
        namespace: String,
        max_permits: usize,
    ) -> Self {
        debug!(
            identity = %identity,
            task_queue = %task_queue,
            namespace = %namespace,
            max_permits = max_permits,
            "Creating TaskPermits"
        );

        Self {
            max_permits,
            acquired_permits: AtomicUsize::new(0),
            identity,
            task_queue,
            namespace,
            registered_at: Instant::now(),
            last_acquire_at: std::sync::RwLock::new(None),
            last_release_at: std::sync::RwLock::new(None),
            total_acquired: AtomicUsize::new(0),
            total_released: AtomicUsize::new(0),
            held: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.acquired_permits.load(Ordering::Relaxed);
            if current >= self.max_permits {
                debug!(
                    identity = %self.identity,
                    current = current,
                    max = self.max_permits,
                    "Permit acquisition failed: at capacity"
                );
                return false;
            }

            match self.acquired_permits.compare_exchange_weak(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if let Ok(mut last) = self.last_acquire_at.write() {
                        *last = Some(Instant::now());
                    }
                    self.total_acquired.fetch_add(1, Ordering::Relaxed);

                    debug!(
                        identity = %self.identity,
                        acquired = current + 1,
                        max = self.max_permits,
                        available = self.max_permits - current - 1,
                        "Permit acquired"
                    );
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    /// Returns the number of permits actually acquired (may be less than requested).
    /// Take a permit that gives itself back unless the work is dispatched.
    ///
    /// Use this wherever the permit is held across an await. See [`PermitGuard`].
    pub fn try_acquire_guard(self: &Arc<Self>) -> Option<PermitGuard> {
        self.try_acquire().then(|| PermitGuard {
            permits: Arc::clone(self),
            release_on_drop: true,
        })
    }

    pub fn try_acquire_many(&self, count: usize) -> usize {
        let mut acquired = 0;
        for _ in 0..count {
            if self.try_acquire() {
                acquired += 1;
            } else {
                break;
            }
        }
        acquired
    }

    /// Give a permit back.
    ///
    /// Stops at zero rather than wrapping. A bare `fetch_sub` on an empty
    /// counter becomes `usize::MAX`, `available()` saturates to nothing, and
    /// the worker is at capacity for the life of the process: it keeps polling
    /// and is never offered work again. `debug_assert!` said so and is compiled
    /// out of the builds that run in production, so the one place this mattered
    /// was the one place it was silent.
    ///
    /// A release without a matching acquire is a bug — a completion arriving
    /// twice will do it — so it is worth a warning rather than a shrug.
    pub fn release(&self) {
        // Spelled out rather than `fetch_update`, which Rust 1.99 deprecates in
        // favour of `try_update` — a name older toolchains do not have. Same
        // semantics: decrement-saturating-at-zero, SeqCst on success, Relaxed
        // on failure, and `previous` is the value that was replaced.
        let mut current = self.acquired_permits.load(Ordering::Relaxed);
        let previous = loop {
            match self.acquired_permits.compare_exchange_weak(
                current,
                current.saturating_sub(1),
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(previous) => break previous,
                Err(actual) => current = actual,
            }
        };

        if previous == 0 {
            warn!(
                identity = %self.identity,
                task_queue = %self.task_queue,
                namespace = %self.namespace,
                "Released a permit that was never acquired; ignoring so the worker keeps its capacity"
            );
            return;
        }

        if let Ok(mut last) = self.last_release_at.write() {
            *last = Some(Instant::now());
        }
        self.total_released.fetch_add(1, Ordering::Relaxed);

        debug!(
            identity = %self.identity,
            acquired = previous - 1,
            max = self.max_permits,
            available = self.max_permits - previous + 1,
            "Permit released"
        );
    }

    /// Record that an acquired permit now belongs to `execution_id`'s
    /// activation. A second permit for the same execution replaces the first,
    /// which is returned: one activation holds one permit.
    fn hold(&self, execution_id: &str, activation: Option<i64>) {
        let replaced = self
            .held
            .lock()
            .map(|mut held| {
                held.insert(execution_id.to_string(), (Instant::now(), activation))
                    .is_some()
            })
            .unwrap_or(false);
        if replaced {
            self.release();
        }
    }

    /// Return the permit held for `execution_id`'s activation, if this replica
    /// holds it. `false` means it does not: the activation was handed out by
    /// another replica, whose own expiry returns it.
    pub fn release_for(&self, execution_id: &str) -> bool {
        let held = self
            .held
            .lock()
            .map(|mut held| held.remove(execution_id).is_some())
            .unwrap_or(false);
        if held {
            self.release();
        }
        held
    }

    /// Return the permit held for activation `activation` of `execution_id`,
    /// and only that one. A permit held for a later activation of the same
    /// execution — or for one whose number was not recorded — is left alone, so
    /// a completion that arrives late cannot give back capacity that belongs to
    /// the activation handed out after it.
    pub fn release_for_activation(&self, execution_id: &str, activation: i64) -> bool {
        let held = self
            .held
            .lock()
            .map(|mut held| match held.get(execution_id) {
                Some((_, Some(recorded))) if *recorded == activation => {
                    held.remove(execution_id);
                    true
                }
                _ => false,
            })
            .unwrap_or(false);
        if held {
            self.release();
        }
        held
    }

    /// Return every permit held longer than `ttl`, and say how many.
    ///
    /// An activation still unfinished after the claim timeout is already
    /// treated as lost and handed out again, so its permit is too. This is how
    /// a permit whose completion reached another replica comes back. Returning
    /// one too early costs at most one extra concurrent activation, which the
    /// worker's own back-pressure absorbs; never returning it cost the worker.
    pub fn release_expired(&self, ttl: Duration) -> usize {
        let expired: Vec<String> = self
            .held
            .lock()
            .map(|mut held| {
                let ids: Vec<String> = held
                    .iter()
                    .filter(|(_, (at, _))| at.elapsed() > ttl)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in &ids {
                    held.remove(id);
                }
                ids
            })
            .unwrap_or_default();
        for _ in &expired {
            self.release();
        }
        if !expired.is_empty() {
            debug!(
                identity = %self.identity,
                task_queue = %self.task_queue,
                returned = expired.len(),
                "Returned permits whose activations outlived the claim timeout"
            );
        }
        expired.len()
    }

    pub fn release_many(&self, count: usize) {
        for _ in 0..count {
            self.release();
        }
    }

    pub fn available(&self) -> usize {
        self.max_permits
            .saturating_sub(self.acquired_permits.load(Ordering::Relaxed))
    }

    pub fn capacity(&self) -> usize {
        self.max_permits
    }

    pub fn used(&self) -> usize {
        self.acquired_permits.load(Ordering::Relaxed)
    }

    pub fn is_saturated(&self) -> bool {
        self.available() == 0
    }

    pub fn is_active(&self) -> bool {
        self.used() > 0
    }

    pub fn is_idle(&self) -> bool {
        self.used() == 0
    }

    pub fn utilization(&self) -> f64 {
        if self.max_permits == 0 {
            return 0.0;
        }
        self.used() as f64 / self.max_permits as f64
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn uptime(&self) -> Duration {
        self.registered_at.elapsed()
    }

    pub fn time_since_last_acquire(&self) -> Option<Duration> {
        self.last_acquire_at
            .read()
            .ok()
            .and_then(|guard| guard.map(|t| t.elapsed()))
    }

    pub fn time_since_last_release(&self) -> Option<Duration> {
        self.last_release_at
            .read()
            .ok()
            .and_then(|guard| guard.map(|t| t.elapsed()))
    }

    pub fn stats(&self) -> TaskPermitStats {
        TaskPermitStats {
            identity: self.identity.clone(),
            task_queue: self.task_queue.clone(),
            namespace: self.namespace.clone(),
            max_permits: self.max_permits,
            used_permits: self.used(),
            available_permits: self.available(),
            utilization: self.utilization(),
            total_acquired: self.total_acquired.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
            uptime: self.uptime(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_permits_basic() {
        let permits = TaskPermits::new(
            "worker-1".to_string(),
            "queue".to_string(),
            "ns".to_string(),
            10,
        );

        assert_eq!(permits.capacity(), 10);
        assert_eq!(permits.available(), 10);
        assert_eq!(permits.used(), 0);
        assert!(!permits.is_saturated());
        assert!(permits.is_idle());
    }

    #[test]
    fn test_task_permits_acquire_release() {
        let permits = TaskPermits::new(
            "worker-1".to_string(),
            "queue".to_string(),
            "ns".to_string(),
            3,
        );

        assert!(permits.try_acquire());
        assert_eq!(permits.available(), 2);

        assert!(permits.try_acquire());
        assert_eq!(permits.available(), 1);

        assert!(permits.try_acquire());
        assert_eq!(permits.available(), 0);
        assert!(permits.is_saturated());

        assert!(!permits.try_acquire());

        permits.release();
        assert_eq!(permits.available(), 1);
        assert!(!permits.is_saturated());

        assert!(permits.try_acquire());
        assert!(permits.is_saturated());
    }

    #[test]
    fn test_task_permits_acquire_many() {
        let permits = TaskPermits::new(
            "worker-1".to_string(),
            "queue".to_string(),
            "ns".to_string(),
            5,
        );

        let acquired = permits.try_acquire_many(3);
        assert_eq!(acquired, 3);
        assert_eq!(permits.available(), 2);

        let acquired = permits.try_acquire_many(5);
        assert_eq!(acquired, 2);
        assert_eq!(permits.available(), 0);

        permits.release_many(5);
        assert_eq!(permits.available(), 5);
    }

    #[test]
    fn test_task_permits_utilization() {
        let permits = TaskPermits::new(
            "worker-1".to_string(),
            "queue".to_string(),
            "ns".to_string(),
            100,
        );

        assert!((permits.utilization() - 0.0).abs() < 0.001);

        permits.try_acquire_many(50);
        assert!((permits.utilization() - 0.5).abs() < 0.001);

        permits.try_acquire_many(50);
        assert!((permits.utilization() - 1.0).abs() < 0.001);
    }
}

/// A permit that gives itself back.
///
/// A poll takes its permit before it waits, and that wait can end in ways that
/// run no further code: a worker shutting down, a dropped connection, a client
/// that gave up. The request future is cancelled where it stands, so releasing
/// on the way out of each branch quietly missed every branch that is never
/// reached — and each miss cost a permit permanently, until the worker was at
/// capacity while doing nothing at all. Dropping is the one ending they share.
///
/// [`PermitGuard::keep`] hands the permit over to the work being dispatched:
/// the activation owns it from there, and its completion gives it back.
#[derive(Debug)]
pub struct PermitGuard {
    permits: Arc<TaskPermits>,
    release_on_drop: bool,
}

impl PermitGuard {
    /// Work was dispatched — `execution_id`'s completion owns this permit now.
    pub fn keep(mut self, execution_id: &str) {
        self.release_on_drop = false;
        self.permits.hold(execution_id, None);
    }

    /// As [`keep`](Self::keep), recording which activation of the execution
    /// the permit went with.
    pub fn keep_activation(mut self, execution_id: &str, activation: i64) {
        self.release_on_drop = false;
        self.permits.hold(execution_id, Some(activation));
    }

    /// The permits this guard was taken from.
    pub fn permits(&self) -> &Arc<TaskPermits> {
        &self.permits
    }
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.permits.release();
        }
    }
}

#[cfg(test)]
mod guard_tests {
    use super::*;

    /// A late completion for an earlier activation of an execution must not
    /// return the permit taken for the activation handed out after it.
    #[test]
    fn a_permit_is_returned_only_for_its_own_activation() {
        let permits = permits(4);
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep_activation("exec", 2);

        assert!(
            !permits.release_for_activation("exec", 1),
            "an earlier activation"
        );
        assert_eq!(permits.used(), 1);
        assert!(
            !permits.release_for_activation("other", 2),
            "another execution"
        );
        assert!(permits.release_for_activation("exec", 2));
        assert_eq!(permits.used(), 0);
        assert!(!permits.release_for_activation("exec", 2), "only once");
        assert_eq!(permits.used(), 0);
    }

    fn permits(max: usize) -> Arc<TaskPermits> {
        Arc::new(TaskPermits::new(
            "worker@host".to_string(),
            "transfers".to_string(),
            "staging".to_string(),
            max,
        ))
    }

    /// The case that cost a worker its capacity: a poll that ends without
    /// reaching its release, because the request was cancelled mid-wait.
    /// The failure this prevents: an unmatched release wrapped the counter to
    /// `usize::MAX`, so the worker reported no capacity for the life of the
    /// process and was never offered work again. A completion arriving twice is
    /// enough to do it.
    #[test]
    fn an_unmatched_release_does_not_cost_the_worker_its_capacity() {
        let permits = permits(2);

        permits.release();

        assert_eq!(permits.available(), 2, "capacity is intact");
        assert!(!permits.is_saturated(), "the worker is still offered work");
        assert!(permits.try_acquire(), "and can still take work");
    }

    /// Releasing twice for one acquire gives back one permit, not two.
    #[test]
    fn a_duplicate_release_returns_at_most_what_was_taken() {
        let permits = permits(2);
        assert!(permits.try_acquire());

        permits.release();
        permits.release();

        assert_eq!(permits.available(), 2);
    }

    #[test]
    fn a_dropped_guard_gives_the_permit_back() {
        let permits = permits(2);
        {
            let _guard = permits.try_acquire_guard().expect("a permit");
            assert_eq!(permits.available(), 1);
        }
        assert_eq!(permits.available(), 2, "cancelling a poll costs nothing");
    }

    /// Dispatched work keeps its permit: the completion releases it, and a
    /// guard that also released would hand back one permit twice.
    #[test]
    fn a_kept_guard_leaves_the_permit_with_the_work() {
        let permits = permits(2);
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep("exec-1");
        assert_eq!(permits.available(), 1);
    }

    /// The completion returns exactly the permit its activation took.
    #[test]
    fn a_completion_returns_its_own_permit() {
        let permits = permits(2);
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep("exec-1");
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep("exec-2");
        assert_eq!(permits.available(), 0);

        assert!(permits.release_for("exec-1"));
        assert_eq!(permits.available(), 1);
        assert!(!permits.release_for("exec-1"), "returned once, not twice");
        assert_eq!(permits.available(), 1);
    }

    /// With two replicas, a completion can reach the one that did not hand the
    /// activation out. It must not return someone else's permit there, and the
    /// replica that holds the permit must get it back on its own.
    #[test]
    fn a_permit_whose_completion_went_elsewhere_comes_back_when_it_expires() {
        let here = permits(1);
        here.try_acquire_guard().expect("a permit").keep("exec-1");
        assert_eq!(
            here.available(),
            0,
            "the worker is at capacity on this replica"
        );

        let elsewhere = permits(1);
        assert!(
            !elsewhere.release_for("exec-1"),
            "the other replica holds nothing for it"
        );
        assert_eq!(elsewhere.available(), 1);

        assert_eq!(
            here.release_expired(Duration::from_secs(3600)),
            0,
            "not yet expired"
        );
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(here.release_expired(Duration::from_millis(10)), 1);
        assert_eq!(here.available(), 1, "capacity is back without a completion");
    }

    /// A second dispatch of the same execution replaces the first permit
    /// rather than holding two.
    #[test]
    fn one_activation_holds_one_permit() {
        let permits = permits(2);
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep("exec-1");
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep("exec-1");
        assert_eq!(permits.available(), 1);
        assert!(permits.release_for("exec-1"));
        assert_eq!(permits.available(), 2);
    }

    #[test]
    fn no_guard_is_handed_out_at_capacity() {
        let permits = permits(1);
        let _held = permits.try_acquire_guard().expect("a permit");
        assert!(permits.try_acquire_guard().is_none());
    }
}
