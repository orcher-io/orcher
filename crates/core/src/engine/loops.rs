//! The background loops a running engine depends on, and whether they still run.
//!
//! A loop that returns or panics is gone for good: nothing restarts it, and
//! the engine goes on answering requests while, say, no timer fires. Health
//! used to look only at whether a loop had been started — and the handle it
//! looked at was a task that spawned the loop and finished at once — so it
//! reported such an engine healthy (orcher-io/orcher#48). A loop spawned or
//! watched through [`Loops`] can be asked about instead.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use tokio::task::{AbortHandle, JoinHandle};

/// A set of named background loops, each of which should run until the
/// engine stops.
#[derive(Debug, Default)]
pub struct Loops {
    tasks: Mutex<Vec<(&'static str, AbortHandle)>>,
    stopping: AtomicBool,
}

impl Loops {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn `future` as the loop `name` and watch it.
    pub fn spawn<F>(&self, name: &'static str, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.watch(name, handle.abort_handle());
        handle
    }

    /// Watch a loop spawned elsewhere, by its task's abort handle; whoever
    /// spawned it keeps its join handle.
    pub fn watch(&self, name: &'static str, task: AbortHandle) {
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((name, task));
    }

    /// Whether any loop has been started.
    pub fn started(&self) -> bool {
        !self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Say the loops are being stopped: from now on a loop that has ended
    /// was asked to.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    /// Whether the loops are being stopped.
    pub fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// The loops that have ended — returned or panicked — although nothing
    /// asked them to. Empty once they are being stopped.
    pub fn exited(&self) -> Vec<&'static str> {
        if self.stopping() {
            return Vec::new();
        }
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, task)| task.is_finished())
            .map(|(name, _)| *name)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn a_loop_that_ends_or_panics_is_reported_until_the_loops_are_stopped() {
        let loops = Loops::new();
        assert!(!loops.started());
        let _running = loops.spawn("running", std::future::pending::<()>());
        let _returned = loops.spawn("returned", async {});
        let _panicked = loops.spawn("panicked", async { panic!("the loop broke") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(loops.started());
        let mut exited = loops.exited();
        exited.sort();
        assert_eq!(exited, vec!["panicked", "returned"]);

        loops.stop();
        assert!(loops.exited().is_empty());
    }

    #[tokio::test]
    async fn a_watched_loop_is_followed_by_its_abort_handle() {
        let loops = Loops::new();
        let handle = tokio::spawn(async {});
        loops.watch("watched", handle.abort_handle());
        handle.await.expect("the loop ends");
        assert_eq!(loops.exited(), vec!["watched"]);
    }
}
