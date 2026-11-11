//! # Async Utilities
//!
//! This module provides async utilities for storage operations,
//! integrating with orcher-common async coordination patterns.
//!
//! These utilities can be used in both production code and testing.

use orcher_common::testing::AsyncTestUtils;
use std::future::Future;
use std::time::Duration;

/// Wait for a condition to be met with timeout
///
/// This utility repeatedly checks a condition until it becomes true or times out.
/// Useful for polling operations and state synchronization.
pub async fn wait_for_storage_condition<F>(condition: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool + Send,
{
    AsyncTestUtils::wait_for(condition, timeout).await
}

/// Wait for an async condition to be met with timeout
///
/// Similar to `wait_for_storage_condition`, but for async predicates.
pub async fn wait_for_async_storage_condition<F, Fut>(condition: F, timeout: Duration) -> bool
where
    F: Fn() -> Fut + Send,
    Fut: Future<Output = bool> + Send,
{
    AsyncTestUtils::wait_for_async(condition, timeout).await
}

/// Run an async operation with timeout
///
/// Executes a future with a timeout, returning an error if it doesn't complete in time.
pub async fn with_storage_timeout<F, Fut, T>(
    future: F,
    timeout: Duration,
) -> Result<T, &'static str>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    AsyncTestUtils::with_timeout(future, timeout).await
}

/// Helper to run async operations in a blocking context (test-only)
///
/// This is primarily useful for testing scenarios where you need to
/// run async code in a synchronous test function.
#[cfg(test)]
pub fn run_storage_test<F, Fut>(test: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(test());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_wait_for_storage_condition() {
        let mut counter = 0;
        let result = wait_for_storage_condition(
            || {
                counter += 1;
                counter >= 3
            },
            Duration::from_secs(1),
        )
        .await;
        assert!(result);
        assert!(counter >= 3);
    }

    #[tokio::test]
    async fn test_wait_for_storage_condition_timeout() {
        let result = wait_for_storage_condition(|| false, Duration::from_millis(100)).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_wait_for_async_storage_condition() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let result = wait_for_async_storage_condition(
            move || {
                let counter = counter_clone.clone();
                async move {
                    let val = counter.fetch_add(1, Ordering::SeqCst);
                    val >= 2
                }
            },
            Duration::from_secs(1),
        )
        .await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_with_storage_timeout() {
        let result = with_storage_timeout(|| async { 42 }, Duration::from_secs(1)).await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_with_storage_timeout_exceeded() {
        let result = with_storage_timeout(
            || async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                42
            },
            Duration::from_millis(100),
        )
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_run_storage_test() {
        run_storage_test(|| async {
            let value = 2 + 2;
            assert_eq!(value, 4);
        });
    }
}
