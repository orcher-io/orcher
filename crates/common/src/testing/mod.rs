//! Test fixtures, mock data generators, and async/perf test helpers.

use crate::types::time::Timestamptz;
use crate::types::*;
use std::collections::HashMap;
use std::time::Duration;

/// Reusable key-value test data builder.
#[derive(Debug, Clone)]
pub struct TestFixture {
    pub name: String,
    pub data: HashMap<String, String>,
    pub created_at: Timestamptz,
}

impl Default for TestFixture {
    fn default() -> Self {
        Self::new("default")
    }
}

impl TestFixture {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data: HashMap::new(),
            created_at: Timestamptz::now(),
        }
    }

    pub fn with_data(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.data.insert(key.into(), value.into());
        self
    }

    pub fn get_data(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn user() -> Self {
        Self::new("user")
            .with_data("user_id", "test-user-123")
            .with_data("username", "testuser")
            .with_data("email", "test@example.com")
    }

    pub fn workflow() -> Self {
        Self::new("workflow")
            .with_data("workflow_id", "test-workflow-123")
            .with_data("name", "test-workflow")
            .with_data("status", "running")
    }

    pub fn task() -> Self {
        Self::new("task")
            .with_data("task_id", "test-task-123")
            .with_data("name", "test-task")
            .with_data("status", "pending")
    }
}

/// Async test helpers.
pub struct AsyncTestUtils;

impl AsyncTestUtils {
    pub async fn wait_for<F>(mut condition: F, timeout: Duration) -> bool
    where
        F: FnMut() -> bool + Send,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    pub async fn wait_for_async<F, Fut>(condition: F, timeout: Duration) -> bool
    where
        F: Fn() -> Fut + Send,
        Fut: std::future::Future<Output = bool> + Send,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if condition().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    pub async fn with_timeout<F, Fut, T>(future: F, timeout: Duration) -> Result<T, &'static str>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        match tokio::time::timeout(timeout, future()).await {
            Ok(result) => Ok(result),
            Err(_) => Err("Test timed out"),
        }
    }
}

/// Performance measurement helpers.
pub struct PerformanceTestUtils;

impl PerformanceTestUtils {
    pub fn measure_time<F, R>(f: F) -> (R, Duration)
    where
        F: FnOnce() -> R,
    {
        let start = std::time::Instant::now();
        let result = f();
        let duration = start.elapsed();
        (result, duration)
    }

    pub async fn measure_time_async<F, Fut, R>(f: F) -> (R, Duration)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let start = std::time::Instant::now();
        let result = f().await;
        let duration = start.elapsed();
        (result, duration)
    }

    pub fn benchmark<F>(name: &str, iterations: usize, f: F) -> Duration
    where
        F: Fn() + Clone,
    {
        let mut total_duration = Duration::ZERO;
        for _ in 0..iterations {
            let (_, duration) = Self::measure_time(f.clone());
            total_duration += duration;
        }
        let avg_duration = total_duration / iterations as u32;
        println!(
            "Benchmark '{}': {} iterations, avg: {:?}",
            name, iterations, avg_duration
        );
        avg_duration
    }
}

/// Random data generators for tests.
pub struct TestDataGenerator;

impl TestDataGenerator {
    pub fn random_string(length: usize) -> String {
        use std::iter;
        const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let mut rng = fastrand::Rng::new();
        iter::repeat_with(|| CHARSET[rng.usize(..CHARSET.len())] as char)
            .take(length)
            .collect()
    }

    pub fn random_task_id() -> TaskId {
        TaskId::new_v4()
    }

    pub fn random_workflow_id() -> WorkflowId {
        WorkflowId::new_v4()
    }

    pub fn random_user_id() -> UserId {
        UserId::new_v4()
    }

    pub fn random_test_data() -> HashMap<String, String> {
        let mut data = HashMap::new();
        data.insert("id".to_string(), Self::random_string(10));
        data.insert("name".to_string(), Self::random_string(20));
        data.insert("value".to_string(), fastrand::i32(0..1000).to_string());
        data
    }
}

/// Asserts that a condition becomes `true` before the timeout elapses.
#[macro_export]
macro_rules! assert_eventually {
    ($condition:expr, $timeout:expr) => {
        let start = std::time::Instant::now();
        while start.elapsed() < $timeout {
            if $condition {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!($condition, "Condition was not met within timeout");
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_fixture() {
        let fixture = TestFixture::new("test")
            .with_data("key1", "value1")
            .with_data("key2", "value2");

        assert_eq!(fixture.name, "test");
        assert_eq!(fixture.get_data("key1"), Some(&"value1".to_string()));
        assert_eq!(fixture.get_data("key2"), Some(&"value2".to_string()));
        assert_eq!(fixture.get_data("nonexistent"), None);
    }

    #[test]
    fn test_fixture_shortcuts() {
        let user_fixture = TestFixture::user();
        assert_eq!(user_fixture.name, "user");
        assert!(user_fixture.get_data("user_id").is_some());

        let workflow_fixture = TestFixture::workflow();
        assert_eq!(workflow_fixture.name, "workflow");
        assert!(workflow_fixture.get_data("workflow_id").is_some());

        let task_fixture = TestFixture::task();
        assert_eq!(task_fixture.name, "task");
        assert!(task_fixture.get_data("task_id").is_some());
    }

    #[tokio::test]
    async fn test_async_wait_for() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();
        let result = AsyncTestUtils::wait_for(
            move || {
                let current = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                current >= 5
            },
            Duration::from_secs(1),
        )
        .await;

        assert!(result);
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 5);
    }

    #[tokio::test]
    async fn test_async_wait_for_timeout() {
        let result = AsyncTestUtils::wait_for(
            || false, // Never true
            Duration::from_millis(50),
        )
        .await;

        assert!(!result);
    }

    #[tokio::test]
    async fn test_with_timeout() {
        let result = AsyncTestUtils::with_timeout(|| async { 42 }, Duration::from_secs(1)).await;

        assert_eq!(result, Ok(42));

        let timeout_result = AsyncTestUtils::with_timeout(
            || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                42
            },
            Duration::from_millis(50),
        )
        .await;

        assert_eq!(timeout_result, Err("Test timed out"));
    }

    #[test]
    fn test_performance_measure_time() {
        let (result, duration) = PerformanceTestUtils::measure_time(|| {
            std::thread::sleep(Duration::from_millis(10));
            42
        });

        assert_eq!(result, 42);
        assert!(duration >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_performance_measure_time_async() {
        let (result, duration) = PerformanceTestUtils::measure_time_async(|| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            42
        })
        .await;

        assert_eq!(result, 42);
        assert!(duration >= Duration::from_millis(10));
    }

    #[test]
    fn test_data_generator() {
        let random_string = TestDataGenerator::random_string(10);
        assert_eq!(random_string.len(), 10);

        let task_id = TestDataGenerator::random_task_id();
        assert!(!task_id.is_nil());

        let workflow_id = TestDataGenerator::random_workflow_id();
        assert!(!workflow_id.is_nil());

        let test_data = TestDataGenerator::random_test_data();
        assert!(test_data.contains_key("id"));
        assert!(test_data.contains_key("name"));
        assert!(test_data.contains_key("value"));
    }

    #[test]
    fn test_assertion_macros() {
        // Test that eventually macro works with immediate condition
        let condition_met = true;
        assert_eventually!(condition_met, Duration::from_millis(100));
    }
}
