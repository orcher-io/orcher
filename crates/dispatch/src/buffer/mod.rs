//! Pre-allocated buffer pooling for task dispatch to reduce allocation overhead.

pub mod integration;
pub mod pool;

pub use integration::{BufferPoolIntegration, BufferPoolIntegrationBuilder, PooledSerializer};
pub use pool::{BufferPool, BufferPoolConfig, BufferPoolStats, PooledBuffer, SizeClass};
