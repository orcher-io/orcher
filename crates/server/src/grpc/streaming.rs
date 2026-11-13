//! gRPC streaming utilities
//!
//! This module provides utilities for working with gRPC streaming responses,
//! particularly for bridging gRPC event streams to WebSocket and other protocols.

use futures::Stream;
use std::pin::Pin;
use tonic::Streaming;

/// Type alias for a pinned boxed stream
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

/// Convert a gRPC streaming response to a pinned boxed stream
///
/// This is useful for abstracting away the concrete streaming type
/// and making it easier to work with in async contexts.
///
/// # Arguments
///
/// * `stream` - The gRPC streaming response
///
/// # Returns
///
/// A pinned boxed stream that can be easily composed and transformed
pub fn to_boxed_stream<T>(stream: Streaming<T>) -> BoxStream<Result<T, tonic::Status>>
where
    T: Send + 'static,
{
    Box::pin(stream)
}

/// Stream adapter for converting gRPC streams to other formats
pub struct StreamAdapter<T> {
    inner: BoxStream<Result<T, tonic::Status>>,
}

impl<T> StreamAdapter<T>
where
    T: Send + 'static,
{
    /// Create a new stream adapter
    #[must_use]
    pub fn new(stream: Streaming<T>) -> Self {
        Self {
            inner: to_boxed_stream(stream),
        }
    }

    /// Get a reference to the inner stream
    #[must_use]
    pub fn inner(&mut self) -> &mut BoxStream<Result<T, tonic::Status>> {
        &mut self.inner
    }

    /// Consume the adapter and return the inner stream
    pub fn into_inner(self) -> BoxStream<Result<T, tonic::Status>> {
        self.inner
    }
}
