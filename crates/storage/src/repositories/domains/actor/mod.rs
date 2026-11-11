//! # ActorState Domain Repository Module
//!
//! This module provides repository implementations for actor state persistence.
//! It handles CRUD operations, optimistic locking, TTL management, and analytics
//! for stateful actors following the virtual-objects pattern.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::actor::ActorStateRepository;
//! use orcher_storage::schema::types::actor::ActorState;
//!
//! let repo = ActorStateRepository::new(pool);
//!
//! // Store state
//! let key = ActorState::build_key("ShoppingCart", "user-123", "cart");
//! let value = serde_json::to_vec(&cart_data)?;
//! let state = ActorState::new(key, value);
//! repo.create(state).await?;
//!
//! // Retrieve state
//! let state = repo.get_state("ShoppingCart", "user-123", "cart").await?;
//!
//! // Update with optimistic locking
//! if let Some(mut state) = state {
//!     state.update_value(new_value);
//!     repo.update_with_version(state.key, state.value, state.version - 1).await?;
//! }
//!
//! // List all state keys for an actor instance
//! let keys = repo.list_keys("ShoppingCart", "user-123", None).await?;
//!
//! // Get statistics
//! let stats = repo.get_instance_stats("ShoppingCart", "user-123").await?;
//! ```

pub mod repository;

pub use repository::ActorStateRepository;
