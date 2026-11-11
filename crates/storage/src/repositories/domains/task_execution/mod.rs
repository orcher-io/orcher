//! # TaskExecution Domain Repository Module
//!
//! This module provides repository implementations for task execution persistence.
//! It handles CRUD operations, filtering, updates, completion tracking, and analytics
//! for task execution history.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
//! use orcher_storage::schema::types::execution::TaskExecution;
//!
//! let repo = TaskExecutionRepository::new(pool);
//!
//! // Create execution
//! let execution = TaskExecution::new(task_id, Some("worker-1".to_string()), "running".to_string());
//! let created = repo.create(execution).await?;
//!
//! // Find by ID
//! let found = repo.find_by_id(execution_id).await?;
//!
//! // Find by task
//! let executions = repo.find_by_task_id(task_id).await?;
//!
//! // Update resource metrics
//! repo.update_resource_metrics(execution_id, Some(1000), Some(512), Some(100), Some(50)).await?;
//!
//! // Complete successfully
//! repo.complete_success(execution_id, result_data, Some(0)).await?;
//! ```

pub mod repository;

pub use repository::TaskExecutionRepository;
