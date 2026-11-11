//! Permit-based worker capacity tracking for task dispatch.

mod permits;
mod registry;

pub use permits::{PermitGuard, TaskPermits};
pub use registry::{
    create_capacity_registry, create_capacity_registry_with_config, CapacityRegistry,
};

pub use orcher_common::types::dispatch::{
    CapacityProvider, GlobalCapacityStats, QueueCapacityStats, TaskPermitStats,
};
