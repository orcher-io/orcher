//! Timer infrastructure (wheel, accelerator).

pub mod accelerator;
pub mod wheel;

pub use accelerator::{
    AcceleratorConfig, AcceleratorStats, AddTimerResult, ExternalTimer, LoadResult,
    TimerAccelerator,
};
pub use wheel::{
    TimerEntry, TimerId, TimerWheel, TimerWheelConfig, TimerWheelMetrics, TimerWheelStats,
};
