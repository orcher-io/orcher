//! # Schedule Domain Repository Module
//!
//! Schedule persistence implementation for the ORCHER platform.

pub mod repository;

pub use repository::ScheduleRepository;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_module_exports() {
        // Compile-time test to ensure ScheduleRepository is properly exported
        fn _assert_schedule_repo<T: Clone + Send + Sync + std::fmt::Debug>() {}
        _assert_schedule_repo::<ScheduleRepository>();
    }
}
