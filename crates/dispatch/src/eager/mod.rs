//! Eager task return - returns tasks directly in workflow completion responses to skip extra polls.

pub mod dispatcher;
pub mod integration;
pub mod predictor;

pub use dispatcher::{
    EagerDispatchConfig, EagerDispatchable, EagerTask, EagerTaskDispatcher, SelectionReason,
};
pub use integration::{EagerDispatchIntegration, EagerDispatchIntegrationBuilder};
pub use predictor::{PredictedTask, TaskPattern, WorkflowPredictor};
