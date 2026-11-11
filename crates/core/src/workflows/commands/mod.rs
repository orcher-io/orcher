//! Workflow command processing and routing.

mod handlers;
mod processor;
mod result;

pub use processor::CommandProcessor;
pub use result::CommandProcessingResult;
