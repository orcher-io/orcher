/// Trait for tasks that can be eagerly dispatched to workers.
pub trait EagerDispatchable {
    fn task_type(&self) -> &str;
    fn task_queue(&self) -> &str;
    fn namespace(&self) -> &str;
    fn workflow_type(&self) -> Option<&str>;
}
