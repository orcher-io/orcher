//! Releasing the claims of an engine that died.
//!
//! A workflow claim is made by the engine that serves the poll, and every
//! claim names that engine (`claimed_on_node`). An engine that dies abruptly
//! leaves behind two kinds of claim nobody will ever answer: executions it
//! batch-claimed into its prefetch buffer and had not handed to any poll yet,
//! and activations it handed out whose reply never reached the worker. Both
//! used to wait out the claim timeout — five minutes in production — although
//! the surviving engines knew the engine was gone within a few missed
//! heartbeats. They were the last workflows to finish in every run that
//! killed an engine.
//!
//! Each surviving engine runs [`DeadNodeClaims::sweep`] from its membership
//! loop. It reads which members have stopped heartbeating and releases the
//! claims they made whose activation is still out, then offers the released
//! executions to its own pollers. See
//! `WorkflowExecutionRepository::release_claims_of_dead_nodes` for what is and
//! is not released, and why a worker that did get the activation is not
//! refused for it.
//!
//! The capacity permits the dead engine took for those activations died with
//! it: a permit lives in the memory of the engine that handed the activation
//! out, so there is nothing on a survivor to return.
//!
//! Tasks are claimed by workers, and a worker that got its task completes it
//! through any engine by its token, so a task in a worker's hands is not
//! bound to the engine that handed it over. What is bound to it is the
//! moment in between: a poll claims the task's matching row, then marks the
//! task started, then replies. An engine that died after the first step left
//! the row claimed and the task scheduled, and nothing ever offered it again.
//! Those rows are returned to the queue — only while the task is still
//! scheduled, since a started one may be running on a worker that got the
//! reply, and a second copy of a task is not harmless the way a second
//! activation is. One whose reply was lost after it was marked started is,
//! from here, the same as a worker that crashed with it, and its timeouts
//! recover it as they recover that.

use std::sync::Arc;

use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::cluster::ClusterMembershipRepository;
use orcher_storage::repositories::domains::matching::PgMatchingStore;
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::repositories::domains::workflow_execution::{
    DeadNode, WorkflowExecutionRepository,
};
use tracing::info;

use super::{PollingManager, TaskPollingManager};

/// What one sweep released.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Workflow claims released, each queued as a continuation here.
    pub workflows: usize,
    /// Tasks returned to their queue.
    pub tasks: usize,
}

/// The task side: where tasks are claimed, where their state is, and who to
/// tell when one is offered again.
struct TaskSide {
    matching: Arc<PgMatchingStore>,
    tasks: Arc<TaskExecutionRepository>,
    polling: Arc<TaskPollingManager>,
}

/// Releases, on behalf of one surviving engine, the claims of engines that
/// stopped heartbeating.
pub struct DeadNodeClaims {
    this_node: String,
    membership: Arc<ClusterMembershipRepository>,
    executions: Arc<WorkflowExecutionRepository>,
    polling_manager: Arc<PollingManager>,
    stale_after_secs: i64,
    grace_secs: i64,
    claim_timeout_secs: i64,
    lookback_secs: i64,
    task_side: Option<TaskSide>,
}

impl DeadNodeClaims {
    /// `stale_after_secs` is the membership's own staleness bound: a member
    /// not seen for that long is out of the live set. A member is treated as
    /// dead only `grace_secs` after that, which leaves a worker that got an
    /// activation from it just before it died time to complete it through
    /// another engine before the activation is offered to anyone else.
    /// `claim_timeout_secs` bounds how far back to look: by then any claim a
    /// dead engine made has timed out and been reclaimed anyway.
    pub fn new(
        this_node: impl Into<String>,
        membership: Arc<ClusterMembershipRepository>,
        executions: Arc<WorkflowExecutionRepository>,
        polling_manager: Arc<PollingManager>,
        stale_after_secs: i64,
        grace_secs: i64,
        claim_timeout_secs: i64,
    ) -> Self {
        Self {
            this_node: this_node.into(),
            membership,
            executions,
            polling_manager,
            stale_after_secs,
            grace_secs,
            claim_timeout_secs,
            lookback_secs: claim_timeout_secs + stale_after_secs + grace_secs,
            task_side: None,
        }
    }

    /// Also return to their queue the tasks a dead engine claimed from
    /// `matching` and never started.
    pub fn with_tasks(
        mut self,
        matching: Arc<PgMatchingStore>,
        tasks: Arc<TaskExecutionRepository>,
        polling: Arc<TaskPollingManager>,
    ) -> Self {
        self.task_side = Some(TaskSide {
            matching,
            tasks,
            polling,
        });
        self
    }

    /// Release what dead engines left claimed, and offer it to this engine's
    /// pollers. Returns what this engine released.
    ///
    /// Safe to run on every engine at once and on every tick: each claim is
    /// released by one engine, and it is that engine alone that queues the
    /// continuation for it. The call is cheap when no member is dead — one
    /// read of the membership table.
    pub async fn sweep(&self) -> OrcherResult<Swept> {
        let dead: Vec<DeadNode> = self
            .membership
            .stale_members(self.stale_after_secs + self.grace_secs, self.lookback_secs)
            .await?
            .into_iter()
            // This engine is running this; whatever its row says, its claims
            // are its own to answer.
            .filter(|member| member.node_id != self.this_node)
            .map(|member| DeadNode {
                node_id: member.node_id,
                last_seen: member.heartbeat_at,
            })
            .collect();
        if dead.is_empty() {
            return Ok(Swept::default());
        }

        // Any claim the dead engine made, it made before it stopped
        // heartbeating plus at most the membership's staleness bound.
        let released = self
            .executions
            .release_claims_of_dead_nodes(&dead, self.stale_after_secs, self.claim_timeout_secs)
            .await?;

        for claim in &released {
            // No preferred worker: the one that last ran it may have been
            // polling through the engine that died. The continuation is only
            // on this engine; a poller waiting here is woken for it (each
            // notification wakes at most one, and none if nobody is waiting
            // — then the next poll finds the continuation), and pollers on
            // other engines find the execution by their own scan.
            self.polling_manager.push_continuation(
                &claim.task_queue,
                &claim.namespace,
                &claim.execution_id,
                None,
            );
            self.polling_manager
                .notify_work_available(&claim.task_queue, &claim.namespace)
                .await;
        }
        if !released.is_empty() {
            info!(
                released = released.len(),
                dead = ?dead.iter().map(|d| d.node_id.as_str()).collect::<Vec<_>>(),
                "Handed out again the workflows a dead engine left claimed"
            );
        }
        let tasks = match &self.task_side {
            Some(side) => self.release_tasks(side, &dead).await?,
            None => 0,
        };
        Ok(Swept {
            workflows: released.len(),
            tasks,
        })
    }

    async fn release_tasks(&self, side: &TaskSide, dead: &[DeadNode]) -> OrcherResult<usize> {
        let candidates = side
            .matching
            .claims_of_dead_nodes(dead, self.stale_after_secs)
            .await?;
        if candidates.is_empty() {
            return Ok(0);
        }
        let executions: Vec<_> = candidates.iter().map(|(_, execution)| *execution).collect();
        let scheduled = side.tasks.still_scheduled(&executions).await?;
        // Nothing but the dead engine could have started one of these between
        // that read and this release. Were it only paused and back, the
        // release still hands out no second copy: whichever poll marks the
        // task started first has it, and the other's mark is refused.
        let unstarted: Vec<_> = candidates
            .iter()
            .filter(|(_, execution)| scheduled.contains(execution))
            .map(|(task_id, _)| *task_id)
            .collect();
        let returned = side.matching.release_to_ready(&unstarted, dead).await?;
        for task in &returned {
            side.polling
                .notify_work_available(&task.task_queue, &task.namespace)
                .await;
        }
        if !returned.is_empty() {
            info!(
                returned = returned.len(),
                "Returned to their queues the tasks a dead engine claimed and never started"
            );
        }
        Ok(returned.len())
    }
}
