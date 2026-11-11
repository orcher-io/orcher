# Upgrading

What changes for an existing deployment or client when you move to a release,
beyond the bug fixes listed in [CHANGELOG.md](CHANGELOG.md). Read the section
for every release between the one you run and the one you are moving to.

## 0.7.0

- `security.tenancy` is no longer inferred from the `organizations` table.
  Unset, a deployment serves one tenant, and the engine **refuses to start**
  if an active API key is bound to an organization or if
  `security.oidc.organization_claim` is set, since either says it serves
  several. If yours does, set `security.tenancy = "multi"` before upgrading.
  `orcher api-key create --organization` likewise refuses while tenancy is
  unset.

- V085 drops the foreign keys from the engine's tables to `organizations`
  and `users` (17 on `organization_id`, and `api_keys.user_id`). An
  organization id is now a tenancy key the engine never looks up: runs, keys
  and schedules can be stored for a tenant named by an identity provider's
  token, or on a shard database, without a row in `organizations`.
  `orcher api-key create --organization` no longer checks that the
  organization exists. The keys were `ON DELETE CASCADE`: if you hard-delete
  organization or user rows (the cloud deletes them softly), that no longer
  removes the engine data or API keys that carried them; remove those
  yourself. The migration takes each table's lock briefly and fails rather
  than waits beyond five seconds; it is retried at the next start.

## Unreleased (after 0.5.5)

### Before you start

- Run `orcher-migrate run` before starting the new engine. This release adds
  migrations V064 to V069:
  - V064 backfills `completed_at` on runs that ended without one.
  - V065 adds the run and task timeout columns.
  - V066 builds an index concurrently and drops `idx_workflow_exec_workflow_id`,
    which the new index covers. Building it does not block writes, but it takes
    time on a large `workflow_executions` table.
  - V067 adds `execution_deadline`, settles every `timed_out` run that still
    had a `next_retry_at` (the old timeout watcher left one on every timeout
    and nothing ever took it, so none of them is retried now), and rebuilds
    the workflow id hold index and the retry scan index concurrently. Neither
    rebuild blocks writes.
  - V068 builds `idx_outbox_pending_dispatch_by_queue` concurrently and drops
    `idx_outbox_pending_by_organization` and `idx_outbox_pending_unstamped`,
    which only the old task claim used. Like V066 it does not block writes but
    takes time on a large `outbox_queue` table.
  - V069 adds `annotations`, `start_request_id`, `start_after` and
    `awaiting_start` to `workflow_executions` (no table rewrite), builds the
    start request id's unique index and the delayed-start index concurrently,
    and rebuilds the two workflow claim indexes (`idx_workflow_executions_claim`,
    `idx_workflow_executions_claim_by_organization`) concurrently, beside the
    old ones, without the runs waiting out a delay. None of it blocks writes;
    the rebuilds take time on a large `workflow_executions` table. Until every
    engine runs this release, do not start workflows with a `start_delay`: an
    engine from before it does not know a run is waiting and hands it out at
    once. The rebuilt claim indexes leave waiting runs out, so an engine from
    before this release cannot use them for its workflow claims; roll every
    engine forward promptly after migrating, since until then those claims
    read the table without them.
- Redis is no longer read by anything. The Redis and stream settings
  (`REDIS_URL`, `ORCHER_USE_STREAMS`, `storage.redis_url`, `streams_*`) are
  ignored; the engine warns at startup if the environment variables are still
  set.

### Run resolution

- A call that names a run by workflow id looks only in the request's namespace
  (an empty namespace means `default`). A client that started a run in one
  namespace and looks it up from a client whose default namespace differs now
  gets `NOT_FOUND`; pass the namespace the run was started in.
- Describe and the execution journal no longer match a run by workflow type.
- `GetWorkflowResult` with a run id follows that run's successors (retries,
  restarts, resets) instead of returning the latest run that reused the
  workflow id.
- A workflow's send-event command resolves its target the same way, so a run
  id given in the workflow id field now reaches that run.

### Cancellation

- A cancelled workflow is told and may clean up before it ends (`feat!`); a
  worker must speak protocol version 1 or later to receive the request.
- Tasks the workflow scheduled before the cancel request and that had not
  started do not start. Each ends with a non-retryable `Cancelled` step the
  workflow replays; tasks scheduled after the request (cleanup) still run. A
  workflow terminated without a cancel request has its queued tasks refused
  the same way.
- A task step that ends cancelled, by its worker or by its workflow's cancel,
  replays to the workflow as a non-retryable failure of type `Cancelled`.

### Timeouts

- `run_timeout` and `task_timeout` on StartWorkflow are enforced. A run past
  its run timeout ends timed out; an activation unanswered for the task timeout
  is handed to another worker.
- A run timeout is retried under the workflow's retry policy, as a failure is:
  a fresh run starts with its own run clock. Until then the timed-out run holds
  its workflow id and is not the workflow's result. Its failure type is
  `WorkflowRunTimedOut`; list it in the policy's non-retryable types to turn
  this off.
- `execution_timeout` bounds the whole workflow, not each run: the deadline
  (`execution_deadline`) is fixed at start and carried by every retry and
  restart-fresh. A run past it ends timed out (`WorkflowExecutionTimedOut`)
  and is never retried, and no retry or restart-fresh starts after it. Runs
  started before this release keep being measured from their own start.
- A reset starts a fresh execution deadline, measured from the reset run's
  start, so a workflow that timed out can still be reset and run again.
- The `execution_timeout` on a restart-fresh command is
  ignored: the new run keeps the workflow's deadline.

### Workflow retry policy

- A workflow `RetryPolicy.maximum_attempts` counts runs, the first one
  included, as a task's does: `N` allows at most `N` runs (`N - 1` retries),
  and `1` means no retry. It used to be taken as the number of retries, so a
  workflow ran once more than its policy said: `maximum_attempts = 2` ran
  three times, and `1` still retried once. To keep a policy's old behaviour,
  raise its `maximum_attempts` by one.
- `maximum_attempts = 0` is unlimited, as the proto documents it: the workflow
  is retried until it succeeds or its `execution_timeout` runs out. It used to
  mean no retry. A start with no retry policy is still never retried; a
  policy whose attempt count is left unset (the Python and TypeScript SDKs
  send 0 when a policy object omits it) is now unlimited. A negative count is
  refused with `INVALID_ARGUMENT`.
- Runs started before this release keep the retries they were started with:
  the stored `max_retries` is a retry count and keeps that meaning; only new
  starts are converted.
- A batch start, a scheduled run and a restarted-fresh run now carry the
  workflow's retry policy. A batch start used to drop it and give every
  workflow three retries; a schedule dropped it, so scheduled runs were never
  retried; a restarted-fresh run got three retries and no policy. A
  restarted-fresh run counts its retries afresh.
- `GetWorkflowResult` answers with the workflow's final outcome only. A run
  that fails with a retry to come no longer answers a waiting client with its
  failure: the wait follows the workflow to the retry and returns its outcome.
  A run that completed without a result is answered as completed with no
  result; a client that began waiting after such a run ended used to wait
  until its timeout.
- A scheduled retry wakes the retry scheduler, so a retry starts when its
  backoff ends rather than at the next poll (every 30 seconds by default),
  whether or not `workflow_retry.enable_realtime_notifications` is set; with it
  off, the scheduler now holds one database connection to listen on.
  `ORCHER_WORKFLOW_RETRY_POLL_INTERVAL_SECS` sets the poll interval.

### Non-retryable workflow failures (orcher-io/sdk-core#102)

- A workflow failure marked `non_retryable` is not retried, whatever the
  workflow's retry policy allows, as a task's is. A failure whose type the
  policy lists in `non_retryable_error_types` is still not retried either.
- The mark is honoured only from workers that report worker protocol version
  2 or later on `CompleteWorkflowExecution` and `FailWorkflowExecution`
  (`protocol_version`). Released SDKs report version 1 or below and mark every
  workflow failure non-retryable, or none, whatever the workflow code raised;
  honouring that would end every workflow retry for them. Their failures are
  retried by the policy alone, as before, so no deployment loses a retry by
  upgrading the engine first.
- SDK releases that report version 2 mark a workflow failure non-retryable
  only when the workflow code raises the error the SDK gives task code for it
  (Rust `TaskError::non_retryable`, a truthy `non_retryable` attribute in
  Python, a truthy `nonRetryable` property in TypeScript). Upgrade the engine
  before or with those SDKs: an engine from before this release ignores the
  mark and retries such a workflow by its policy.
- The mark is stored in the failed run's `error_details` as `non_retryable`,
  so the retry scheduler decides as the failure path did. No migration.

### Start paths

- StartWorkflow, BatchStartWorkflow and SendEventWithStartWorkflow read a
  start the same way and store the same run. A batch-started workflow now
  keeps its `execution_timeout`, `run_timeout`, `task_timeout` and `labels`
  (it used to drop all four), runs on its own `task_queue` (every workflow in
  a batch used to run on the first one's), and is given a generated workflow
  id when it names none, as a single start is.
- A batch honours each workflow's `workflow_id_reuse_policy`, as a single
  start does. The batch is still all or nothing: a refused start fails it
  with `ALREADY_EXISTS` (the message names `workflow[i]`, the trailing
  metadata the run in the way), and nothing in it is started or terminated.
  A workflow id named twice in one batch is refused the same way.
- A batch checks every namespace it starts in, not only the first
  workflow's, and refuses a workflow with a cron schedule
  (`INVALID_ARGUMENT`) instead of starting it at once; schedule it with
  StartWorkflow.
- A batch-started run wakes the workflow polls waiting on its queue and is
  announced on the event bus, as a single start's is.
- A child workflow keeps the `execution_timeout`, `run_timeout` and
  `task_timeout` its start command gives it. Its execution deadline is
  measured from its own start; a child still inherits none of its parent's
  limits.
- A start on a cron schedule keeps its labels and time limits for every
  scheduled run. Schedules created before this release start their runs
  without them, as before.

### Start delay, annotations and request id

Every start path accepted these three fields and dropped them.

- `start_delay` delays a run's first activation: the run is created at once
  (it holds its workflow id, shows in listings and can be cancelled or
  terminated), and no worker is handed it until its submission plus the
  delay. It is handed out within a few milliseconds of being due by the
  engine that started it, and within a second by any other. Its clocks start
  when it is due: `run_timeout` is measured from then, and the execution
  deadline is `execution_timeout` from then, so the delay is not spent from
  either. A delayed start is answered with `started_at` set to when the run is
  due. A negative delay, one over
  100 years, or a delay on a start with a cron schedule is refused with
  `INVALID_ARGUMENT`; a zero delay is none. Batch starts honour each
  workflow's delay; SendEventWithStartWorkflow and child workflows have none.
- `annotations` are stored on the run and returned by Describe, ListWorkflows
  and SearchWorkflows. They are not searchable; use `labels` for that. A
  workflow's retries, resets and restarted-fresh runs carry its annotations,
  and a child carries the annotations its start command gives it. A schedule
  started with annotations gives them to every scheduled run.
- Retries already carried labels; a reset and a restarted-fresh run now carry
  them too (they used to start with none). A restart-fresh command's labels or
  annotations replace the run's when it gives any.
- `request_id` makes a start idempotent. A start repeating a request id
  already used in the same organization and namespace is answered with the
  run the first start made — under any `workflow_id_reuse_policy`, whether
  that run is still open or has closed, and without starting or terminating
  anything — instead of `ALREADY_EXISTS` or a second run. A request id is
  remembered for as long as the run its start made is kept; a retry,
  reset or restart-fresh of that run does not carry it. A request id given
  to a start of a different workflow (another type or workflow id) is refused
  with `INVALID_ARGUMENT`, naming the run it was used for. Request ids are at
  most 255 bytes. A start that names no workflow id but gives a request id is
  given a workflow id derived from the request id, so a repeat names the same
  workflow. Clients that send a fresh request id with every call (the Rust
  SDK does) see no change.
- A batch start repeating request ids is answered, in each such place, with
  the run the earlier start made, and starts the rest; one request id given
  twice in a batch is refused with `INVALID_ARGUMENT`.
- A SendEventWithStartWorkflow repeated with the request id of one that
  started the workflow is answered with that run (`workflow_started` true) and
  its event is seeded into it once. A repeat of one that delivered its event
  to a running workflow delivers nothing more (see below).

### Event delivery

- An event sent with a `request_id` is delivered once per target run.
  SendEvent, and SendEventWithStartWorkflow when it finds the workflow
  running, repeated with the same request id is answered as the first was
  (`recorded`; the same run, `workflow_started` false) and appends no second
  `EventReceived`, so a client retrying a send whose answer it lost no longer
  hands the workflow the event twice. Racing repeats also land once.
- The request id names one delivery to one run: a different request id is a
  different event and is delivered, as is the same request id sent to another
  run (including a later run of the same workflow id, if the first has ended
  or been replaced by then). A send without a request id is delivered every
  time, as before. Clients that send a fresh request id with every call (the
  Rust SDK does) see no change; a client that reused one request id for
  distinct events to one run now has every one after the first dropped and
  must give each event its own.
- A repeat opens no event gate, but still wakes the run, in case the request
  it repeats stopped before doing so.
- BatchStartWorkflow now answers each run's `started_at` (it was empty).

### Task retry policy

- A task `RetryPolicy.maximum_attempts` of `0` is unlimited, as the proto
  documents it and as a workflow's is: the task is retried until it succeeds,
  fails with a non-retryable error or its workflow ends. It used to be read
  as 3. `N` still allows `N` attempts, the first included, and a negative
  count now fails the workflow with the reason. A task scheduled without a
  retry policy is unchanged.
- Released SDKs send 3 for a task policy that leaves the count out, so only
  an explicit `0` changes meaning. Workers on the Python SDK (0.4.x), the Rust
  SDK up to 0.9.x or the TypeScript SDK up to 0.5.6 send a `0` a user wrote
  as it is, and those tasks are now retried without limit; give them the
  count they meant. Rust 0.10/0.11 and TypeScript 0.5.7 (on sdk-core 0.14)
  replace `0` with 3 before it is sent, so nothing changes for them until
  they move to sdk-core 0.15.

### Describe

- DescribeWorkflowExecution, ListWorkflows and SearchWorkflows fill in each
  run's `attempt`, counted from 1 as its activations carry it. It read 0.

### Tasks and logs

- A task's attempt number is 1 for its first attempt on every hand-off path.
- A task handed to a worker carries the retry policy it was scheduled with.
  It used to read as `maximum_attempts` 0 with no intervals whatever the
  workflow asked for; the engine's own retry decisions were not affected.
- A workflow activation's attempt number is counted from 1 as well: 1
  for the first run, 2 for the run of its first workflow-level retry, and so
  on. It used to be the run's retry count, 0 for the first run. The number is
  fixed for a run, so every activation and replay of it reads the same one.
  Worker code that compared the attempt against 0 to detect a first run should
  compare against 1; SDK releases that pass the number through unchanged
  expect this engine.
- Without the matching layer (the default), a task poll is handed the oldest
  pending task of its own queue. It used to take only the oldest pending task
  of each workflow across all queues, so a workflow's task on a queue no worker
  served held back its tasks on every other queue.
- The outbox processor claims the oldest ready rows, several of one workflow
  at once if they are ready. It used to take one row per workflow per poll,
  choosing workflows by id rather than by age, so under load a workflow whose
  id sorted late waited behind every other and a workflow's later rows waited
  a poll each. The unused lease-based outbox claims are removed, with
  `CoordinationRepositoryTrait::claim_outbox_tasks_parallel`.
- A task's `outbox_queue` row is marked `completed` when the task is handed
  out, by a poll or eagerly, and set back to `pending` if an eager hand-out is
  reclaimed. Delivered rows used to stay `processing`, be reset to `pending`
  by the stuck-task sweep every five minutes, and end up `pending` for good.
  Monitoring that counts outbox rows by status sees fewer `pending` and
  `processing` rows, and retention now deletes delivered ones.
- An execution's log reads in write order, and records "execution submitted"
  when the run is created and "execution started" once, on its first
  activation.

### Schedules

- A start with a cron schedule is accepted (it was refused as unconfigured),
  five-field expressions are accepted, and each scheduled run's workflow id is
  `<schedule id>@<fire time>`.

### Security

- An API key with no owner is refused on a deployment that has organizations,
  unless it delegates.

### Gateway

- `--orchestrator-url` / `ORCHESTRATOR_URL` names the orchestrator the gateway
  connects to, over `ORCHESTRATOR_ENDPOINT`. It was printed and then ignored.
- The gateway's workflow event handler is gone, with
  `ORCHESTRATOR_EVENT_STREAM_ENABLED`, `ORCHESTRATOR_EVENT_RECONNECT_DELAY` and
  the `event_stream_enabled` field of the system configuration response. It
  listened on an event bus nothing published to, so it never ran; had it run,
  it would have written run statuses outside the engine. The two variables
  are now ignored.

### Tooling

- The CLI moved to its own repository, github.com/orcher-io/cli.

### Polls for several items

- `PollWorkflowExecutions` and `PollTaskExecutions` hand out up to
  `max_items` activations or tasks in one poll, at most 16, each claimed as a
  poll for one claims it. Workers on sdk-core 0.19 and later use them; older
  workers keep polling for one item and are unaffected.
- A workflow poll with a zero `poll_timeout` now looks for work once before
  answering. It used to answer empty without looking.

### Heartbeats for several tasks

- `RecordTaskHeartbeats` records the heartbeats of several running tasks in
  one call, two statements per database however many it carries, and answers
  each: recorded with its cancel flag, or refused with the status a single
  heartbeat would get. Workers on sdk-core 0.19 and later send the heartbeats
  due together this way; older workers send one call per task as before.
