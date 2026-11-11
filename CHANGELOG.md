# Changelog

## 0.7.0 (2026-11-09)


### Features

* hold a resumed run for the worker process keeping its journal
* **image:** ship the dashboard in the engine image
* organization ids are tenancy keys the engine never looks up
* send a worker only the journal entries it does not hold
* serve the dashboard on the HTTP port
* tenancy is declared, never inferred from the account tables


### Bug Fixes

* **ci:** grant the release's image job what engine-image.yml asks for

## 0.6.2 (2026-11-06)


### Bug Fixes

* record a workflow's failure however long its error


### Documentation

* a short, true README per crate
* SECURITY.md, and a README that says what is true

## 0.6.1 (2026-11-03)


### Bug Fixes

* apply a workflow completion whole, whether or not its worker waits
* **dispatch:** a workflow poll hears a wake that lands while it claims

## 0.6.0 (2026-11-02)


### ⚠ BREAKING CHANGES

* the engine no longer starts when it would serve the API without authentication on an address other than loopback. Enable authentication (security.auth_mode, for example api-key), listen on 127.0.0.1, or set ORCHER_INSECURE=true. The container image listens on every interface, so a container run without authentication needs ORCHER_INSECURE=true.
* cancelling a workflow whose code has run, while no worker is serving it, no longer ends it at once; it ends when a worker picks it up or at its cleanup deadline. Terminate still stops a workflow immediately. Requires orcher-proto 0.2.
* move the CLI to orcher-io/cli

### Features

* accept tokens from an OpenID Connect identity provider
* **auth:** give API keys and tokens scopes, enforced per call
* **auth:** issue a deployment's first API key from the engine's command line
* **auth:** issue, list and revoke API keys through the engine's REST API
* bind a run's step-id scheme and history lineage at its first activation
* cancel every workflow through one path, whoever asks
* declare a deployment's tenancy instead of inferring it
* **engine:** delay a start's first activation, store its annotations, and answer a repeated request id with the run it made
* hand out up to k items in one workflow or task poll
* let a cancelled workflow clean up before it ends
* one authenticator for every API the engine serves
* record several running tasks' heartbeats in one call
* record step results as journal entries
* refuse to serve the API without authentication off loopback
* serve the REST API from the engine, behind its authentication


### Bug Fixes

* accept a timer that is already due when it is recorded
* answer a racing repeat of a start with the run its start made
* answer a result wait when the workflow timed out
* build the Rust benchmark worker from the sdk-rust examples
* connect the gateway to the orchestrator its flag names
* deliver every event through one path, and run batch send_event
* **dispatch:** claim a poll's own queue and settle a dispatch once its task is taken
* drop the Redis settings nothing reads
* end the tasks a finished run leaves scheduled
* enforce the run and task timeouts a workflow is started with
* **engine:** answer a result wait with the final outcome and count maximum_attempts from the first run
* **engine:** count a workflow activation's attempt from 1
* **engine:** deliver an event once per run and request id
* **engine:** read a run's log in write order and log its start once; guard task attempt numbering
* **engine:** store every start's run the same way, count task attempts from 1 with 0 unlimited, and describe a run's attempt
* filter a workflow listing by task queue
* **gateway:** remove the workflow event handler nothing feeds
* give every ended run the time it ended
* **grpc:** name a run the same way in every RPC
* hand a child's start to workers as the child's start
* hand a prefetched run only to a poll of the release it was bound to
* honour a workflow failure's non-retryable mark from protocol version 2
* keep a run's ending when a workflow cancels itself late
* log every engine crate, dispatch and events included
* never leave a prefetched activation claimed by nobody
* never record a negative step duration
* **outbox:** claim the processor's oldest ready rows, not one per workflow
* record a completed or cancelled task's input on its step
* refuse an ownerless API key on a deployment with organisations
* replay a cancelled task step to its workflow
* replay a failed task step with the failure type it recorded
* replay a task's floats with the bits the task returned
* report a task's terminal timeout to its workflow
* report every way a run ends over REST, and refuse an unknown status filter
* resolve a workflow's send-event target as every run reference is
* retry run timeouts and bound the run chain by the execution timeout
* run a REST batch cancel or terminate as its single-workflow call
* run the contract suite and the Rust examples from a fresh checkout
* say an API key or a token when a REST request has no credential
* start a workflow over REST through the engine alone
* start cron workflows and accept five-field cron expressions
* stop a cancelled workflow's queued tasks from starting
* tell a worker the retry policy its task was scheduled with
* tell a workflow its session is lost when the session's worker dies
* wake waiting workflow polls when a run starts


### Performance

* find a workflow id's latest run with one index probe
* keep per-task database work from growing with the run and the queue's churn
* write gRPC responses without waiting on Nagle's algorithm


### Documentation

* add upgrade notes for the changes since 0.5.5
* say restart-fresh in comments and upgrade notes


### Chores

* move the CLI to orcher-io/cli

## 0.5.5 (2026-10-04)


### Bug Fixes

* **engine:** hand a task out as the attempt it is, counted from 1

## 0.5.4 (2026-10-04)


### Bug Fixes

* **engine:** carry a task result once and refuse oversized payloads instead of looping

## 0.5.3 (2026-10-03)


### Bug Fixes

* **storage:** survive an additive migration under a running engine


### Documentation

* **licence:** name the contact for production-use permission

## 0.5.2 (2026-10-03)


### Bug Fixes

* **ci:** build and test on GitHub-hosted runners

## 0.5.1 (2026-10-02)


### Features

* **tasks:** time out a silent task by a default heartbeat timeout when its worker heartbeats on its own


### Bug Fixes

* **actors,tasks:** scope actor state by organization, honour reported non-retryable failures
* **dispatch:** recover lost wake-ups, keep long eager tasks with their worker, and settle matching rows
* **tasks:** never retry a failure its worker marks non-retryable
* **workers:** release a dead worker process's claims within seconds
* **workflows:** render workflow and execution ids one way, so a sent event reaches its target
* **workflows:** start a workflow id once while it is open, as its reuse policy allows

## 0.5.0 (2026-09-27)


### Features

* **workers:** let a worker hand back an activation and announce its shutdown, and claim work only within its organization

## 0.4.4 (2026-09-27)


### Bug Fixes

* **cluster:** release a dead engine's claims within seconds, not the claim timeout

## 0.4.3 (2026-09-26)


### Bug Fixes

* **tasks:** answer each task report once, for the attempt still running, and scope actor operations by organization

## 0.4.2 (2026-09-25)


### Bug Fixes

* **engine:** write a scheduled task and its dispatch together, and dispatch one left without

## 0.4.1 (2026-09-25)


### Bug Fixes

* **engine:** apply a workflow completion once, and only for the activation it answers

## 0.4.0 (2026-09-25)


### Features

* **cluster:** relay a workflow completion to the node that owns its shard


### Bug Fixes

* **children:** deliver a child's outcome to its real parent once, honour its retry policy and close policy
* **cli:** authenticate with ORCHER_API_KEY as documented
* **cluster:** publish a shard's fencing generation where its rows are written
* **dispatch:** keep worker capacity and registration sound across replicas
* **engine:** stop a lock held across an await from hanging the runtime, and shut down promptly
* **tasks:** record a task's failure or cancellation all at once

## 0.3.0 (2026-09-23)


### Features

* **security:** sign task tokens


### Bug Fixes

* **auth:** make acting for any organisation an explicit capability
* **children:** a refused start fails the parent, and a half-recorded start converges
* five silent failures in dispatch, timers, sessions and results
* **journal:** allocate event ids inside the insert, and tell a parent its child's outcome reliably
* **metrics:** one label set per metric, and count in-flight work from the database
* **migrations:** fail loudly instead of skipping
* never log a connection string's password
* **orchestrator:** fail startup when a server cannot bind, and exit if one dies
* **tenancy:** take a task's owner from its execution

## 0.2.9 (2026-09-23)


### Bug Fixes

* **tenancy:** give batch operations and release policy an owner

## 0.2.8 (2026-09-22)


### Bug Fixes

* **retries:** stop a stuck backlog from starving every new retry
* **tenancy:** scope the by-id paths that collection queries already scoped

## 0.2.7 (2026-09-20)


### Bug Fixes

* **execution:** say so when a completion cannot be read back

## 0.2.6 (2026-09-20)


### Bug Fixes

* **storage:** register the schedule migration so it runs

## 0.2.5 (2026-09-20)


### Bug Fixes

* **api:** refuse a workflow that belongs to someone else
* **batch:** a batch acts on one organisation's work

## 0.2.4 (2026-09-20)


### Bug Fixes

* **schedules:** a schedule name belongs to an organisation

## 0.2.3 (2026-09-20)


### Bug Fixes

* **api:** return only the caller's workers
* **api:** scope executions, schedules and namespaces to the caller
* **dispatch:** stop an unmatched release from costing a worker its capacity

## 0.2.2 (2026-09-19)


### Bug Fixes

* **dispatch:** stop a cancelled poll from costing a permit
* **workflows:** start a child where its parent runs

## 0.2.1 (2026-09-19)


### Bug Fixes

* **dispatch:** release the permit the poll actually took

## 0.2.0 (2026-09-19)


### Features

* bind a workflow execution to the code release that first ran it
* **cluster:** claim only the rows on shards this node owns
* **cluster:** complete shard_id coverage on task and dispatch rows
* **cluster:** contiguous shard ranges for claim-side ownership
* **cluster:** fence the remaining fenceable status writes
* **cluster:** fence the workflow write path against superseded owners
* **cluster:** live DB-backed membership view (DbCluster)
* **cluster:** live DB-backed membership view (DbCluster)
* **cluster:** MultiPoolRouter — shard ranges across databases
* **cluster:** MultiPoolRouter — shard ranges across databases
* **cluster:** namespace-aware placement (namespace → pool-set)
* **cluster:** shard hashing and HRW ownership
* **cluster:** populate shard_id so ownership can route on it
* **cluster:** shard-ownership runtime (held generations, lazy acquire)
* **cluster:** shard-ownership runtime (held generations, lazy acquire)
* **cluster:** ShardRouter seam + SinglePoolRouter (partitioned-persistence foundation)
* **cluster:** ShardRouter seam + SinglePoolRouter (v2 partitioned-persistence foundation)
* **cluster:** wire partitioned persistence across shard databases
* **core:** fence the workflow write path by shard ownership
* **core:** fence the workflow write path by shard ownership
* **core:** route the coordinator's per-shard reads (state reconstruction)
* **core:** route the coordinator's per-shard reads (state reconstruction)
* **core:** route the coordinator's per-shard write transactions
* **core:** route the coordinator's per-shard write transactions
* **core:** scatter-gather crash recovery across shard databases
* **core:** scatter-gather crash recovery across shard databases
* **dispatch:** observe task delivery path (sync_match vs claim)
* **dispatch:** observe task delivery path (sync_match vs claim)
* **engine:** wire matching delivery — transfer emitter + poll claim
* **example:** add a lean quickstart reference example
* **example:** add a lean quickstart reference example
* **executions:** record which organisation a workflow belongs to
* **executions:** scope execution reads to the calling organisation
* **grpc:** carry the calling tenant on outgoing workflow calls
* **grpc:** scope namespace resolution to the calling organisation
* **grpc:** send a configured API key on every outgoing call
* **grpc:** validate API keys against the api_keys table
* **migrate:** run migrations across shard databases
* **migrate:** run migrations across shard databases
* **orchestrator:** enable the matching layer by default
* **orchestrator:** opt-in cluster membership loop
* **orchestrator:** opt-in cluster membership loop
* **orchestrator:** run on PostgreSQL alone, without Redis
* **retries:** shard-route retry + timeout paths
* **retries:** shard-route workflow-level retry + cancel/heartbeat
* **retries:** workflow-level retry is opt-in (no retry by default)
* **server:** pluggable SecurityPolicy seam + hardening-header default
* **server:** pluggable SecurityPolicy seam with hardening-header default
* **storage:** ClaimScope seam, and group the shard seams under sharding/
* **storage:** cluster membership + shard fencing substrate (V028)
* **storage:** cluster membership + shard fencing substrate (V028)
* **storage:** co-locate a workflow whole state on its shard
* **storage:** matching store — delivery queue by (namespace, task_queue)
* **storage:** route the workflow ledger to its shard database
* **storage:** route workflow claims by the release an execution is bound to
* **versioning:** route new work to a current release, and unstick stalled runs


### Bug Fixes

* **bench:** make benchmark.sh port-configurable via ORCHER_SERVER_URL
* **benchmark:** make the local setup work as documented
* **benchmark:** repoint SDK paths to sibling repos after monorepo split
* **benchmark:** repoint SDK paths to sibling repos after monorepo split
* **ci:** make the engine release actually run
* **config:** tuning sections default when absent
* **deploy:** make the orchestrator read the mounted config
* **dispatch:** keep the claim on a workflow parked for an event
* **dispatch:** wake a parked poller on task dispatch (cut wake latency ~4-5x)
* **dispatch:** wake a parked poller on task dispatch (cut wake latency)
* **dispatch:** wake activity task pollers on transfer to matching
* **dispatch:** wake activity task pollers on transfer to matching
* **engine:** make journal event-id allocation + task completion race-free
* **engine:** make journal event-id allocation + task completion race-free
* **engine:** release workflow locks properly, and schedule a replayed timer once
* **examples:** repoint durable_execution SDK dep so benchmark.sh builds
* **examples:** repoint durable_execution SDK dep to the sibling sdk-rust crate
* **grpc:** read the tenant from one header name
* **grpc:** stop a cancellation overwriting a workflow that already finished
* **grpc:** unwrap Payload envelope on batch-start input
* **grpc:** unwrap Payload envelope on batch-start input
* **infra:** make the k8s deploy bundle deployable + HA, and test what we ship
* **infra:** make the k8s deploy bundle deployable + HA, and test what we ship
* make workflow reset work
* make workflow reset work
* **namespace:** scope namespaces to the calling organisation
* **orchestrator:** auto-enable matching layer for partitioned persistence
* **orchestrator:** auto-enable matching layer for partitioned persistence
* **orchestrator:** give the build a credential for the private proto crate
* **orchestrator:** make the Postgres pool size configurable
* **orchestrator:** make the Postgres pool size configurable (enables multi-node)
* refuse to start on a schema this build does not match
* **release:** let the workspace version move without breaking the build
* **retries:** workflow-level retry re-executes via a fresh successor
* **server:** clamp client-supplied pagination limit/offset
* **server:** clamp client-supplied pagination limit/offset
* **server:** correct status mapping + ISO-8601 timestamps
* **server:** correct status mapping + ISO-8601 timestamps
* **server:** honest status codes — batch 404→409, registry 400→501
* **server:** honest status codes (batch 404→409, registry 400→501)
* **server:** SSE stream closes on real status; reject invalid cron
* **server:** SSE stream closes on real status; reject invalid cron
* **server:** stop leaking internal errors; honest 503s
* **server:** stop leaking internal errors; honest 503s
* **storage:** comment-aware CONCURRENTLY detection + dollar-quote-aware statement splitter
* **storage:** make migration CONCURRENTLY detection comment-aware + splitter dollar-quote-aware
* **storage:** stop a batch status update overwriting a workflow that completed
* **tasks:** take the attempt number from the row that defines it
* **tenant:** read the prefixed organisation id the API hands out
* **timers:** wake the timer processor when a timer is scheduled
* **versioning:** carry the release pin onto a reset successor
* **worker:** record the registering caller's organisation
* **worker:** report a worker's real health and load
* **workflows:** honor the execution timeout a caller asks for


### Performance

* **engine:** batch workflow-execution logs via async writer
* **engine:** batch workflow-execution logs via async writer
* **engine:** cache namespace-existence checks + guard started_at write
* **engine:** cache namespace-existence checks + guard started_at write
* **engine:** collapse workflow completion into one atomic UPDATE
* **engine:** collapse workflow completion into one atomic UPDATE
* **grpc:** drop a per-completion outbox lookup that never matched
* **storage:** drop an unusable GIN and two duplicate indexes from outbox_queue
* **storage:** drop unused workflow_executions GIN indexes (V030)
* **storage:** drop unused workflow_executions GIN indexes (V030)
* **storage:** drop unused workflow_executions indexes (V029)
* **storage:** drop unused workflow_executions indexes (V029)
* **timers:** keep sub-second precision when waiting for the next timer


### Changes

* **cluster:** group store + fencing into concern folders
* **cluster:** group store + fencing into concern folders
* **cluster:** one definition of shard ownership
* **cluster:** split flat lib.rs into module layout
* **cluster:** split flat lib.rs into module layout
* **core:** remove the queue plumbing that could never be populated
* rename the conformance harness to contract
* **server:** delete dead middleware framework (-2.2k lines)
* **server:** delete dead middleware framework (-2.2k lines)


### Documentation

* stop listing Redis as a prerequisite
