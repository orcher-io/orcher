//! Which workers have said they are shutting down, as every replica sees it.
//!
//! A worker shutting down calls `ShutdownWorker`, and from then on none of its
//! polls may take work: an open long poll is answered at once with nothing,
//! and one that arrives later is answered the same way without claiming.
//! Otherwise a worker that is draining keeps taking new activations and tasks
//! until its last poller notices, and everything it took in that time sits
//! claimed until the claim times out.
//!
//! # Across replicas
//!
//! A worker's polls reach whichever replica its load balancer picks, which is
//! usually not the one that took its `ShutdownWorker`. So the shutdown is
//! written to the database (`worker_shutdowns`) and announced on a Postgres
//! channel as it commits. Each replica listens on that channel and keeps what
//! it hears in memory, where a poll consults it on every pass of its wait loop
//! at no cost. When the listener (re)connects, it reads the table in full, so
//! an announcement missed while it was disconnected is still applied.
//!
//! The alternative, relaying the call to every live cluster member, would work
//! only with clustering on and would miss a replica that joins a moment later;
//! reading the table on every poll would put a query on the hottest path in
//! the engine. The table plus a notification costs one row per shutdown and
//! one listener connection per replica.
//!
//! # Which polls a shutdown covers
//!
//! A shutdown names a worker identity, and covers polls sent under it or under
//! the names sdk-core derives from it for its pollers. It may also name:
//!
//! - the process that is stopping (`worker_instance_id`). Then only polls
//!   from that process are refused, and a process started later under the
//!   same identity, which sends a different instance id, is served at once.
//!   A poll that sends no instance id cannot be told apart. It is refused
//!   only for the first [`ANONYMOUS_TTL`] of the shutdown, as it would be by a
//!   shutdown naming no process, since it may well be a later process.
//! - the task queues it applies to. A credential limited to some queues may
//!   stop polls on those only, so its shutdown is limited to them. Sent again
//!   with other queues, a shutdown covers those as well: it only ever grows.
//!
//! # Bounded
//!
//! A shutdown holds for [`DEFAULT_TTL`] when it names its process, ample for
//! any shutdown grace period, and for the shorter [`ANONYMOUS_TTL`] when it
//! does not: then a later process reusing the identity, which is common (a
//! fixed `service@hostname` restarted in the same pod, or developers sharing
//! one name), is refused work until it expires. That is five minutes: long
//! enough for the drain of a worker finishing long activations, short enough
//! that a restarted one is not stuck for long. An SDK should send an instance
//! id, here and on its polls, which spares a restarted worker the wait
//! altogether and lets the shutdown last as long as any drain. Clearing the
//! shutdown when the worker registers again was considered and rejected: the
//! SDK re-registers from the very process that is shutting down whenever a
//! heartbeat is answered "re-register", which would undo the shutdown.
//!
//! Every field is limited in size, and so is the set of queues a shutdown
//! covers across repeats, so the announcement always fits in a notification.
//! An organization may hold at most [`MAX_PER_ORGANIZATION`] shutdowns at once
//! and an identity [`MAX_PER_IDENTITY`], so none can grow the table, or every
//! replica's copy of it, without bound.
//!
//! # Waking waiting polls
//!
//! A shutdown wakes the polls waiting in its organization and namespace only,
//! through one of a fixed set of wake-ups chosen by hashing the two. A woken
//! poll asks again, in memory, whether the shutdown covers it, and goes back to
//! waiting if not: no claim query runs for a shutdown of some other worker.
//! Keying the wake-up by identity as well would spare even that check, but
//! would mean registering every poll, under each name it may belong to, in a
//! shared map on the hottest path in the engine, for the sake of an event that
//! happens a few times per deployment.

use dashmap::DashMap;
use orcher_common::error::OrcherError;
use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::worker::{
    ttl_for, ShutdownLimits, WorkerShutdown, WorkerShutdownRepository, MAX_NOTICE_BYTES,
    WORKER_SHUTDOWN_CHANNEL,
};
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// How long a shutdown that names its process holds.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);

/// How long a shutdown that names no process holds, and how long one that
/// names a process covers polls that name none. Shorter, because it also
/// refuses any later process that reuses the identity.
pub const ANONYMOUS_TTL: Duration = Duration::from_secs(5 * 60);

/// The longest identity, instance id, namespace or task queue a shutdown may
/// name, in bytes.
pub const MAX_FIELD_BYTES: usize = 256;

/// The most task queues one shutdown may cover, across every time it is sent.
pub const MAX_TASK_QUEUES: usize = 16;

/// The most shutdowns one organization may hold at once.
pub const MAX_PER_ORGANIZATION: usize = 1000;

/// The most processes of one identity that may be shut down at once.
pub const MAX_PER_IDENTITY: usize = 16;

/// A shutdown sent again within this long of the last time it was recorded
/// through this replica, covering nothing it does not cover already, is
/// acknowledged without writing it again: it would only move its expiry by as
/// much. With the queues merged rather than replaced, a worker sending it in
/// a loop, with any mix of queues, causes a bounded number of writes.
const REPEAT_WINDOW: Duration = Duration::from_secs(5);

/// How long to wait before reconnecting a listener that lost its connection,
/// or reading the table again after failing to, the first time. Doubled on
/// each failure in a row, up to [`MAX_RETRY_PAUSE`].
const RETRY_PAUSE: Duration = Duration::from_secs(1);
const MAX_RETRY_PAUSE: Duration = Duration::from_secs(30);

/// How many wake-ups the waiting polls are spread over.
const WAKE_BUCKETS: usize = 64;

/// The suffixes sdk-core appends to a worker's identity to name each of its
/// pollers, each followed by the poller's number. A poll arrives under the
/// poller's name; the shutdown is sent under the worker's.
const NUMBERED_POLLERS: [&str; 3] = [
    "-wf-driver-poller-",
    "-task-driver-poller-",
    "-actor-driver-poller-",
];

/// The suffix of a session's poller, followed by the session's queue.
const SESSION_POLLER: &str = "-session-poller-";

/// A poll, as far as shutdowns are concerned. Borrowed from the request, so
/// asking about it allocates nothing.
#[derive(Debug, Clone, Copy)]
pub struct Poll<'a> {
    /// The organization it was made for, if it named one.
    pub organization_id: Option<Uuid>,
    /// The namespace it polls.
    pub namespace: &'a str,
    /// The name it was sent under: the worker's identity or a poller's.
    pub identity: &'a str,
    /// The process it came from, or empty if it did not say.
    pub worker_instance_id: &'a str,
    /// The task queue it polls.
    pub task_queue: &'a str,
}

/// Which worker a shutdown is for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Key {
    organization_id: Uuid,
    namespace: String,
    identity: String,
}

/// A key or a borrowed stand-in for one, so a poll can be looked up by the
/// names it borrows rather than by strings copied for each lookup.
trait AsKey {
    fn parts(&self) -> (Uuid, &str, &str);
}

impl AsKey for Key {
    fn parts(&self) -> (Uuid, &str, &str) {
        (self.organization_id, &self.namespace, &self.identity)
    }
}

impl AsKey for (Uuid, &str, &str) {
    fn parts(&self) -> (Uuid, &str, &str) {
        *self
    }
}

// Both sides hash their parts the same way, which is what lets the map find a
// `Key` by a borrowed stand-in.
impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state)
    }
}

impl Hash for dyn AsKey + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state)
    }
}

impl PartialEq for dyn AsKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn AsKey + '_ {}

impl<'a> Borrow<dyn AsKey + 'a> for Key {
    fn borrow(&self) -> &(dyn AsKey + 'a) {
        self
    }
}

/// One shutdown of a worker, as held here.
#[derive(Debug, Clone)]
struct Held {
    /// The process shutting down; empty covers every process.
    instance: String,
    /// The queues it covers; empty covers every queue.
    task_queues: Vec<String>,
    /// When it stops holding.
    until: Instant,
    /// When it stops covering polls that name no process; `until` for a
    /// shutdown that names none.
    unnamed_until: Instant,
    /// When it was last recorded through this replica, if it was.
    recorded: Option<Instant>,
}

impl Held {
    fn live(&self, now: Instant) -> bool {
        now < self.until
    }

    fn covers(&self, poll: &Poll<'_>, now: Instant) -> bool {
        let process = if self.instance.is_empty() {
            true
        } else if poll.worker_instance_id.is_empty() {
            now < self.unnamed_until
        } else {
            self.instance == poll.worker_instance_id
        };
        self.live(now) && process && covers_queue(&self.task_queues, poll.task_queue)
    }
}

/// Whether a shutdown covering `task_queues` covers polls on `task_queue`.
fn covers_queue(task_queues: &[String], task_queue: &str) -> bool {
    task_queues.is_empty() || task_queues.iter().any(|q| q == task_queue)
}

/// The queues covered by a shutdown covering `a` sent again covering `b`: the
/// union, where every queue (empty) absorbs any other. Sorted, as stored.
fn merge_queues(a: &[String], b: &[String]) -> Vec<String> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let mut merged: Vec<String> = a.iter().chain(b).cloned().collect();
    merged.sort();
    merged.dedup();
    merged
}

/// The workers shut down, as this replica knows them.
pub struct WorkerShutdowns {
    /// Each worker's shutdowns, one per process it named.
    held: DashMap<Key, Vec<Held>>,
    /// Whether `held` has any entry: read on every poll, so a replica with no
    /// shutdowns answers without touching the map.
    any: AtomicBool,
    /// Serialises changes to `held` with the update of `any`, so a change
    /// racing another cannot leave `any` false while a shutdown is held.
    changing: Mutex<()>,
    /// Woken when a shutdown is added, one per bucket of (organization,
    /// namespace), so polls elsewhere are not disturbed.
    bells: Box<[Notify]>,
    /// Absent in tests that need no database; the shutdown is then local.
    repository: Option<WorkerShutdownRepository>,
    /// Where the listener connects; present with `repository`.
    pool: Option<PgPool>,
    /// Whether the listener is subscribed and has read the table.
    listening: tokio::sync::watch::Sender<bool>,
    ttl: Duration,
    anonymous_ttl: Duration,
}

impl std::fmt::Debug for WorkerShutdowns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerShutdowns")
            .field("shut_down", &self.held.len())
            .field("persisted", &self.repository.is_some())
            .finish()
    }
}

impl WorkerShutdowns {
    /// Shutdowns recorded in the database on `pool` and shared with every
    /// replica on it, once [`start_listener`](Self::start_listener) runs.
    pub fn new(pool: PgPool) -> Self {
        Self {
            repository: Some(WorkerShutdownRepository::new(pool.clone())),
            pool: Some(pool),
            ..Self::local()
        }
    }

    /// Shutdowns known to this replica only.
    pub fn local() -> Self {
        Self {
            held: DashMap::new(),
            any: AtomicBool::new(false),
            changing: Mutex::new(()),
            bells: (0..WAKE_BUCKETS).map(|_| Notify::new()).collect(),
            repository: None,
            pool: None,
            listening: tokio::sync::watch::Sender::new(false),
            ttl: DEFAULT_TTL,
            anonymous_ttl: ANONYMOUS_TTL,
        }
    }

    /// Hold each shutdown for `ttl`, whether or not it names its process,
    /// instead of [`DEFAULT_TTL`] and [`ANONYMOUS_TTL`].
    pub fn with_ttl(self, ttl: Duration) -> Self {
        self.with_ttls(ttl, ttl)
    }

    /// Hold a shutdown that names its process for `ttl`, and one that does
    /// not, or a poll that names none, for `anonymous_ttl`.
    pub fn with_ttls(mut self, ttl: Duration, anonymous_ttl: Duration) -> Self {
        self.ttl = ttl;
        self.anonymous_ttl = anonymous_ttl;
        self
    }

    fn limits(&self) -> ShutdownLimits {
        ShutdownLimits {
            ttl: self.ttl,
            anonymous_ttl: self.anonymous_ttl,
            max_per_organization: MAX_PER_ORGANIZATION,
            max_per_identity: MAX_PER_IDENTITY,
            max_task_queues: MAX_TASK_QUEUES,
        }
    }

    /// Record that the worker `shutdown.identity` is shutting down, for the
    /// process and queues it names. `shutdown.remaining_ms` is ignored.
    ///
    /// Refused as `InvalidInput` when a field is empty where it must not be or
    /// too long, and as `ResourceExhausted` when the organization already
    /// holds [`MAX_PER_ORGANIZATION`] shutdowns; nothing is applied then.
    /// Otherwise applied here whether or not the database write succeeds, so
    /// a poll on this replica is refused from the moment this returns; a
    /// failed write is still returned, since the other replicas have not
    /// heard.
    pub async fn shut_down(&self, mut shutdown: WorkerShutdown) -> OrcherResult<()> {
        validate(&shutdown)?;
        shutdown.task_queues.sort();
        shutdown.task_queues.dedup();
        let key = Key {
            organization_id: shutdown.organization_id,
            namespace: shutdown.namespace.clone(),
            identity: shutdown.identity.clone(),
        };
        let now = Instant::now();
        // What this replica already holds for the same process, if anything:
        // the queues it covers, and when it was last written from here.
        let (known, of_identity) = self
            .held
            .get(&key)
            .map(|held| {
                let known = held
                    .iter()
                    .find(|h| h.live(now) && h.instance == shutdown.worker_instance_id)
                    .map(|h| (h.task_queues.clone(), h.recorded));
                (known, held.iter().filter(|h| h.live(now)).count())
            })
            .unwrap_or((None, 0));
        let merged = match &known {
            Some((queues, _)) => merge_queues(queues, &shutdown.task_queues),
            None => shutdown.task_queues.clone(),
        };
        if merged.len() > MAX_TASK_QUEUES {
            return Err(OrcherError::invalid_input_field(
                "task_queues",
                format!(
                    "a worker's shutdown may cover at most {MAX_TASK_QUEUES} task queues, \
                     across every time it is sent"
                ),
            ));
        }
        let repeated = known.as_ref().is_some_and(|(queues, recorded)| {
            *queues == merged && recorded.is_some_and(|at| now < at + REPEAT_WINDOW)
        });
        if repeated {
            return Ok(());
        }
        // Checked here as well as where the rows are, so a replica without a
        // database is bounded too, and one with a database refuses a flood
        // without a write per call.
        if known.is_none() {
            if self.held_by(shutdown.organization_id, now) >= MAX_PER_ORGANIZATION {
                return Err(too_many(format!(
                    "the organization already has {MAX_PER_ORGANIZATION} workers shutting \
                     down; try again once some have expired"
                )));
            }
            if of_identity >= MAX_PER_IDENTITY {
                return Err(too_many(format!(
                    "{MAX_PER_IDENTITY} processes of this identity are already shutting \
                     down; try again once some have expired"
                )));
            }
        }

        let limits = self.limits();
        let ttl = ttl_for(&shutdown, limits);
        let recorded = match &self.repository {
            Some(repository) => repository.record(&shutdown, limits).await,
            None => Ok(WorkerShutdown {
                task_queues: merged.clone(),
                ..shutdown.clone()
            }),
        };
        match recorded {
            // Refused where the rows are: not applied here either.
            Err(e @ OrcherError::ResourceExhausted { .. }) => return Err(e),
            Err(e @ OrcherError::InvalidInput { .. }) => return Err(e),
            _ => {}
        }
        let task_queues = recorded
            .as_ref()
            .map(|recorded| recorded.task_queues.clone())
            .unwrap_or(merged);
        let unnamed = ttl.min(self.anonymous_ttl);
        self.hold(
            key,
            Held {
                instance: shutdown.worker_instance_id.clone(),
                task_queues: task_queues.clone(),
                until: now + ttl,
                unnamed_until: now + unnamed,
                // Only a write that succeeded may spare the next one: a worker
                // sending it again after a failure is owed another attempt.
                recorded: recorded.is_ok().then_some(now),
            },
        );
        recorded?;
        info!(
            namespace = %shutdown.namespace,
            identity = %shutdown.identity,
            worker_instance_id = %shutdown.worker_instance_id,
            task_queues = ?task_queues,
            ttl_secs = ttl.as_secs(),
            "Worker is shutting down; its polls get no more work"
        );
        Ok(())
    }

    /// Whether `poll` belongs to a worker that is shutting down.
    pub fn is_shut_down(&self, poll: &Poll<'_>) -> bool {
        if !self.any.load(Ordering::Acquire) {
            return false;
        }
        let organization_id = poll.organization_id.unwrap_or_else(Uuid::nil);
        let now = Instant::now();
        worker_identities(poll.identity).any(|identity| {
            let probe = (organization_id, poll.namespace, identity);
            let probe: &dyn AsKey = &probe;
            let (covered, expired) = match self.held.get(probe) {
                Some(held) => (
                    held.iter().any(|h| h.covers(poll, now)),
                    held.iter().all(|h| !h.live(now)),
                ),
                None => (false, false),
            };
            if expired {
                // Forgotten once nothing in it holds, here rather than only
                // when another shutdown is added, which may never happen.
                let _changing = self.lock();
                self.held
                    .remove_if(probe, |_, held| held.iter().all(|h| !h.live(now)));
                self.any.store(!self.held.is_empty(), Ordering::Release);
            }
            covered
        })
    }

    /// Watch for shutdowns covering `poll`. Take it before the poll's first
    /// check, and keep it for the whole poll.
    pub fn watch<'a>(&'a self, poll: Poll<'a>) -> ShutdownWatch<'a> {
        ShutdownWatch::new(Some(self), poll)
    }

    /// Wait until the listener is subscribed and has read the shutdowns
    /// already recorded, so every shutdown recorded from now on reaches this
    /// replica. Never returns without a database.
    pub async fn listening(&self) {
        let mut listening = self.listening.subscribe();
        let _ = listening.wait_for(|listening| *listening).await;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.changing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn bell(&self, organization_id: Uuid, namespace: &str) -> &Notify {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (organization_id, namespace).hash(&mut hasher);
        &self.bells[(hasher.finish() % self.bells.len() as u64) as usize]
    }

    /// How many live shutdowns `organization_id` holds here.
    fn held_by(&self, organization_id: Uuid, now: Instant) -> usize {
        self.held
            .iter()
            .filter(|entry| entry.key().organization_id == organization_id)
            .map(|entry| entry.value().iter().filter(|h| h.live(now)).count())
            .sum()
    }

    fn hold(&self, key: Key, held: Held) {
        let bell = self.bell(key.organization_id, &key.namespace);
        {
            let _changing = self.lock();
            let now = Instant::now();
            let mut entry = self.held.entry(key).or_default();
            entry.retain(|h| h.live(now));
            match entry.iter_mut().find(|h| h.instance == held.instance) {
                Some(current) => {
                    current.until = current.until.max(held.until);
                    current.unnamed_until = current.unnamed_until.max(held.unnamed_until);
                    current.task_queues = merge_queues(&current.task_queues, &held.task_queues);
                    current.recorded = held.recorded.or(current.recorded);
                }
                None => entry.push(held),
            }
            drop(entry);
            // Expired entries are otherwise removed only when a poll looks
            // them up, which a worker that never polls again will not do.
            self.held.retain(|_, held| held.iter().any(|h| h.live(now)));
            self.any.store(!self.held.is_empty(), Ordering::Release);
        }
        bell.notify_waiters();
    }

    fn apply(&self, shutdown: WorkerShutdown) {
        if shutdown.remaining_ms <= 0 {
            return;
        }
        debug!(
            namespace = %shutdown.namespace,
            identity = %shutdown.identity,
            worker_instance_id = %shutdown.worker_instance_id,
            "Heard of a worker shutting down"
        );
        let now = Instant::now();
        self.hold(
            Key {
                organization_id: shutdown.organization_id,
                namespace: shutdown.namespace,
                identity: shutdown.identity,
            },
            Held {
                // A notice from a replica that did not say is taken to cover
                // polls naming no process for as long as it holds at all.
                unnamed_until: now
                    + Duration::from_millis(if shutdown.worker_instance_id.is_empty() {
                        shutdown.remaining_ms as u64
                    } else {
                        shutdown.unnamed_remaining_ms.max(0) as u64
                    }),
                instance: shutdown.worker_instance_id,
                task_queues: shutdown.task_queues,
                until: now + Duration::from_millis(shutdown.remaining_ms as u64),
                recorded: None,
            },
        );
    }

    /// Listen for shutdowns recorded by any replica, and read those already
    /// recorded. Runs until the process ends. Does nothing without a database.
    pub fn start_listener(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let (Some(repository), Some(pool)) = (self.repository.clone(), self.pool.clone())
            else {
                return;
            };
            let mut pause = RETRY_PAUSE;
            loop {
                let result = self.listen(&pool, &repository, &mut pause).await;
                self.listening.send_replace(false);
                if let Err(e) = result {
                    warn!(
                        error = %e,
                        retry_in_ms = pause.as_millis() as u64,
                        "Worker shutdown listener lost its connection; reconnecting"
                    );
                }
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(MAX_RETRY_PAUSE);
            }
        })
    }

    /// One connection's worth of listening: subscribe, catch up from the
    /// table, then apply announcements until the connection drops. `pause` is
    /// reset once caught up, so only failures in a row back off.
    async fn listen(
        &self,
        pool: &PgPool,
        repository: &WorkerShutdownRepository,
        pause: &mut Duration,
    ) -> Result<(), sqlx::Error> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen(WORKER_SHUTDOWN_CHANNEL).await?;
        // Subscribed first, read second: a shutdown recorded in between is
        // both read and heard, which is harmless, rather than neither. Read
        // until it succeeds: the channel only carries shutdowns from now on,
        // so going on without the table would miss every earlier one for as
        // long as this connection lasts. Announcements arriving meanwhile wait
        // in the listener's buffer.
        let mut reading = RETRY_PAUSE;
        loop {
            match repository.active(self.anonymous_ttl).await {
                Ok(active) => {
                    active.into_iter().for_each(|shutdown| self.apply(shutdown));
                    break;
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        retry_in_ms = reading.as_millis() as u64,
                        "Could not read the worker shutdowns already recorded; retrying"
                    );
                    tokio::time::sleep(reading).await;
                    reading = (reading * 2).min(MAX_RETRY_PAUSE);
                }
            }
        }
        *pause = RETRY_PAUSE;
        self.listening.send_replace(true);
        // `try_recv` rather than `recv`: it says when the connection was lost
        // (`Ok(None)`), so the table is read again after reconnecting.
        while let Some(notification) = listener.try_recv().await? {
            match serde_json::from_str::<WorkerShutdown>(notification.payload()) {
                Ok(shutdown) => self.apply(shutdown),
                Err(e) => warn!(error = %e, "Ignoring a malformed worker shutdown notice"),
            }
        }
        Ok(())
    }
}

/// Refuse a shutdown that could not be stored and announced whole.
fn validate(shutdown: &WorkerShutdown) -> OrcherResult<()> {
    let invalid =
        |field: &str, message: String| Err(OrcherError::invalid_input_field(field, message));
    if shutdown.identity.is_empty() {
        return invalid("identity", "identity is required".to_string());
    }
    if shutdown.namespace.is_empty() {
        return invalid("namespace", "namespace is required".to_string());
    }
    for (field, value) in [
        ("identity", &shutdown.identity),
        ("namespace", &shutdown.namespace),
        ("worker_instance_id", &shutdown.worker_instance_id),
    ] {
        if value.len() > MAX_FIELD_BYTES {
            return invalid(
                field,
                format!("{field} is longer than {MAX_FIELD_BYTES} bytes"),
            );
        }
    }
    if shutdown.task_queues.len() > MAX_TASK_QUEUES {
        return invalid(
            "task_queues",
            format!("a shutdown may name at most {MAX_TASK_QUEUES} task queues"),
        );
    }
    if let Some(queue) = shutdown
        .task_queues
        .iter()
        .find(|q| q.is_empty() || q.len() > MAX_FIELD_BYTES)
    {
        return invalid(
            "task_queues",
            format!(
                "task queue names must be 1 to {MAX_FIELD_BYTES} bytes long; one is {}",
                queue.len()
            ),
        );
    }
    // The limits above keep it well under, but the notice is what has to fit.
    if shutdown.notice()?.len() > MAX_NOTICE_BYTES {
        return invalid(
            "task_queues",
            "the shutdown is too large to announce".to_string(),
        );
    }
    Ok(())
}

fn too_many(message: String) -> OrcherError {
    OrcherError::ResourceExhausted {
        resource: "worker_shutdowns".to_string(),
        message,
    }
}

/// A wait that ends when a shutdown covering one poll is added. See
/// [`WorkerShutdowns::watch`].
pub struct ShutdownWatch<'a> {
    shutdowns: Option<&'a WorkerShutdowns>,
    poll: Poll<'a>,
}

impl<'a> ShutdownWatch<'a> {
    /// Watch `shutdowns` for `poll`, or nothing: without shutdowns the wait
    /// never ends.
    pub fn new(shutdowns: Option<&'a WorkerShutdowns>, poll: Poll<'a>) -> Self {
        Self { shutdowns, poll }
    }

    /// Whether a shutdown covers the poll now.
    pub fn is_shut_down(&self) -> bool {
        self.shutdowns
            .is_some_and(|shutdowns| shutdowns.is_shut_down(&self.poll))
    }

    /// Wait until a shutdown covers the poll; at once if one does already.
    ///
    /// Woken by any shutdown in the poll's organization and namespace, and
    /// then asks again, so a shutdown of another worker only costs a lookup
    /// in memory. Listening starts before each check, so a shutdown recorded
    /// between the check and the wait still ends it.
    pub async fn wait(&self) {
        let Some(shutdowns) = self.shutdowns else {
            return std::future::pending().await;
        };
        let organization_id = self.poll.organization_id.unwrap_or_else(Uuid::nil);
        let bell = shutdowns.bell(organization_id, self.poll.namespace);
        loop {
            let rung = bell.notified();
            tokio::pin!(rung);
            rung.as_mut().enable();
            if shutdowns.is_shut_down(&self.poll) {
                return;
            }
            rung.await;
        }
    }
}

/// The worker identities a poll sent as `poller` may belong to: the name
/// itself, and the worker's name when it is one of sdk-core's poller names.
fn worker_identities(poller: &str) -> impl Iterator<Item = &str> {
    let numbered = NUMBERED_POLLERS.iter().filter_map(move |suffix| {
        let (identity, n) = poller.rsplit_once(suffix)?;
        (!identity.is_empty() && !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            .then_some(identity)
    });
    // A session's queue is free text and may itself contain the marker, so
    // every place it occurs is a candidate.
    let session = poller
        .match_indices(SESSION_POLLER)
        .filter(move |(at, _)| *at > 0 && at + SESSION_POLLER.len() < poller.len())
        .map(move |(at, _)| &poller[..at]);
    std::iter::once(poller).chain(numbered).chain(session)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shutdown(organization_id: Option<Uuid>, namespace: &str, identity: &str) -> WorkerShutdown {
        WorkerShutdown {
            organization_id: organization_id.unwrap_or_else(Uuid::nil),
            namespace: namespace.to_string(),
            identity: identity.to_string(),
            worker_instance_id: String::new(),
            task_queues: Vec::new(),
            remaining_ms: 0,
            unnamed_remaining_ms: 0,
        }
    }

    fn poll<'a>(organization_id: Option<Uuid>, namespace: &'a str, identity: &'a str) -> Poll<'a> {
        Poll {
            organization_id,
            namespace,
            identity,
            worker_instance_id: "",
            task_queue: "q",
        }
    }

    #[test]
    fn a_poller_belongs_to_the_worker_it_is_named_after() {
        let of = |poller| worker_identities(poller).collect::<Vec<_>>();
        assert_eq!(of("w@h"), vec!["w@h"]);
        assert_eq!(
            of("w@h-wf-driver-poller-3"),
            vec!["w@h-wf-driver-poller-3", "w@h"]
        );
        assert_eq!(of("w@h-task-driver-poller-0")[1], "w@h");
        assert_eq!(of("w@h-actor-driver-poller-12")[1], "w@h");
        assert_eq!(of("w@h-session-poller-q-1")[1], "w@h");
        // Not a poller name: only the name itself.
        assert_eq!(of("w@h-wf-driver-poller-x").len(), 1);
        assert_eq!(of("w@h-wf-driver-poller-").len(), 1);
        assert_eq!(of("-wf-driver-poller-1").len(), 1);
        assert_eq!(of("w@h-session-poller-").len(), 1);
    }

    #[tokio::test]
    async fn a_shutdown_refuses_the_worker_and_its_pollers_only() {
        let shutdowns = WorkerShutdowns::local();
        let org = Some(Uuid::new_v4());
        shutdowns
            .shut_down(shutdown(org, "ns", "w@h"))
            .await
            .unwrap();

        assert!(shutdowns.is_shut_down(&poll(org, "ns", "w@h")));
        assert!(shutdowns.is_shut_down(&poll(org, "ns", "w@h-wf-driver-poller-0")));
        assert!(shutdowns.is_shut_down(&poll(org, "ns", "w@h-task-driver-poller-2")));
        // Another worker, namespace or organization is untouched.
        assert!(!shutdowns.is_shut_down(&poll(org, "ns", "w@h2-wf-driver-poller-0")));
        assert!(!shutdowns.is_shut_down(&poll(org, "other", "w@h")));
        assert!(!shutdowns.is_shut_down(&poll(Some(Uuid::new_v4()), "ns", "w@h")));
        assert!(!shutdowns.is_shut_down(&poll(None, "ns", "w@h")));
    }

    #[tokio::test]
    async fn a_shutdown_naming_its_process_spares_a_later_one() {
        let shutdowns = WorkerShutdowns::local();
        let mut stopping = shutdown(None, "ns", "w@h");
        stopping.worker_instance_id = "first".to_string();
        shutdowns.shut_down(stopping).await.unwrap();

        let from = |instance| Poll {
            worker_instance_id: instance,
            ..poll(None, "ns", "w@h-wf-driver-poller-0")
        };
        assert!(
            shutdowns.is_shut_down(&from("first")),
            "the stopping process"
        );
        assert!(
            !shutdowns.is_shut_down(&from("second")),
            "a later process under the same identity"
        );
        assert!(
            shutdowns.is_shut_down(&from("")),
            "a poll that cannot be told apart"
        );
    }

    /// A poll naming no process may come from a later one, so a shutdown that
    /// names its process covers such polls only for as long as a shutdown
    /// naming none would, not for all of its own period.
    #[tokio::test]
    async fn a_poll_naming_no_process_is_refused_only_briefly() {
        let shutdowns =
            WorkerShutdowns::local().with_ttls(Duration::from_secs(60), Duration::from_millis(50));
        let mut stopping = shutdown(None, "ns", "w@h");
        stopping.worker_instance_id = "first".to_string();
        shutdowns.shut_down(stopping).await.unwrap();

        let from = |instance| Poll {
            worker_instance_id: instance,
            ..poll(None, "ns", "w@h")
        };
        assert!(shutdowns.is_shut_down(&from("")), "at first");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!shutdowns.is_shut_down(&from("")), "not for long");
        assert!(
            shutdowns.is_shut_down(&from("first")),
            "while the stopping process stays refused"
        );
    }

    #[tokio::test]
    async fn a_shutdown_naming_queues_covers_those_only() {
        let shutdowns = WorkerShutdowns::local();
        let mut stopping = shutdown(None, "ns", "w@h");
        stopping.task_queues = vec!["qa".to_string()];
        shutdowns.shut_down(stopping).await.unwrap();

        let on = |task_queue| Poll {
            task_queue,
            ..poll(None, "ns", "w@h")
        };
        assert!(shutdowns.is_shut_down(&on("qa")));
        assert!(!shutdowns.is_shut_down(&on("prod")));
    }

    /// Sent again with other queues, a shutdown covers those too; it does not
    /// forget the ones it covered. One covering every queue stays so.
    #[tokio::test]
    async fn a_shutdown_sent_again_covers_the_queues_of_both() {
        let shutdowns = WorkerShutdowns::local();
        let on = |task_queue| Poll {
            task_queue,
            ..poll(None, "ns", "w@h")
        };
        for queue in ["q1", "q2"] {
            let mut stopping = shutdown(None, "ns", "w@h");
            stopping.task_queues = vec![queue.to_string()];
            shutdowns.shut_down(stopping).await.unwrap();
        }
        assert!(shutdowns.is_shut_down(&on("q1")), "the first queue");
        assert!(shutdowns.is_shut_down(&on("q2")), "and the second");
        assert!(!shutdowns.is_shut_down(&on("q3")));

        shutdowns
            .shut_down(shutdown(None, "ns", "w@h"))
            .await
            .unwrap();
        let mut narrower = shutdown(None, "ns", "w@h");
        narrower.task_queues = vec!["q1".to_string()];
        shutdowns.shut_down(narrower).await.unwrap();
        assert!(shutdowns.is_shut_down(&on("q3")), "every queue absorbs");
    }

    /// Alternating queue sets cannot grow a shutdown past what an announcement
    /// can carry, nor can new process ids for one identity grow without bound.
    #[tokio::test]
    async fn repeats_and_processes_of_one_identity_are_bounded() {
        let shutdowns = WorkerShutdowns::local();
        for n in 0..MAX_TASK_QUEUES {
            let mut stopping = shutdown(None, "ns", "w@h");
            stopping.task_queues = vec![format!("q{n}")];
            shutdowns.shut_down(stopping).await.unwrap();
        }
        let mut one_more = shutdown(None, "ns", "w@h");
        one_more.task_queues = vec!["q-extra".to_string()];
        assert!(matches!(
            shutdowns.shut_down(one_more).await,
            Err(OrcherError::InvalidInput { .. })
        ));

        for n in 0..MAX_PER_IDENTITY {
            let mut stopping = shutdown(None, "ns", "v@h");
            stopping.worker_instance_id = format!("process-{n}");
            shutdowns.shut_down(stopping).await.unwrap();
        }
        let mut another = shutdown(None, "ns", "v@h");
        another.worker_instance_id = "process-extra".to_string();
        assert!(matches!(
            shutdowns.shut_down(another).await,
            Err(OrcherError::ResourceExhausted { .. })
        ));
    }

    #[tokio::test]
    async fn a_shutdown_that_would_not_fit_is_refused() {
        let shutdowns = WorkerShutdowns::local();
        let long = "x".repeat(MAX_FIELD_BYTES + 1);
        let too_long = shutdown(None, "ns", &long);
        assert!(matches!(
            shutdowns.shut_down(too_long).await,
            Err(OrcherError::InvalidInput { .. })
        ));
        let mut too_many_queues = shutdown(None, "ns", "w@h");
        too_many_queues.task_queues = (0..=MAX_TASK_QUEUES).map(|i| i.to_string()).collect();
        assert!(matches!(
            shutdowns.shut_down(too_many_queues).await,
            Err(OrcherError::InvalidInput { .. })
        ));
        assert!(shutdowns.held.is_empty(), "and nothing is held");

        // The largest shutdown allowed fits in a notification.
        let widest = "x".repeat(MAX_FIELD_BYTES);
        let mut largest = shutdown(Some(Uuid::new_v4()), &widest, &widest);
        largest.worker_instance_id = widest.clone();
        largest.task_queues = vec![widest.clone(); MAX_TASK_QUEUES];
        largest.remaining_ms = i64::MAX;
        largest.unnamed_remaining_ms = i64::MAX;
        assert!(largest.notice().unwrap().len() <= MAX_NOTICE_BYTES);
    }

    #[tokio::test]
    async fn an_organization_may_hold_only_so_many() {
        let shutdowns = WorkerShutdowns::local();
        let org = Some(Uuid::new_v4());
        for n in 0..MAX_PER_ORGANIZATION {
            shutdowns
                .shut_down(shutdown(org, "ns", &format!("w{n}")))
                .await
                .unwrap();
        }
        assert!(matches!(
            shutdowns.shut_down(shutdown(org, "ns", "one-more")).await,
            Err(OrcherError::ResourceExhausted { .. })
        ));
        // One already held may be sent again, and another organization is
        // unaffected.
        shutdowns
            .shut_down(shutdown(org, "ns", "w0"))
            .await
            .unwrap();
        shutdowns
            .shut_down(shutdown(Some(Uuid::new_v4()), "ns", "w0"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_shutdown_expires() {
        let shutdowns = WorkerShutdowns::local().with_ttl(Duration::from_millis(50));
        shutdowns
            .shut_down(shutdown(None, "ns", "w@h"))
            .await
            .unwrap();
        assert!(shutdowns.is_shut_down(&poll(None, "ns", "w@h")));
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!shutdowns.is_shut_down(&poll(None, "ns", "w@h")));
        assert!(shutdowns.held.is_empty(), "and is forgotten");
        assert!(!shutdowns.any.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn a_waiting_poll_is_woken_by_its_shutdown() {
        let shutdowns = Arc::new(WorkerShutdowns::local());
        let waiting = {
            let shutdowns = shutdowns.clone();
            tokio::spawn(async move {
                let watch = shutdowns.watch(poll(None, "ns", "w@h"));
                watch.wait().await;
                watch.is_shut_down()
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdowns
            .shut_down(shutdown(None, "ns", "w@h"))
            .await
            .unwrap();
        let woken = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("woken promptly")
            .unwrap();
        assert!(woken);
    }

    /// A waiting poll goes on waiting through the shutdown of another worker
    /// in its namespace, rather than returning to claim.
    #[tokio::test]
    async fn a_waiting_poll_sleeps_through_another_workers_shutdown() {
        let shutdowns = Arc::new(WorkerShutdowns::local());
        let waiting = {
            let shutdowns = shutdowns.clone();
            tokio::spawn(async move { shutdowns.watch(poll(None, "ns", "mine")).wait().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        for n in 0..20 {
            shutdowns
                .shut_down(shutdown(None, "ns", &format!("other-{n}")))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "still waiting");
        shutdowns
            .shut_down(shutdown(None, "ns", "mine"))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("woken by its own")
            .unwrap();
    }
}
