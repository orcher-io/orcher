//! Recovery from statements prepared against an older schema.
//!
//! sqlx prepares each statement once per connection and keeps it. Postgres
//! fixes a prepared statement's result shape when it is prepared, and refuses
//! to run it once that shape would change ("cached plan must not change result
//! type", SQLSTATE 0A000) — for as long as the connection keeps it. A
//! statement reading `*` from a table a migration then extends is one such,
//! and in a rolling upgrade the engines still on the old build hold them on
//! every connection.
//!
//! The repositories name their columns (`repositories::columns`), so an
//! additive migration leaves their statements' shapes alone. This is what
//! stands behind that for a statement it misses:
//!
//! - [`retry_on_stale_plan!`] runs an operation on one connection and, when it
//!   fails this way, clears that connection's statements and runs it once
//!   more. The claim and journal paths go through it.
//! - [`recycle_stale_connections`] makes a pool close, rather than hand out,
//!   a connection opened before such a failure was last seen, so the
//!   statements it holds are not tried again anywhere else either.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use tracing::{debug, warn};

/// What Postgres says when a statement's result shape moved under it.
const STALE_PLAN_SQLSTATE: &str = "0A000";
const STALE_PLAN_MESSAGE: &str = "cached plan must not change result type";

/// Whether `error` is Postgres refusing a statement prepared against an
/// older shape of the tables it reads.
pub fn is_stale_plan(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(db) => {
            db.code().as_deref() == Some(STALE_PLAN_SQLSTATE)
                && db.message().contains(STALE_PLAN_MESSAGE)
        }
        _ => false,
    }
}

/// When a stale plan was last seen, in nanoseconds since [`clock_origin`]
/// plus one; zero for never.
static LAST_SEEN: AtomicU64 = AtomicU64::new(0);
static WARNED: AtomicBool = AtomicBool::new(false);

fn clock_origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

fn nanos_since_origin(at: Instant) -> u64 {
    u64::try_from(at.saturating_duration_since(clock_origin()).as_nanos()).unwrap_or(u64::MAX)
}

/// Record that a connection's statements went stale, so pools set up with
/// [`recycle_stale_connections`] stop handing out connections as old as it.
#[doc(hidden)]
pub fn note_stale_plan(operation: &str) {
    LAST_SEEN.fetch_max(nanos_since_origin(Instant::now()) + 1, Ordering::SeqCst);
    if !WARNED.swap(true, Ordering::SeqCst) {
        warn!(
            operation,
            "The database schema changed under this process's prepared statements \
             (cached plan must not change result type); clearing them and retrying. \
             Expected once while a newer release migrates the database."
        );
    } else {
        debug!(operation, "Stale prepared statement; clearing and retrying");
    }
}

/// Whether a connection `age` old was opened before a stale plan was last
/// seen, and so may hold statements prepared against the older schema.
fn opened_before_stale_plan(age: Duration) -> bool {
    let last_seen = LAST_SEEN.load(Ordering::SeqCst);
    if last_seen == 0 {
        return false;
    }
    let now = nanos_since_origin(Instant::now());
    let age = u64::try_from(age.as_nanos()).unwrap_or(u64::MAX);
    now.saturating_sub(age) < last_seen
}

/// Evaluate `$body` — an expression on the connection bound to `$conn`,
/// usually ending in `.await` — and if Postgres refuses one of its statements
/// as prepared against an older schema, clear that connection's statements and
/// evaluate it once more.
///
/// `$conn` names a `PoolConnection<Postgres>` or `&mut PgConnection` the
/// caller holds, so the retry runs where the refusal happened rather than on
/// whichever connection the pool hands out next. `$body` must be safe to run
/// twice: a refused statement does nothing, but anything before it in the
/// same body has happened. Begin a transaction inside it when there is
/// anything before it: the refusal aborts the transaction and the retry runs
/// the whole of it again.
///
/// A macro rather than a function taking a closure: a closure returning a
/// future that borrows the connection cannot be proved `Send` for every
/// lifetime, and the engine's futures must be.
macro_rules! retry_on_stale_plan {
    ($operation:expr, $conn:ident => $body:expr) => {{
        match $body {
            Err(e) if $crate::stale_plan::is_stale_plan(&e) => {
                $crate::stale_plan::note_stale_plan($operation);
                #[allow(unused_imports)]
                use ::sqlx::Connection as _;
                match $conn.clear_cached_statements().await {
                    Ok(()) => $body,
                    Err(clear_error) => Err(clear_error),
                }
            }
            other => other,
        }
    }};
}
pub(crate) use retry_on_stale_plan;

/// `options`, closing instead of handing out a connection opened before a
/// stale plan was last seen anywhere in this process.
///
/// A migration changes the shape of every table it touches for every
/// connection at once; one connection refusing a statement means the others
/// opened before it hold the same statement. Closing them costs a reconnect
/// each, once per migration.
pub fn recycle_stale_connections(options: PgPoolOptions) -> PgPoolOptions {
    options.before_acquire(|_conn, meta| {
        Box::pin(async move { Ok(!opened_before_stale_plan(meta.age)) })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    /// A database error with a chosen SQLSTATE and message.
    #[derive(Debug)]
    struct FakeDbError {
        code: &'static str,
        message: &'static str,
    }

    impl fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.message)
        }
    }

    impl StdError for FakeDbError {}

    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            self.message
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }
        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn db_error(code: &'static str, message: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError { code, message }))
    }

    #[test]
    fn only_the_stale_plan_refusal_is_a_stale_plan() {
        assert!(is_stale_plan(&db_error(
            "0A000",
            "cached plan must not change result type"
        )));
        // The same SQLSTATE for anything else is not one.
        assert!(!is_stale_plan(&db_error(
            "0A000",
            "cannot use subquery in DEFAULT expression"
        )));
        // Nor the same words under another code.
        assert!(!is_stale_plan(&db_error(
            "XX000",
            "cached plan must not change result type"
        )));
        assert!(!is_stale_plan(&sqlx::Error::RowNotFound));
    }

    fn stale() -> sqlx::Error {
        db_error("0A000", "cached plan must not change result type")
    }

    /// A stand-in connection that refuses its statements, as Postgres does,
    /// until they are cleared — `refusals` times over.
    struct Conn {
        refusals: u32,
        runs: u32,
        cleared: u32,
    }

    impl Conn {
        fn refusing(refusals: u32) -> Self {
            Self {
                refusals,
                runs: 0,
                cleared: 0,
            }
        }

        async fn run(&mut self) -> Result<u32, sqlx::Error> {
            self.runs += 1;
            if self.cleared < self.refusals {
                Err(stale())
            } else {
                Ok(self.runs)
            }
        }

        async fn fail(&mut self) -> Result<u32, sqlx::Error> {
            self.runs += 1;
            Err(db_error(
                "23505",
                "duplicate key value violates unique constraint",
            ))
        }

        async fn clear_cached_statements(&mut self) -> Result<(), sqlx::Error> {
            self.cleared += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_stale_plan_is_cleared_and_retried_once() {
        let mut conn = Conn::refusing(1);
        let result = retry_on_stale_plan!("test", conn => conn.run().await);
        assert_eq!(result.unwrap(), 2);
        assert_eq!((conn.runs, conn.cleared), (2, 1));
    }

    #[tokio::test]
    async fn a_second_refusal_is_returned_not_retried_again() {
        let mut conn = Conn::refusing(2);
        let result = retry_on_stale_plan!("test", conn => conn.run().await);
        assert!(is_stale_plan(&result.unwrap_err()));
        assert_eq!((conn.runs, conn.cleared), (2, 1));
    }

    #[tokio::test]
    async fn any_other_error_is_returned_as_it_is() {
        let mut conn = Conn::refusing(0);
        let result = retry_on_stale_plan!("test", conn => conn.fail().await);
        assert!(!is_stale_plan(&result.unwrap_err()));
        assert_eq!((conn.runs, conn.cleared), (1, 0));
    }

    #[tokio::test]
    async fn without_a_refusal_it_runs_once() {
        let mut conn = Conn::refusing(0);
        let result = retry_on_stale_plan!("test", conn => conn.run().await);
        assert_eq!(result.unwrap(), 1);
        assert_eq!((conn.runs, conn.cleared), (1, 0));
    }

    #[test]
    fn a_connection_older_than_the_last_stale_plan_is_recycled() {
        // Nothing seen yet in this test's view of the clock: pin a sighting
        // now, then judge connections either side of it.
        clock_origin();
        std::thread::sleep(Duration::from_millis(5));
        note_stale_plan("test");
        let since_sighting = Duration::from_millis(1);
        std::thread::sleep(Duration::from_millis(5));
        // Opened well before the sighting: recycled.
        assert!(opened_before_stale_plan(Duration::from_secs(3600)));
        // Opened after it (younger than the time since): kept.
        assert!(!opened_before_stale_plan(since_sighting));
    }
    /// The test database, as the other database-backed tests find it.
    fn database_url() -> String {
        std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        })
    }

    /// A statement kept from before a column was added is refused without the
    /// retry, and cleared and run again with it — against a real Postgres, on
    /// a table of the test's own.
    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn a_statement_kept_from_before_a_column_was_added_is_retried() {
        use sqlx::{Connection, Row};

        let mut conn = sqlx::PgConnection::connect(&database_url())
            .await
            .expect("connect to the test database");
        let table = format!("stale_plan_{}", uuid::Uuid::new_v4().simple());
        sqlx::raw_sql(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY); INSERT INTO {table} VALUES (1)"
        ))
        .execute(&mut conn)
        .await
        .expect("create the table");

        // The failure, on purpose. sql-columns: allow
        let whole_row = format!("SELECT * FROM {table}");
        sqlx::query(&whole_row)
            .fetch_all(&mut conn)
            .await
            .expect("read before the column is added");
        sqlx::raw_sql(&format!("ALTER TABLE {table} ADD COLUMN added TEXT"))
            .execute(&mut conn)
            .await
            .expect("add a column");

        let refused = sqlx::query(&whole_row)
            .fetch_all(&mut conn)
            .await
            .expect_err("the kept statement was not refused");
        assert!(
            is_stale_plan(&refused),
            "refused for another reason: {refused}"
        );

        let rows = retry_on_stale_plan!("test", conn => {
            sqlx::query(&whole_row).fetch_all(&mut conn).await
        })
        .expect("the retry did not recover the statement");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns().len(), 2, "the retry read the new shape");

        sqlx::raw_sql(&format!("DROP TABLE {table}"))
            .execute(&mut conn)
            .await
            .expect("drop the table");
    }

    /// Once a stale plan has been seen, a pool set up with
    /// [`recycle_stale_connections`] hands out a new connection in place of
    /// one it opened before.
    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn a_pool_replaces_a_connection_older_than_a_stale_plan() {
        let pool = recycle_stale_connections(PgPoolOptions::new())
            .max_connections(1)
            .connect(&database_url())
            .await
            .expect("connect to the test database");
        let backend = || async {
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                .fetch_one(&pool)
                .await
                .expect("read the backend")
        };
        let before = backend().await;
        assert_eq!(
            backend().await,
            before,
            "an unaffected connection was replaced"
        );

        std::thread::sleep(Duration::from_millis(5));
        note_stale_plan("test");
        let after = backend().await;
        assert_ne!(
            after, before,
            "a connection older than the stale plan was handed out"
        );
        assert_eq!(backend().await, after, "a new connection was replaced too");
    }
}
