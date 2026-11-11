//! Database-backed tests for [`ClusterMembershipRepository`] — the membership +
//! shard-fencing substrate (V028). These prove the actual repository methods, not
//! hand-written SQL, uphold the fencing invariants:
//!
//! 1. Fresh acquire starts the generation at 1.
//! 2. A **live** other owner fences out a competing acquire (the split-brain net).
//! 3. A **stale** owner is reclaimed, bumping the generation monotonically.
//! 4. Renew at the held generation keeps ownership without bumping it.
//! 5. Renew/release at the wrong generation is rejected (fenced out).
//! 6. `live_members` excludes stale + non-active nodes.
//!
//! ## Running
//! ```bash
//! DATABASE_URL="postgresql://localhost/orcher_cluster_test" \
//!   cargo test -p orcher-storage cluster::membership_tests -- --ignored --nocapture
//! ```
//! The target DB must have migration V028 applied.

use super::membership::{begin_shard_guarded_tx, ClusterMembershipRepository};
use serial_test::serial;
use sqlx::PgPool;

async fn test_repo() -> ClusterMembershipRepository {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/orcher_cluster_test".to_string());
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to test database (V028 applied)");
    // Isolate each test run.
    sqlx::query("TRUNCATE cluster_members, cluster_shards")
        .execute(&pool)
        .await
        .expect("truncate");
    ClusterMembershipRepository::new(pool)
}

const STALE_SECS: i64 = 30;

#[tokio::test]
#[ignore] // Requires database with V028
#[serial]
async fn shard_guard_commits_on_match_and_rejects_when_superseded() {
    let repo = test_repo().await;
    let pool = repo.pool().clone();
    repo.upsert_member("owner", "o:1").await.unwrap();

    // Acquire shard 3 → generation 1; that's the token we "hold".
    let held = repo
        .acquire_shard(3, "owner", STALE_SECS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held, 1);

    // Guard with the matching generation → we get a usable tx; a write in it commits.
    let mut tx = begin_shard_guarded_tx(&pool, 3, held)
        .await
        .expect("guard opens at held gen");
    sqlx::query("UPDATE cluster_shards SET updated_at = now() WHERE shard_id = 3")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit()
        .await
        .expect("guarded write commits while we hold the shard");

    // A new owner supersedes us (generation bumps to 2)...
    repo.upsert_member("other", "x:1").await.unwrap();
    // force takeover by expiring our heartbeat
    sqlx::query("UPDATE cluster_members SET heartbeat_at = now() - interval '120 seconds' WHERE node_id='owner'")
        .execute(&pool).await.unwrap();
    let newer = repo
        .acquire_shard(3, "other", STALE_SECS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newer, 2);

    // ...so a guard with our STALE generation 1 is rejected — no tx, nothing written.
    let before = repo.get_shard(3).await.unwrap().unwrap().updated_at;
    let err = begin_shard_guarded_tx(&pool, 3, held).await;
    assert!(err.is_err(), "guard must reject a superseded generation");
    let after = repo.get_shard(3).await.unwrap().unwrap().updated_at;
    assert_eq!(
        before, after,
        "a rejected guard must not have written anything"
    );
}

#[tokio::test]
#[ignore] // Requires database with V028
#[serial]
async fn fencing_invariants_hold() {
    let repo = test_repo().await;

    // Two live nodes register; heartbeats are fresh.
    repo.upsert_member("node-a", "a:1").await.unwrap();
    repo.upsert_member("node-b", "b:1").await.unwrap();

    // 1. Fresh acquire → generation 1.
    let g = repo.acquire_shard(7, "node-a", STALE_SECS).await.unwrap();
    assert_eq!(g, Some(1), "fresh acquire starts at generation 1");

    // 2. A live other owner fences out node-b (no takeover, no bump).
    let g = repo.acquire_shard(7, "node-b", STALE_SECS).await.unwrap();
    assert_eq!(g, None, "live owner must fence out a competing acquire");
    assert_eq!(
        repo.get_shard(7)
            .await
            .unwrap()
            .unwrap()
            .owner_node
            .as_deref(),
        Some("node-a")
    );

    // 3. Re-acquire by the same owner bumps its generation (returns the new token).
    let g = repo.acquire_shard(7, "node-a", STALE_SECS).await.unwrap();
    assert_eq!(g, Some(2), "self re-acquire bumps the generation");

    // 4. Renew at the held generation keeps ownership, no bump.
    assert!(
        repo.renew_shard(7, "node-a", 2).await.unwrap(),
        "renew at held gen succeeds"
    );
    assert_eq!(
        repo.get_shard(7).await.unwrap().unwrap().generation,
        2,
        "renew does not bump"
    );

    // 5. Renew/release at the wrong generation is fenced out.
    assert!(
        !repo.renew_shard(7, "node-a", 99).await.unwrap(),
        "renew at wrong gen is rejected"
    );
    assert!(
        !repo.release_shard(7, "node-a", 99).await.unwrap(),
        "release at wrong gen is rejected"
    );
    assert_eq!(
        repo.get_shard(7)
            .await
            .unwrap()
            .unwrap()
            .owner_node
            .as_deref(),
        Some("node-a")
    );
}

#[tokio::test]
#[ignore] // Requires database with V028
#[serial]
async fn stale_owner_is_reclaimed() {
    let repo = test_repo().await;
    repo.upsert_member("node-b", "b:1").await.unwrap();
    // node-c registered but its heartbeat is ancient → stale.
    repo.upsert_member("node-c", "c:1").await.unwrap();
    sqlx::query("UPDATE cluster_members SET heartbeat_at = now() - interval '120 seconds' WHERE node_id = 'node-c'")
        .execute(repo.pool()).await.unwrap();

    // node-c owns shard 9...
    assert_eq!(
        repo.acquire_shard(9, "node-c", STALE_SECS).await.unwrap(),
        Some(1)
    );
    // ...but it's stale, so node-b reclaims it, bumping the generation.
    assert_eq!(
        repo.acquire_shard(9, "node-b", STALE_SECS).await.unwrap(),
        Some(2),
        "a stale owner's shard is reclaimable, generation bumps"
    );
    assert_eq!(
        repo.get_shard(9)
            .await
            .unwrap()
            .unwrap()
            .owner_node
            .as_deref(),
        Some("node-b")
    );
}

#[tokio::test]
#[ignore] // Requires database with V028
#[serial]
async fn live_members_excludes_stale_and_inactive() {
    let repo = test_repo().await;
    repo.upsert_member("live", "l:1").await.unwrap();
    repo.upsert_member("stale", "s:1").await.unwrap();
    repo.upsert_member("draining", "d:1").await.unwrap();
    sqlx::query("UPDATE cluster_members SET heartbeat_at = now() - interval '120 seconds' WHERE node_id = 'stale'")
        .execute(repo.pool()).await.unwrap();
    repo.set_member_status("draining", "draining")
        .await
        .unwrap();

    let live: Vec<String> = repo
        .live_members(STALE_SECS)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.node_id)
        .collect();
    assert_eq!(
        live,
        vec!["live".to_string()],
        "only the fresh, active node is live"
    );
}

#[tokio::test]
#[ignore] // Requires database with V028
#[serial]
async fn shards_owned_by_lists_only_this_nodes_shards() {
    let repo = test_repo().await;
    repo.upsert_member("n1", "n1:1").await.unwrap();
    for s in [3, 1, 2] {
        repo.acquire_shard(s, "n1", STALE_SECS).await.unwrap();
    }
    repo.upsert_member("n2", "n2:1").await.unwrap();
    repo.acquire_shard(50, "n2", STALE_SECS).await.unwrap();

    assert_eq!(
        repo.shards_owned_by("n1").await.unwrap(),
        vec![1, 2, 3],
        "sorted, only n1's shards"
    );
    assert_eq!(repo.shards_owned_by("n2").await.unwrap(), vec![50]);
}
