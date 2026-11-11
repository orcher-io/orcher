//! Actor state belongs to the organization that wrote it.
//!
//! The storage builds each row key from the organization in scope, but the
//! state RPCs never scoped one, so every caller wrote and read the unscoped
//! key: two organizations running the same actor with the same instance key
//! shared one row, each overwriting and reading the other's state.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test actor_state_tenancy -- --ignored
//! ```

use orcher_core::actor::{ActorRouter, ActorStateStorage, ServiceRegistry};
use orcher_core::grpc::services::ActorServiceImpl;
use orcher_proto::orcher::v1::actor_service_server::ActorService;
use orcher_proto::orcher::v1::{
    DeleteStateRequest, GetStateRequest, ListStateKeysRequest, SetStateRequest,
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const INSTANCE: &str = "account-1";
const STATE_KEY: &str = "balance";

async fn service() -> (PgPool, ActorServiceImpl) {
    let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect to the test database");
    static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    MIGRATED
        .get_or_init(|| async {
            orcher_storage::setup::run_migrations(&pool)
                .await
                .expect("apply migrations");
        })
        .await;
    let service = ActorServiceImpl::new(
        Arc::new(ActorRouter::new()),
        Arc::new(ActorStateStorage::new(pool.clone())),
        Arc::new(ServiceRegistry::new()),
    );
    (pool, service)
}

/// A request as the auth layer leaves it: the caller's organization, if it
/// has one, in the metadata.
fn from<T>(message: T, organization: Option<Uuid>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(organization) = organization {
        orcher_core::grpc::tenant::set_organization(&mut request, &organization);
    }
    request
}

async fn set(
    service: &ActorServiceImpl,
    actor: &str,
    organization: Option<Uuid>,
    value: &[u8],
    expected_version: &str,
) -> String {
    let response = service
        .set_state(from(
            SetStateRequest {
                actor_name: actor.to_string(),
                key: INSTANCE.to_string(),
                state_key: STATE_KEY.to_string(),
                value: value.to_vec(),
                expected_version: expected_version.to_string(),
                ..Default::default()
            },
            organization,
        ))
        .await
        .expect("set state")
        .into_inner();
    assert!(response.success, "{}", response.error_message);
    response.version
}

async fn get(
    service: &ActorServiceImpl,
    actor: &str,
    organization: Option<Uuid>,
) -> Option<(Vec<u8>, String)> {
    let response = service
        .get_state(from(
            GetStateRequest {
                actor_name: actor.to_string(),
                key: INSTANCE.to_string(),
                state_key: STATE_KEY.to_string(),
                ..Default::default()
            },
            organization,
        ))
        .await
        .expect("get state")
        .into_inner();
    response
        .exists
        .then_some((response.value, response.version))
}

async fn delete(service: &ActorServiceImpl, actor: &str, organization: Option<Uuid>) -> bool {
    service
        .delete_state(from(
            DeleteStateRequest {
                actor_name: actor.to_string(),
                key: INSTANCE.to_string(),
                state_key: STATE_KEY.to_string(),
                ..Default::default()
            },
            organization,
        ))
        .await
        .expect("delete state")
        .into_inner()
        .existed
}

async fn cleanup(pool: &PgPool, actor: &str) {
    let _ = sqlx::query("DELETE FROM actor_state WHERE key LIKE '%actor:' || $1 || ':%'")
        .bind(actor)
        .execute(pool)
        .await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn two_organizations_with_the_same_actor_and_key_have_separate_state() {
    let (pool, service) = service().await;
    let actor = format!("Ledger-{}", Uuid::new_v4());
    let (a, b) = (Some(Uuid::new_v4()), Some(Uuid::new_v4()));

    set(&service, &actor, a, b"a:10", "").await;
    set(&service, &actor, b, b"b:99", "").await;

    let (value, version) = get(&service, &actor, a).await.expect("a's state");
    assert_eq!(value, b"a:10", "a reads its own state, not b's write");
    assert_eq!(
        get(&service, &actor, b).await.expect("b's state").0,
        b"b:99"
    );

    // A versioned write against the version a read is a write to a's row.
    set(&service, &actor, a, b"a:20", &version).await;
    assert_eq!(
        get(&service, &actor, a).await.expect("a's state").0,
        b"a:20"
    );
    assert_eq!(
        get(&service, &actor, b).await.expect("b's state").0,
        b"b:99"
    );

    // Each lists only its own key.
    for organization in [a, b] {
        let keys = service
            .list_state_keys(from(
                ListStateKeysRequest {
                    actor_name: actor.clone(),
                    key: INSTANCE.to_string(),
                    ..Default::default()
                },
                organization,
            ))
            .await
            .expect("list keys")
            .into_inner()
            .keys;
        assert_eq!(keys, vec![STATE_KEY.to_string()], "{organization:?}");
    }

    assert!(delete(&service, &actor, a).await, "a deletes its own state");
    assert!(get(&service, &actor, a).await.is_none());
    assert_eq!(
        get(&service, &actor, b)
            .await
            .expect("b's state survives")
            .0,
        b"b:99"
    );

    cleanup(&pool, &actor).await;
}

/// A caller with no organization — a single-tenant deployment — reads and
/// writes the unscoped key it always did, and an organization does not fall
/// back to it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_caller_without_an_organization_keeps_the_unscoped_key() {
    let (pool, service) = service().await;
    let actor = format!("Ledger-{}", Uuid::new_v4());

    set(&service, &actor, None, b"single", "").await;
    let stored: Vec<String> = sqlx::query_scalar("SELECT key FROM actor_state WHERE key LIKE $1")
        .bind(format!("%actor:{actor}:%"))
        .fetch_all(&pool)
        .await
        .expect("read keys");
    assert_eq!(
        stored,
        vec![format!("actor:{actor}:{INSTANCE}:{STATE_KEY}")]
    );
    assert_eq!(
        get(&service, &actor, None).await.expect("state").0,
        b"single"
    );

    assert!(
        get(&service, &actor, Some(Uuid::new_v4())).await.is_none(),
        "an organization does not read unscoped state"
    );

    cleanup(&pool, &actor).await;
}

async fn list(
    service: &ActorServiceImpl,
    actor: &str,
    instance: &str,
    org: Option<Uuid>,
) -> Vec<String> {
    service
        .list_state_keys(from(
            ListStateKeysRequest {
                actor_name: actor.to_string(),
                key: instance.to_string(),
                ..Default::default()
            },
            org,
        ))
        .await
        .expect("list keys")
        .into_inner()
        .keys
}

/// A listed key is the state key a caller uses: passed back to GetState and
/// DeleteState, as SDKs do to clear an actor's state, it addresses the row.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_listed_key_can_be_read_and_deleted() {
    let (pool, service) = service().await;
    let actor = format!("Ledger-{}", Uuid::new_v4());

    for organization in [Some(Uuid::new_v4()), None] {
        set(&service, &actor, organization, b"10", "").await;
        let keys = list(&service, &actor, INSTANCE, organization).await;
        assert_eq!(keys, vec![STATE_KEY.to_string()], "{organization:?}");

        let found = service
            .get_state(from(
                GetStateRequest {
                    actor_name: actor.clone(),
                    key: INSTANCE.to_string(),
                    state_key: keys[0].clone(),
                    ..Default::default()
                },
                organization,
            ))
            .await
            .expect("get state")
            .into_inner();
        assert!(found.exists, "a listed key is found");

        let deleted = service
            .delete_state(from(
                DeleteStateRequest {
                    actor_name: actor.clone(),
                    key: INSTANCE.to_string(),
                    state_key: keys[0].clone(),
                    ..Default::default()
                },
                organization,
            ))
            .await
            .expect("delete state")
            .into_inner();
        assert!(deleted.existed, "a listed key is deleted");
        assert!(list(&service, &actor, INSTANCE, organization)
            .await
            .is_empty());
    }

    cleanup(&pool, &actor).await;
}

/// `_` and `%` in an instance key are the characters they are, not
/// wildcards: `user_1` lists and deletes only its own state, not `userZ1`'s.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn instance_keys_are_matched_literally() {
    let (pool, service) = service().await;
    let actor = format!("Ledger-{}", Uuid::new_v4());
    let storage = ActorStateStorage::new(pool.clone());

    for instance in ["user_1", "userZ1", "%"] {
        storage
            .set_state(&actor, instance, "balance", instance.as_bytes())
            .await
            .expect("set state");
    }

    for instance in ["user_1", "userZ1", "%"] {
        assert_eq!(
            list(&service, &actor, instance, None).await,
            vec!["balance".to_string()],
            "{instance} lists only its own key"
        );
    }

    assert_eq!(
        storage
            .delete_all_state(None, &actor, "user_1")
            .await
            .expect("delete all"),
        1,
        "only user_1's state is deleted"
    );
    assert_eq!(
        storage
            .delete_all_state(None, &actor, "%")
            .await
            .expect("delete all"),
        1,
        "an instance named % deletes only its own state"
    );
    assert_eq!(
        list(&service, &actor, "userZ1", None).await,
        vec!["balance".to_string()],
        "userZ1's state survives"
    );

    cleanup(&pool, &actor).await;
}
