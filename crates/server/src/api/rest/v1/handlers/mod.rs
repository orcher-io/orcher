//! REST API v1 Request Handlers — core orchestration endpoints.
//!
//! Account handlers (auth, users, orgs, API keys, audit) are in `orcher-account`.

/// Maximum number of rows any list endpoint will return for a single request.
///
/// List handlers clamp a client-supplied `limit` to this ceiling so an
/// unauthenticated caller cannot request an unbounded result set and exhaust
/// server/database resources. Kept as `i64` (the widest list `limit` type);
/// handlers cast to their own field type at the clamp site.
pub(crate) const MAX_PAGE_SIZE: i64 = 1000;

pub mod api_keys;
pub mod batch_operations;
pub mod health;
pub mod namespaces;
pub mod registry;
pub mod releases;
pub mod schedules;
pub mod system;
pub mod version;
pub mod workers;
pub mod workflows;
