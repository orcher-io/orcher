//! Middleware for the ORCHER API gateway.
//!
//! Only the middleware actually wired into the router lives here: `auth`
//! (request authentication — NoOp in OSS mode, real identity in cloud) and
//! `versioning` (API version negotiation). Compression and CORS are applied
//! directly via `tower-http` layers in `server.rs`.

pub mod auth;
pub mod versioning;

pub use auth::{AuthError, AuthMethod, AuthenticatedUser};
pub use versioning::{versioning_middleware, VersioningConfig, VersioningMiddleware};
