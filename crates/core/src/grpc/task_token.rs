//! The token a worker hands back to say which task it is reporting on.
//!
//! The token names a task. For most of this engine's life that is all it did:
//! the format was `workflow_id:execution_id:task_id:sequence:attempt:started_ms`
//! in plain text, unsigned, and the completion RPCs took it as sufficient
//! authority to complete, fail or cancel the task it named. Those three ids are
//! not secret — `GetTaskExecutions` returns all of them in one call — so
//! anybody who could read them could write a token by hand and finish another
//! tenant's task with a result of their choosing.
//!
//! The ownership check on the task row is what closes that, and it does not
//! depend on anything here: the row carries its own `organization_id` and is
//! the authority. Signing is the second lock. It stops a forged token being
//! resolved at all, rather than resolving it and then refusing, and it carries
//! the organisation inside the signed material so the two can be compared.
//!
//! # Format
//!
//! `v2.<base64url(payload)>.<base64url(signature)>`
//!
//! The payload is the same colon-joined field list as before with the
//! organisation appended, base64url-encoded so that a field containing the
//! delimiter cannot change how the token parses. The old format split on `:`
//! and took the first three fields positionally, which quietly misread any
//! id containing a colon.
//!
//! # Compatibility
//!
//! Tokens are handed out at poll time and handed back at completion, and those
//! two moments can be minutes apart and land on different replicas. A rolling
//! deploy therefore always has both formats in flight, so [`decode`] accepts
//! either and `raw` keeps the exact bytes the client sent — the completion path
//! looks the task up by string equality against the copy stored when it was
//! polled, so `raw` must not be normalised.
//!
//! # The key
//!
//! Every replica must derive the same signature or a token polled from one and
//! completed against another fails to verify. There is no config distribution
//! here, so the key comes from `ORCHER_TASK_TOKEN_KEY` and has to be injected
//! identically everywhere — the same requirement, and the same failure mode, as
//! `JWT_SECRET`.
//!
//! Without a key the engine issues the old unsigned format and accepts both.
//! That is the state every deployment is in today, so it is the default rather
//! than an error: making the key mandatory immediately would mean an engine
//! that refuses to start where one had always worked.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use orcher_common::types::time::Timestamptz;
use sha2::Sha256;
use std::sync::OnceLock;
use tonic::Status;
use tracing::warn;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Marks a token as carrying a signature. Anything without it is read as the
/// pre-signing format.
const V2_PREFIX: &str = "v2.";

/// Environment variable holding the signing key.
const KEY_ENV: &str = "ORCHER_TASK_TOKEN_KEY";

/// The key this process signs and verifies with, resolved once.
///
/// `None` means no key is configured, which is not an error: the engine then
/// issues unsigned tokens, exactly as it did before signing existed.
pub(crate) fn signing_key() -> Option<&'static [u8]> {
    static KEY: OnceLock<Option<Vec<u8>>> = OnceLock::new();

    KEY.get_or_init(|| match std::env::var(KEY_ENV) {
        Ok(key) if !key.trim().is_empty() => Some(key.into_bytes()),
        _ => {
            warn!(
                "{KEY_ENV} is not set — task tokens will be issued unsigned. Set it (identically \
                 on every replica) to have the engine sign them."
            );
            None
        }
    })
    .as_deref()
}

/// The fields a token carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskToken {
    /// Exactly the bytes the client sent, for the completion path's lookup by
    /// string equality against the copy stored at poll time. Never normalised.
    pub raw: String,
    /// Internal id of the workflow execution the task belongs to.
    pub workflow_id: String,
    /// External-facing execution id.
    pub execution_id: String,
    /// Id of the task within the execution.
    pub task_id: String,
    /// Which attempt this token was issued for. 1 for the first.
    pub attempt: i32,
    /// When the task was handed to a worker, if the token recorded it.
    pub started_at: Option<Timestamptz>,
    /// The organisation the task belonged to when it was polled.
    ///
    /// `None` for a token issued before the field existed, or by a deployment
    /// running without tenancy. Present and signed, it lets the completion path
    /// notice a token whose organisation disagrees with the row it resolved to.
    pub organization_id: Option<Uuid>,
}

/// The fields needed to mint a token.
pub struct NewTaskToken<'a> {
    /// Internal id of the workflow execution the task belongs to.
    pub workflow_id: &'a str,
    /// External-facing execution id.
    pub execution_id: &'a str,
    /// Id of the task within the execution.
    pub task_id: &'a str,
    /// Which attempt is being dispatched. 1 for the first.
    pub attempt: i32,
    /// Dispatch time in milliseconds since the epoch.
    pub started_at_ms: i64,
    /// Organisation the task belongs to, signed into the token.
    pub organization_id: Option<Uuid>,
}

pub(crate) fn sign(key: &[u8], payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(payload.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// The signed material: the field list, in order, with the organisation last.
///
/// `sequence` is always 0 and has been since the format was introduced — it is
/// kept so the field positions do not move, because a token minted by an older
/// replica has to stay readable by a newer one during a rolling deploy.
fn payload_of(token: &NewTaskToken<'_>) -> String {
    format!(
        "{}:{}:{}:0:{}:{}:{}",
        token.workflow_id,
        token.execution_id,
        token.task_id,
        token.attempt,
        token.started_at_ms,
        token
            .organization_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
    )
}

/// Mint a token for a task being handed to a worker.
///
/// This is the only place the format is written. It used to be three separate
/// `format!` calls in the poll paths, which is three places to keep in step
/// with one parser.
#[must_use]
pub fn encode(token: &NewTaskToken<'_>) -> String {
    let payload = payload_of(token);

    let Some(key) = signing_key() else {
        // No key: the pre-signing format, so a replica that has not been given
        // the key yet can still read what this one issues.
        return format!(
            "{}:{}:{}:0:{}:{}",
            token.workflow_id,
            token.execution_id,
            token.task_id,
            token.attempt,
            token.started_at_ms,
        );
    };

    let signature = sign(key, &payload);
    format!(
        "{V2_PREFIX}{}.{}",
        URL_SAFE_NO_PAD.encode(payload.as_bytes()),
        signature
    )
}

/// Read a token, verifying it if it carries a signature.
///
/// Accepts both formats on purpose; see the compatibility note in the module
/// docs. A signed token whose signature does not check out is refused outright
/// — that is the case this exists for.
#[allow(clippy::result_large_err)] // tonic::Status is large; every RPC here returns one.
pub fn decode(token: &[u8]) -> Result<TaskToken, Status> {
    if token.is_empty() {
        return Err(Status::invalid_argument("task_token is required"));
    }

    let raw = String::from_utf8(token.to_vec())
        .map_err(|e| Status::invalid_argument(format!("Invalid task_token: {e}")))?;

    if let Some(rest) = raw.strip_prefix(V2_PREFIX) {
        return decode_signed(&raw, rest);
    }

    decode_legacy(raw)
}

#[allow(clippy::result_large_err)]
fn decode_signed(raw: &str, rest: &str) -> Result<TaskToken, Status> {
    let (payload_b64, signature) = rest
        .split_once('.')
        .ok_or_else(|| Status::invalid_argument("Malformed task_token"))?;

    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or_else(|| Status::invalid_argument("Malformed task_token"))?;

    match signing_key() {
        Some(key) => {
            // Constant-time: a byte-at-a-time comparison leaks how much of a
            // guessed signature was right, which is enough to build one.
            let expected = sign(key, &payload);
            if !bool::from(constant_time_eq(expected.as_bytes(), signature.as_bytes())) {
                return Err(Status::permission_denied("Invalid task_token signature"));
            }
        }
        None => {
            // A signed token arriving at a replica with no key. Refusing would
            // strand every task polled from a replica that has one, which is
            // the normal state halfway through a rollout, so it is read without
            // verification and said out loud.
            warn!(
                "Received a signed task_token but {KEY_ENV} is not set on this replica — accepting \
                 it unverified. Set the key identically on every replica."
            );
        }
    }

    let mut fields = payload.split(':');
    let workflow_id = fields.next().unwrap_or_default().to_string();
    let execution_id = fields.next().unwrap_or_default().to_string();
    let task_id = fields.next().unwrap_or_default().to_string();
    let _sequence = fields.next();
    let attempt = fields.next().and_then(|v| v.parse().ok()).unwrap_or(1);
    let started_at = fields
        .next()
        .and_then(|v| v.parse::<i64>().ok())
        .and_then(Timestamptz::from_millisecond);
    let organization_id = fields.next().and_then(|v| Uuid::parse_str(v).ok());

    if workflow_id.is_empty() || execution_id.is_empty() || task_id.is_empty() {
        return Err(Status::invalid_argument("Malformed task_token"));
    }

    Ok(TaskToken {
        raw: raw.to_string(),
        workflow_id,
        execution_id,
        task_id,
        attempt,
        started_at,
        organization_id,
    })
}

#[allow(clippy::result_large_err)]
fn decode_legacy(raw: String) -> Result<TaskToken, Status> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() < 3 {
        return Err(Status::invalid_argument(
            "task_token must have format: workflow_id:execution_id:task_id[:sequence[:attempt[:started_at_ms]]]",
        ));
    }

    let attempt = if parts.len() > 4 {
        parts[4].parse::<i32>().unwrap_or(1)
    } else {
        1
    };

    let started_at = parts
        .get(5)
        .and_then(|v| v.parse::<i64>().ok())
        .and_then(Timestamptz::from_millisecond);

    Ok(TaskToken {
        workflow_id: parts[0].to_string(),
        execution_id: parts[1].to_string(),
        task_id: parts[2].to_string(),
        attempt,
        started_at,
        organization_id: None,
        raw,
    })
}

/// Compare two byte strings without returning early on the first difference.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> subtle::Choice {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return subtle::Choice::from(0);
    }
    a.ct_eq(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(org: Option<Uuid>) -> NewTaskToken<'static> {
        NewTaskToken {
            workflow_id: "wf-123",
            execution_id: "exec-456",
            task_id: "task-789",
            attempt: 3,
            started_at_ms: 1_700_000_000_000,
            organization_id: org,
        }
    }

    /// The pre-signing format, which a rolling deploy always has in flight.
    #[test]
    fn a_legacy_token_still_reads() {
        let token = decode(b"wf-123:exec-456:task-789:0:3:1700000000000").expect("decode");

        assert_eq!(token.workflow_id, "wf-123");
        assert_eq!(token.execution_id, "exec-456");
        assert_eq!(token.task_id, "task-789");
        assert_eq!(token.attempt, 3);
        assert_eq!(token.organization_id, None);
    }

    /// `raw` is the lookup key against the copy stored at poll time, so it has
    /// to come back exactly as it went in.
    #[test]
    fn raw_is_the_bytes_the_caller_sent() {
        let sent = "wf-1:exec-2:task-3";
        assert_eq!(decode(sent.as_bytes()).expect("decode").raw, sent);
    }

    #[test]
    fn a_token_without_the_required_fields_is_refused() {
        assert!(decode(b"").is_err());
        assert!(decode(b"wf-1:exec-2").is_err());
    }

    /// Round-trip through whichever format this process is configured for.
    #[test]
    fn a_minted_token_reads_back() {
        let org = Uuid::new_v4();
        let minted = encode(&parts(Some(org)));
        let token = decode(minted.as_bytes()).expect("decode");

        assert_eq!(token.workflow_id, "wf-123");
        assert_eq!(token.execution_id, "exec-456");
        assert_eq!(token.task_id, "task-789");
        assert_eq!(token.attempt, 3);
        assert_eq!(token.raw, minted);
    }

    /// The signed path, exercised without depending on process environment:
    /// the key is read once per process, so a test that set it would be
    /// order-dependent against every other test in this binary.
    #[test]
    fn a_tampered_payload_does_not_verify() {
        let key = b"test-key";
        let payload = payload_of(&parts(None));
        let signature = sign(key, &payload);

        assert_ne!(
            signature,
            sign(key, &format!("{payload}-tampered")),
            "a changed payload produced the same signature"
        );
        assert!(bool::from(constant_time_eq(
            signature.as_bytes(),
            sign(key, &payload).as_bytes()
        )));
    }

    #[test]
    fn a_different_key_does_not_verify() {
        let payload = payload_of(&parts(None));

        assert_ne!(sign(b"key-one", &payload), sign(b"key-two", &payload));
    }

    /// The organisation is inside the signed material, not alongside it, so it
    /// cannot be swapped without invalidating the signature.
    #[test]
    fn the_organisation_is_part_of_what_is_signed() {
        let key = b"test-key";
        let alpha = payload_of(&parts(Some(Uuid::new_v4())));
        let beta = payload_of(&parts(Some(Uuid::new_v4())));

        assert_ne!(sign(key, &alpha), sign(key, &beta));
    }

    #[test]
    fn a_signed_token_that_is_truncated_is_refused() {
        assert!(decode(b"v2.only-one-part").is_err());
        assert!(decode(b"v2.!!!not-base64!!!.sig").is_err());
    }
}
