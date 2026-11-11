//! The token that ties a workflow completion to the activation it answers.
//!
//! Every claim that hands a workflow to a worker numbers the activation
//! (`workflow_executions.activation_seq`). The worker gets that number back
//! inside this token and returns it with its completion, and the completion is
//! applied only if the number is still the current one. Without it a completion
//! named its workflow and nothing else, so the same completion sent twice was
//! applied twice, and one from a worker whose claim had lapsed was applied over
//! the activation handed to the worker after it.
//!
//! # Where it travels
//!
//! `PollWorkflowExecutionResponse` has no task token field, and every SDK makes
//! one up from the workflow and execution ids. The one field that already makes
//! the round trip from poll to completion untouched is `stream_entry_id`: every
//! SDK copies it from the poll response into `CompleteWorkflowExecutionRequest`
//! verbatim. It was the acknowledgement id of a Redis stream this engine no
//! longer reads from, and nothing on either side interprets it, so the token
//! rides there and existing workers carry it without a change. A completion may
//! also carry it in `task_token`, which is where an SDK that knows about it
//! should put it, and the only place a `FailWorkflowExecutionRequest` can.
//!
//! # Format
//!
//! `act1.<base64url(execution_id:activation_seq)>`, followed by
//! `.<base64url(signature)>` when the engine has a signing key — and then a
//! token without one is refused. The key and the
//! signature scheme are the task token's (`ORCHER_TASK_TOKEN_KEY`), over a
//! payload prefixed so that neither kind of token can be passed off as the
//! other. Anything without the prefix is not an activation token — that is how
//! the tokens SDKs make up today are recognised and let through.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use tonic::Status;
use tracing::warn;

use crate::grpc::task_token::{constant_time_eq, sign, signing_key};

/// Marks a string as an activation token.
const PREFIX: &str = "act1.";

/// Prepended to the signed material, so a signature made for a task token
/// never verifies as one made for an activation token.
const SIGNING_DOMAIN: &str = "activation:";

/// What an activation token says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationToken {
    /// External execution id the activation belongs to.
    pub execution_id: String,
    /// The number the claim gave the activation.
    pub activation_seq: i64,
}

/// Mint the token for an activation being handed to a worker.
#[must_use]
pub fn encode(execution_id: &str, activation_seq: i64) -> String {
    let payload = format!("{execution_id}:{activation_seq}");
    let encoded = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    match signing_key() {
        Some(key) => format!(
            "{PREFIX}{encoded}.{}",
            sign(key, &format!("{SIGNING_DOMAIN}{payload}"))
        ),
        None => format!("{PREFIX}{encoded}"),
    }
}

/// Read an activation token.
///
/// `None` when the bytes are not one at all — empty, or the ids-joined token
/// SDKs make up — which the caller treats as a completion that predates
/// activation tokens. `Some(Err)` when they claim to be one and are not: a
/// token that is malformed or whose signature does not check out is refused
/// rather than waved through as if it were a legacy token.
#[allow(clippy::result_large_err)] // tonic::Status is large; every RPC here returns one.
pub fn decode(token: &[u8]) -> Option<Result<ActivationToken, Status>> {
    let text = std::str::from_utf8(token).ok()?;
    let rest = text.strip_prefix(PREFIX)?;
    Some(decode_marked(rest, signing_key()))
}

#[allow(clippy::result_large_err)]
fn decode_marked(rest: &str, key: Option<&[u8]>) -> Result<ActivationToken, Status> {
    let malformed = || Status::invalid_argument("Malformed activation token");

    let (payload_b64, signature) = match rest.split_once('.') {
        Some((payload, signature)) => (payload, Some(signature)),
        None => (rest, None),
    };
    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or_else(malformed)?;

    match (key, signature) {
        (Some(key), Some(signature)) => {
            let expected = sign(key, &format!("{SIGNING_DOMAIN}{payload}"));
            if !bool::from(constant_time_eq(expected.as_bytes(), signature.as_bytes())) {
                return Err(Status::permission_denied(
                    "Invalid activation token signature",
                ));
            }
        }
        // Unsigned, on a replica that signs. Task tokens accept this during a
        // rollout because engines from before signing issued them; no engine
        // ever issued an unsigned activation token and a signing key at once,
        // so here it can only be forged or from a replica missing the key —
        // and the fence it would open is the one the key exists to guard.
        (Some(_), None) => {
            return Err(Status::permission_denied(
                "Unsigned activation token; this engine signs them",
            ))
        }
        // Read by a replica without the key, which cannot check it: accepted,
        // and said out loud, as for task tokens.
        (None, Some(_)) => warn!(
            "Received a signed activation token but ORCHER_TASK_TOKEN_KEY is not set on this \
             replica — accepting it unverified."
        ),
        (None, None) => {}
    }

    // The sequence is after the last colon, so an execution id containing one
    // still reads back whole.
    let (execution_id, seq) = payload.rsplit_once(':').ok_or_else(malformed)?;
    let activation_seq = seq.parse::<i64>().map_err(|_| malformed())?;
    if execution_id.is_empty() {
        return Err(malformed());
    }

    Ok(ActivationToken {
        execution_id: execution_id.to_string(),
        activation_seq,
    })
}

/// The activation token a completion carries, from whichever field it came in.
///
/// `task_token` is looked at first because it is where the token belongs; a
/// worker that fills it has read this engine's token rather than echoing a
/// field it does not interpret. `stream_entry_id` is where every SDK returns it
/// today. `None` when neither carries one.
#[allow(clippy::result_large_err)]
pub fn from_completion(
    task_token: &[u8],
    stream_entry_id: &str,
) -> Option<Result<ActivationToken, Status>> {
    decode(task_token).or_else(|| decode(stream_entry_id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_token_reads_back() {
        let token = encode("exec-1", 42);
        let read = decode(token.as_bytes())
            .expect("an activation token")
            .expect("valid");
        assert_eq!(read.execution_id, "exec-1");
        assert_eq!(read.activation_seq, 42);
    }

    /// An execution id with a colon in it reads back whole.
    #[test]
    fn an_execution_id_with_a_colon_survives() {
        let token = encode("a:b:c", 7);
        let read = decode(token.as_bytes()).unwrap().unwrap();
        assert_eq!(read.execution_id, "a:b:c");
        assert_eq!(read.activation_seq, 7);
    }

    /// The token SDKs make up, and no token at all, are not activation
    /// tokens: they are the legacy case, not an error.
    #[test]
    fn a_legacy_token_is_not_an_activation_token() {
        assert!(decode(b"").is_none());
        assert!(decode(b"wf-1:exec-1").is_none());
        assert!(decode(b"v2.abc.def").is_none());
    }

    /// Marked as an activation token but unreadable: refused, not treated as
    /// legacy, or a damaged token would silently switch the fence off.
    #[test]
    fn a_damaged_token_is_refused() {
        assert!(decode(b"act1.!!!").unwrap().is_err());
        let no_seq = format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode("exec-only"));
        assert!(decode(no_seq.as_bytes()).unwrap().is_err());
        let bad_seq = format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode("exec:x"));
        assert!(decode(bad_seq.as_bytes()).unwrap().is_err());
    }

    #[test]
    fn the_task_token_field_wins_over_the_stream_entry_id() {
        let in_task_token = encode("exec-1", 2);
        let in_stream = encode("exec-1", 1);
        let read = from_completion(in_task_token.as_bytes(), &in_stream)
            .unwrap()
            .unwrap();
        assert_eq!(read.activation_seq, 2);

        let read = from_completion(b"wf:exec-1", &in_stream).unwrap().unwrap();
        assert_eq!(read.activation_seq, 1);

        assert!(from_completion(b"wf:exec-1", "").is_none());
    }

    /// With a key, only a token signed with it opens the fence: an unsigned
    /// one, a forged signature, and one signed with another key are refused.
    #[test]
    fn a_keyed_engine_requires_a_valid_signature() {
        let key: &[u8] = b"test-key";
        let payload = "exec-1:3";
        let encoded = URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let signed = format!(
            "{encoded}.{}",
            sign(key, &format!("{SIGNING_DOMAIN}{payload}"))
        );
        let other = format!(
            "{encoded}.{}",
            sign(b"other-key", &format!("{SIGNING_DOMAIN}{payload}"))
        );

        assert_eq!(decode_marked(&signed, Some(key)).unwrap().activation_seq, 3);
        assert_eq!(
            decode_marked(&encoded, Some(key)).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            decode_marked(&other, Some(key)).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert!(decode_marked(&encoded, None).is_ok());
    }

    /// A signature made for one kind of token does not verify as the other.
    #[test]
    fn the_signing_domain_separates_it_from_task_tokens() {
        let key = b"test-key";
        assert_ne!(
            sign(key, "exec-1:1"),
            sign(key, &format!("{SIGNING_DOMAIN}exec-1:1"))
        );
    }
}
