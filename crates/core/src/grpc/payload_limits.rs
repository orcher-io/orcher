//! How large a message and a payload may be, and what is said when one is
//! larger.
//!
//! Two limits, one inside the other:
//!
//! - `max_message_bytes` bounds one gRPC message in either direction. tonic's
//!   default is 4 MiB, which a workflow carrying a long prompt or a
//!   conversation history reaches quickly.
//! - `payload_max_bytes` bounds one payload: a workflow's input or result, a
//!   task's input or result, an event's payload, a child's input. It is
//!   enforced where a payload enters the engine, and kept well below the
//!   message limit because an activation carries every payload the workflow
//!   has received so far, not just one.
//!
//! A payload over its limit is refused where it enters, with a message that
//! says what it was, how large, and what to do instead. It must never be
//! accepted and then fail to leave: a task result the engine stored but could
//! not hand to the workflow used to leave the workflow running for good, its
//! activation handed out every claim timeout and refused by the worker every
//! time.

use tonic::metadata::MetadataMap;

/// One MiB.
pub const MIB: usize = 1024 * 1024;

/// The largest gRPC message the engine sends or accepts unless configured.
///
/// 32 MiB: four payloads at the default payload limit, plus the activation
/// around them. A message is held whole in memory while it is decoded or
/// encoded, so this is also the most one call can make the engine hold; at the
/// default `max_concurrent_streams` that bound is reached only by calls that
/// really carry that much.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 32 * MIB;

/// The largest single payload the engine accepts unless configured.
///
/// 8 MiB holds a long prompt or a conversation history several times over. A
/// payload is stored more than once — on the task, in the journal and in the
/// step log — and every activation of the workflow reads all of them again, so
/// a larger one costs on every step, not once; Postgres would store far larger
/// values (TOAST takes up to 1 GB), but reading them back each step is what a
/// limit protects.
pub const DEFAULT_PAYLOAD_MAX_BYTES: usize = 8 * MIB;

/// What a worker that does not say otherwise can receive: tonic's default
/// decoding limit, which every SDK released before the limit was configurable
/// runs with.
pub const LEGACY_RECEIVE_LIMIT_BYTES: usize = 4 * MIB;

/// The header a worker states the largest message it can receive in, in bytes.
/// A worker that sends none is taken to accept [`LEGACY_RECEIVE_LIMIT_BYTES`].
pub const MAX_RECEIVE_MESSAGE_BYTES_HEADER: &str = "x-orcher-max-receive-message-bytes";

/// The environment variable that sets the message limit, read by the engine
/// and by the gateway in front of it.
pub const MAX_MESSAGE_BYTES_ENV: &str = "ORCHER_GRPC_MAX_MESSAGE_BYTES";

/// The message limit from [`MAX_MESSAGE_BYTES_ENV`], or the default. For a
/// process with no config file section of its own for it — the gateway, whose
/// requests and answers carry the same payloads the engine does.
pub fn max_message_bytes_from_env() -> usize {
    std::env::var(MAX_MESSAGE_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES)
}

/// The failure type a payload that is too large is reported under, so a
/// retry policy can name it.
pub const PAYLOAD_TOO_LARGE: &str = "PayloadTooLarge";

/// The two limits, as configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadLimits {
    /// The largest gRPC message sent or accepted.
    pub max_message_bytes: usize,
    /// The largest single payload accepted.
    pub payload_max_bytes: usize,
}

impl Default for PayloadLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            payload_max_bytes: DEFAULT_PAYLOAD_MAX_BYTES,
        }
    }
}

impl PayloadLimits {
    /// `Err` with the reason when `what`, `size` bytes long, is over the
    /// payload limit.
    pub fn check(&self, what: &str, size: usize) -> Result<(), String> {
        if size > self.payload_max_bytes {
            Err(payload_too_large(what, size, self.payload_max_bytes))
        } else {
            Ok(())
        }
    }

    /// The largest message the caller of this request can receive: what it
    /// says in [`MAX_RECEIVE_MESSAGE_BYTES_HEADER`], or the legacy default
    /// when it says nothing, and never more than this engine sends.
    pub fn receive_limit(&self, metadata: &MetadataMap) -> usize {
        receive_limit_stated(metadata)
            .unwrap_or(LEGACY_RECEIVE_LIMIT_BYTES)
            .min(self.max_message_bytes)
    }
}

/// What the caller stated in [`MAX_RECEIVE_MESSAGE_BYTES_HEADER`], if anything
/// usable.
pub fn receive_limit_stated(metadata: &MetadataMap) -> Option<usize> {
    metadata
        .get(MAX_RECEIVE_MESSAGE_BYTES_HEADER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
}

/// The message for a payload that is over the limit.
pub fn payload_too_large(what: &str, size: usize, limit: usize) -> String {
    format!(
        "{what} is {}; the limit is {} (payload_max_bytes). \
         Store large data elsewhere and pass a reference.",
        format_bytes(size),
        format_bytes(limit)
    )
}

/// The message for an activation that is larger than the worker polling for
/// it can receive.
pub fn activation_too_large(what: &str, size: usize, receive_limit: usize, legacy: bool) -> String {
    let remedy = if legacy {
        "This worker's SDK accepts no more than the gRPC default; upgrade it to one \
         that accepts larger messages, and keep payloads small: store large data \
         elsewhere and pass a reference."
    } else {
        "It carries every task result and event payload the workflow has received; \
         keep them small (store large data elsewhere and pass a reference), or raise \
         the worker's max_message_bytes and the engine's grpc_max_message_bytes."
    };
    format!(
        "{what} is {}, more than the {} the worker can receive. {remedy}",
        format_bytes(size),
        format_bytes(receive_limit),
    )
}

/// A byte count as a person reads it: `5.2 MiB`, `8 MiB`, `512.0 KiB`,
/// `100 bytes`.
pub fn format_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    if bytes >= MIB {
        if bytes.is_multiple_of(MIB) {
            format!("{} MiB", bytes / MIB)
        } else {
            format!("{:.1} MiB", bytes as f64 / MIB as f64)
        }
    } else if bytes >= KIB {
        if bytes.is_multiple_of(KIB) {
            format!("{} KiB", bytes / KIB)
        } else {
            format!("{:.1} KiB", bytes as f64 / KIB as f64)
        }
    } else {
        format!("{bytes} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_read_as_a_person_would_write_them() {
        assert_eq!(format_bytes(8 * MIB), "8 MiB");
        assert_eq!(format_bytes(5 * MIB + MIB / 5), "5.2 MiB");
        assert_eq!(format_bytes(512 * 1024), "512 KiB");
        assert_eq!(format_bytes(100), "100 bytes");
    }

    #[test]
    fn a_payload_over_the_limit_says_what_how_large_and_what_to_do() {
        let limits = PayloadLimits {
            max_message_bytes: 32 * MIB,
            payload_max_bytes: 4 * MIB,
        };
        assert!(limits.check("task result", 4 * MIB).is_ok());
        let reason = limits
            .check("task result", 5 * MIB + MIB / 5)
            .expect_err("over the limit");
        assert_eq!(
            reason,
            "task result is 5.2 MiB; the limit is 4 MiB (payload_max_bytes). \
             Store large data elsewhere and pass a reference."
        );
    }

    #[test]
    fn a_worker_that_states_no_limit_is_held_to_the_grpc_default() {
        let limits = PayloadLimits::default();
        let mut metadata = MetadataMap::new();
        assert_eq!(limits.receive_limit(&metadata), LEGACY_RECEIVE_LIMIT_BYTES);

        metadata.insert(
            MAX_RECEIVE_MESSAGE_BYTES_HEADER,
            (16 * MIB).to_string().parse().unwrap(),
        );
        assert_eq!(limits.receive_limit(&metadata), 16 * MIB);

        // Never more than the engine itself sends.
        metadata.insert(
            MAX_RECEIVE_MESSAGE_BYTES_HEADER,
            (1024 * MIB).to_string().parse().unwrap(),
        );
        assert_eq!(limits.receive_limit(&metadata), DEFAULT_MAX_MESSAGE_BYTES);

        // Nonsense is not a statement.
        metadata.insert(MAX_RECEIVE_MESSAGE_BYTES_HEADER, "lots".parse().unwrap());
        assert_eq!(limits.receive_limit(&metadata), LEGACY_RECEIVE_LIMIT_BYTES);
    }
}
