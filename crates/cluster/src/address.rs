//! Whether an address one node registered is one another node can dial.
//!
//! A node registers the address it serves on, and until a node could be told
//! otherwise that was its bind address — `0.0.0.0:50051` on every pod. That is a
//! perfectly good thing to listen on and a useless thing to dial: from a peer it
//! means "yourself". So before anything connects to a peer's registered address
//! it asks here, and an address that names no particular host is treated as
//! absent rather than dialed.

use std::net::{IpAddr, Ipv6Addr};

/// `address` in the form a peer dials, or `None` if it names no host another
/// node could connect to.
///
/// Accepts `host:port`, `[v6]:port`, and either with an `http://` or
/// `https://` scheme in front. Refuses an empty address, one without a port,
/// and one whose host is the unspecified address (`0.0.0.0`, `::`), which is
/// what a bind address looks like.
///
/// An IPv6 address written without brackets — what `$(POD_IP):50051` gives on
/// a single-stack IPv6 cluster — is bracketed: `fd00::1:50051` becomes
/// `[fd00::1]:50051`. The last colon is taken as the port separator, which is
/// the only reading under which such an address has a port at all.
pub fn normalize(address: &str) -> Option<String> {
    let address = address.trim().trim_end_matches('/');
    let (scheme, rest) = ["http://", "https://"]
        .iter()
        .find_map(|s| address.strip_prefix(s).map(|rest| (*s, rest)))
        .unwrap_or(("", address));

    let (host, port) = rest.rsplit_once(':')?;
    if host.is_empty() || port.parse::<u16>().map_or(true, |p| p == 0) {
        return None;
    }
    let host = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(v6) => {
            let ip: Ipv6Addr = v6.parse().ok()?;
            if ip.is_unspecified() {
                return None;
            }
            format!("[{v6}]")
        }
        None => match host.parse::<IpAddr>() {
            Ok(ip) if ip.is_unspecified() => return None,
            Ok(IpAddr::V6(_)) => format!("[{host}]"),
            Ok(IpAddr::V4(_)) => host.to_string(),
            // Neither an IP nor something with a stray colon: a hostname.
            Err(_) if !host.contains(':') => host.to_string(),
            Err(_) => return None,
        },
    };
    Some(format!("{scheme}{host}:{port}"))
}

/// `true` if `address` names a host another node could connect to.
pub fn is_dialable(address: &str) -> bool {
    normalize(address).is_some()
}

/// `true` if `address`'s host is this machine only: `localhost` or a loopback
/// IP. Dialable, but only by processes on the same machine.
pub fn is_loopback(address: &str) -> bool {
    let Some(normalized) = normalize(address) else {
        return false;
    };
    let rest = normalized
        .strip_prefix("http://")
        .or_else(|| normalized.strip_prefix("https://"))
        .unwrap_or(&normalized);
    let Some((host, _)) = rest.rsplit_once(':') else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bind_address_is_not_dialable() {
        assert!(!is_dialable("0.0.0.0:50051"));
        assert!(!is_dialable("[::]:50051"));
        assert!(!is_dialable(":::50051"));
        assert!(!is_dialable("http://0.0.0.0:50051"));
    }

    #[test]
    fn a_concrete_host_is_dialable() {
        assert!(is_dialable("10.1.2.3:50051"));
        assert!(is_dialable("orchestrator-0.orchestrator:50051"));
        assert!(is_dialable("http://127.0.0.1:50151"));
        assert!(is_dialable("https://engine.internal:443"));
        assert!(is_dialable("[fd00::1]:50051"));
    }

    #[test]
    fn anything_without_a_host_and_port_is_not() {
        assert!(!is_dialable(""));
        assert!(!is_dialable("10.1.2.3"));
        assert!(!is_dialable(":50051"));
        assert!(!is_dialable("10.1.2.3:0"));
        assert!(!is_dialable("fd00::1"));
    }

    #[test]
    fn an_unbracketed_ipv6_address_is_bracketed() {
        // "$(POD_IP):50051" on a single-stack IPv6 cluster.
        assert_eq!(
            normalize("fd00:10:244::5:50051").as_deref(),
            Some("[fd00:10:244::5]:50051")
        );
        assert_eq!(
            normalize("[fd00::1]:50051").as_deref(),
            Some("[fd00::1]:50051")
        );
        assert_eq!(
            normalize("http://fd00::1:50051").as_deref(),
            Some("http://[fd00::1]:50051")
        );
        assert_eq!(
            normalize("10.1.2.3:50051").as_deref(),
            Some("10.1.2.3:50051")
        );
    }

    #[test]
    fn loopback_is_recognised_in_every_spelling() {
        assert!(is_loopback("127.0.0.1:50051"));
        assert!(is_loopback("localhost:50051"));
        assert!(is_loopback("[::1]:50051"));
        assert!(is_loopback("::1:50051"));
        assert!(!is_loopback("10.1.2.3:50051"));
        assert!(!is_loopback("0.0.0.0:50051"));
    }
}
