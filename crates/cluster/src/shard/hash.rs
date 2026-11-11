//! Stable hashing primitive for shard mapping.
//!
//! These are hand-rolled (vs `std::hash::DefaultHasher`) precisely because
//! ownership must be deterministic **across nodes and across Rust versions** —
//! a guarantee the std hasher does not make. The constants here are locked; a
//! change would silently re-shuffle every shard's owner across a live fleet.

/// FNV-1a 64-bit — a small, allocation-free, **stable** hash.
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001b3;

/// FNV-1a starting from an explicit seed.
#[inline]
fn fnv1a_seeded(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

#[inline]
pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_seeded(FNV_OFFSET, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_is_stable() {
        // Locked value. This hash decides a workflow's shard, and that shard is
        // persisted to every row it writes — a change here does not just reshuffle
        // ownership, it invalidates shard_id on rows already in the database.
        assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a(b"a"), 0xaf63dc4c8601ec8c);
    }
}
