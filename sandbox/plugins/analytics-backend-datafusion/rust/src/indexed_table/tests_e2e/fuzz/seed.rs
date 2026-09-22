/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Seed plumbing for the fuzz suite.
//!
//! - Master seed comes from `INDEXED_E2E_SEED` env var (hex) if set,
//!   otherwise a fresh seed derived from the system clock — printed to
//!   stderr so failing CI runs are reproducible.
//! - Per-iteration seeds are derived from the master via
//!   `derive_seed(master, test_name, iter_idx)`. Using a named derive
//!   (not just `master + i`) keeps unrelated tests' iteration seeds
//!   uncorrelated, so a change to one test's iteration count doesn't
//!   shift other tests' failure profiles.

use std::time::{SystemTime, UNIX_EPOCH};

/// Fetch the suite's master seed. Printed on stderr when freshly
/// generated so the CI log contains the reproducer.
pub(in crate::indexed_table::tests_e2e) fn master_seed() -> u64 {
    if let Ok(s) = std::env::var("INDEXED_E2E_SEED") {
        let parsed = parse_seed(&s);
        match parsed {
            Some(v) => {
                eprintln!("INDEXED_E2E_SEED={:016x} (from env)", v);
                return v;
            }
            None => panic!("INDEXED_E2E_SEED={:?} is not a valid hex u64", s),
        }
    }
    let s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xdead_beef_cafe_babe);
    eprintln!(
        "INDEXED_E2E_SEED={:016x}   # reproduce: INDEXED_E2E_SEED={:016x} cargo test ...",
        s, s
    );
    s
}

/// Parse a seed from environment value. Accepts:
/// - Plain hex: `DEADBEEF`, `deadbeef`, `0xdeadbeef`
/// - OpenSearch/Lucene format: `L:ABCDEF1234:7890` — takes only the
///   first hex segment (the master seed). Additional segments are
///   per-test derivations in the Java runner that don't apply here.
fn parse_seed(s: &str) -> Option<u64> {
    let s = s.trim();
    // Lucene-style `L:seed:...` → take first hex segment after `L:`.
    let hex_part = if let Some(rest) = s.strip_prefix("L:").or_else(|| s.strip_prefix("l:")) {
        rest.split(':').next().unwrap_or(rest)
    } else {
        s
    };
    let hex_part = hex_part.trim_start_matches("0x").trim_start_matches("0X");
    u64::from_str_radix(hex_part, 16).ok()
}

/// Derive a per-iteration seed from the master. Uses a splitmix64-style
/// mixer over `(master, fnv1a(test_name), iter)` so seeds are
/// well-distributed across tests and iterations.
pub(in crate::indexed_table::tests_e2e) fn derive_seed(
    master: u64,
    test_name: &str,
    iter: u64,
) -> u64 {
    let name_hash = fnv1a_64(test_name.as_bytes());
    let mut x = master
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(name_hash);
    x ^= iter.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    splitmix64(x)
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_seed_is_deterministic() {
        let a = derive_seed(0x1234, "test_foo", 7);
        let b = derive_seed(0x1234, "test_foo", 7);
        assert_eq!(a, b);
    }

    #[test]
    fn derive_seed_varies_by_test_name() {
        let a = derive_seed(0x1234, "test_foo", 0);
        let b = derive_seed(0x1234, "test_bar", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn derive_seed_varies_by_iter() {
        let a = derive_seed(0x1234, "test_foo", 0);
        let b = derive_seed(0x1234, "test_foo", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn derive_seed_varies_by_master() {
        let a = derive_seed(0x1234, "test_foo", 0);
        let b = derive_seed(0x5678, "test_foo", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn parse_seed_plain_hex() {
        assert_eq!(parse_seed("DEADBEEF"), Some(0xdeadbeef));
        assert_eq!(parse_seed("deadbeef"), Some(0xdeadbeef));
        assert_eq!(parse_seed("0xDEADBEEF"), Some(0xdeadbeef));
        assert_eq!(parse_seed("0X12345678"), Some(0x12345678));
    }

    #[test]
    fn parse_seed_lucene_format() {
        // L:<master_hex>:<derived> → take master
        assert_eq!(parse_seed("L:ABCDEF1234:7890"), Some(0xabcdef1234));
        assert_eq!(
            parse_seed("l:deadbeefcafebabe:42"),
            Some(0xdeadbeefcafebabe)
        );
    }

    #[test]
    fn parse_seed_rejects_garbage() {
        assert_eq!(parse_seed(""), None);
        assert_eq!(parse_seed("not-hex"), None);
        assert_eq!(parse_seed("L:ZZZ"), None);
    }

    #[test]
    fn parse_seed_handles_whitespace() {
        assert_eq!(parse_seed("  DEADBEEF  "), Some(0xdeadbeef));
    }
}
