/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! jemalloc memory metrics via pre-resolved MIB keys.

use std::sync::OnceLock;
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats, stats::allocated_mib, stats::resident_mib};

struct StatsMib {
    epoch: epoch_mib,
    allocated: allocated_mib,
    resident: resident_mib,
}

static MIB: OnceLock<StatsMib> = OnceLock::new();

fn mib() -> &'static StatsMib {
    MIB.get_or_init(|| StatsMib {
        epoch: epoch::mib().unwrap(),
        allocated: stats::allocated::mib().unwrap(),
        resident: stats::resident::mib().unwrap(),
    })
}

/// Returns current jemalloc allocated bytes, or -1 on error.
/// This reflects the total bytes actively in use by application code (live malloc'd objects).
/// Useful for application-level memory accounting and DataFusion memory pool budgeting.
///
/// TODO: integrate with node/stats
pub fn allocated_bytes() -> i64 {
    let m = mib();
    if m.epoch.advance().is_err() {
        return -1;
    }
    m.allocated.read().map(|v| v as i64).unwrap_or(-1)
}

/// Returns current jemalloc resident bytes, or -1 on error.
/// This reflects physical RAM consumed by the native (Rust/DataFusion) layer only — excludes
/// JVM heap, metaspace, and other non-jemalloc allocations. Use for OOM prevention and alerting.
///
/// TODO: integrate with node/stats
pub fn resident_bytes() -> i64 {
    let m = mib();
    if m.epoch.advance().is_err() {
        return -1;
    }
    m.resident.read().map(|v| v as i64).unwrap_or(-1)
}

/// FFI: Returns current jemalloc allocated bytes, or -1 on error.
#[no_mangle]
pub extern "C" fn native_jemalloc_allocated_bytes() -> i64 {
    allocated_bytes()
}

/// FFI: Returns current jemalloc resident bytes, or -1 on error.
#[no_mangle]
pub extern "C" fn native_jemalloc_resident_bytes() -> i64 {
    resident_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

    #[test]
    fn allocated_bytes_is_positive() {
        assert!(allocated_bytes() > 0);
    }

    #[test]
    fn resident_bytes_is_positive() {
        assert!(resident_bytes() > 0);
    }

    #[test]
    fn allocated_increases_after_allocation() {
        let before = allocated_bytes();
        let _data: Vec<u8> = vec![42u8; 1024 * 1024];
        let after = allocated_bytes();
        assert!(after > before, "expected {after} > {before}");
    }
}
