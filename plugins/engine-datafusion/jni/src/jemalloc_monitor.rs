/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use snafu::{ResultExt, Snafu};
use std::sync::atomic::{AtomicUsize, Ordering};
use vectorized_exec_spi::log_info;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


#[derive(Debug, Snafu)]
pub enum AllocationMonitorError {
    #[snafu(display("Error getting jemalloc stats: {}", source))]
    Jemalloc { source: tikv_jemalloc_ctl::Error },

    #[snafu(display("Heap exhausted"))]
    HeapExhausted,
}

/// The factor by which the memory known reserved memory is multiplied
/// when considereng whether the jemalloc statistics need to be
/// refreshed. The lower the value the more memory will be reserved
/// before a refresh is triggered.
const MEMORY_RESERVATION_RECHECK_FACTOR: usize = 4;

/// A monitor for jemalloc heap allocations. This monitor attempts to
/// keep the amount of heap memory allocated below a certain threshold.
/// To do this, it attempts to track the amount of memory allocated
/// through polling jemalloc statistics along with reservations made
/// through the monitor.
///
/// Collecting jemalloc statistics is not free, so the monitor attempts
/// to amortize the cost by only polling the statistics after a querter
/// of the available data has been reserved. As the system gets under
/// higher memory pressure the monitor will poll more frequently, making
/// memory reservations slower. This is acceptable when the alternative
/// is having the whole process be OOM killed.
pub struct AllocationMonitor {
    max: usize,
    allocated: AtomicUsize,
    reserved: AtomicUsize,
}

impl std::fmt::Debug for AllocationMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllocationMonitor")
            .field("max", &self.max)
            .field("allocated", &self.allocated)
            .field("reserved", &self.reserved)
            .finish_non_exhaustive()
    }
}

impl AllocationMonitor {
    /// Create a new allocation monitor that will return an error if the
    /// amount of memory allocated exceeds `max`.
    pub fn try_new(max: usize) -> Result<Self, AllocationMonitorError> {
        let monitor = Self {
            max,
            allocated: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
        };
        monitor.try_refresh()?;
        Ok(monitor)
    }

    /// Update the stats for the heap monitor
    fn try_refresh(&self) -> Result<(), AllocationMonitorError> {
        let reserved = self.reserved.load(Ordering::Acquire);
        self.allocated.store(
            jemalloc_stats::refresh_allocated().context(JemallocSnafu)?,
            Ordering::SeqCst,
        );
        self.reserved.fetch_sub(reserved, Ordering::Release);
        Ok(())
    }

    /// Reserve `sz` bytes of memory. This will return an error if the
    /// amount of memory allocated will exceed the maximum if the
    /// allocation were to be allowed.
    pub fn try_reserve(&self, sz: usize) -> Result<(), AllocationMonitorError> {
        let reserved = self.reserved.fetch_add(sz, Ordering::AcqRel) + sz;
        if self.allocated.load(Ordering::Acquire) + MEMORY_RESERVATION_RECHECK_FACTOR * reserved
            > self.max
        {
            log_info!("Refreshing stats sz: {}, reserved: {}, max: {}", sz, reserved + sz, self.max);
            // We have used more than a quarter of the memory that was considered
            // free last time we checkes the stats. Refresh the stats and check again.
            self.try_refresh()?;
            let allocated = self.allocated.load(Ordering::Acquire);
            let reserved = self.reserved.fetch_add(sz, Ordering::Acquire);

            log_info!("New Allocated stats: {}, sz: {}, reserved: {}, max: {}", allocated, sz, reserved, self.max);
            if allocated + sz + reserved > self.max {
                return Err(AllocationMonitorError::HeapExhausted);
            }
        }

        Ok(())
    }

    /// Unconditionally reserve sz bytes of memory. This marks the
    /// additional memory as being reserved, but cannot fail. This is
    /// used to tell the monitor about memory that is being reserved
    /// outside of it's control. It will make it more likely that the
    /// next call to try_reserve will update the memory statistics.
    pub fn reserve(&self, sz: usize) {
        self.reserved.fetch_add(sz, Ordering::AcqRel);
    }
}

mod jemalloc_stats {
    use std::sync::LazyLock;
    use tikv_jemalloc_ctl::{
        Result, epoch, epoch_mib,
        stats::{allocated, allocated_mib},
    };

    static EPOCH_MIB: LazyLock<Result<epoch_mib>> = LazyLock::new(epoch::mib);
    static ALLOCATED_MIB: LazyLock<Result<allocated_mib>> = LazyLock::new(allocated::mib);

    pub(super) fn refresh_allocated() -> Result<usize> {
        (*EPOCH_MIB)?.write(0)?;
        (*ALLOCATED_MIB)?.read()
    }
}
