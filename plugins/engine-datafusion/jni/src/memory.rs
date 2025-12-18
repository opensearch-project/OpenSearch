/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::Arc;
use datafusion::common::DataFusionError;
use crate::jemalloc_monitor::{AllocationMonitor, AllocationMonitorError};

pub type Result<T, E = DataFusionError> = result::Result<T, E>;


#[derive(Debug)]
pub struct AllocationMonitoringMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<AllocationMonitor>,
}

impl AllocationMonitoringMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<AllocationMonitor>) -> Self {
        Self { inner, monitor }
    }
}

impl MemoryPool for AllocationMonitoringMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.monitor.reserve(additional);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        // Shrinking doesn't update the monitor. This means that the
        // monitor will always see the memory reservation is increasing
        // causing the statistics to be polled from jemalloc
        // occasionally, keeping the statistics more accurate.
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<()> {
        self.monitor.try_reserve(additional).map_err(|e| match e {
            AllocationMonitorError::Jemalloc { source } => {
                DataFusionError::External(Box::new(source))
            }
            AllocationMonitorError::HeapExhausted => {
                DataFusionError::ResourcesExhausted(e.to_string())
            }
        })?;
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }
}
