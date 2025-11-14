/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use datafusion::common::DataFusionError;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

#[derive(Debug)]
pub struct CustomMemoryPool {
    memory_pool: Arc<MonitoredMemoryPool>
}

impl CustomMemoryPool {
    pub fn new(memory_pool: Arc<MonitoredMemoryPool>) -> Self {
        Self { memory_pool }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.memory_pool.get_monitor()
    }

    pub fn get_memory_pool(&self) -> Arc<dyn MemoryPool> {
        self.memory_pool.clone()
    }
}

#[derive(Debug, Default)]
pub(crate) struct Monitor {
    pub(crate) value: AtomicUsize,
    pub(crate) max: AtomicUsize,
}

impl Monitor {
    pub(crate) fn max(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }

    fn grow(&self, amount: usize) {
        let old = self.value.fetch_add(amount, Ordering::Relaxed);
        self.max.fetch_max(old + amount, Ordering::Relaxed);
    }

    fn shrink(&self, amount: usize) {
        self.value.fetch_sub(amount, Ordering::Relaxed);
    }

    fn get_current_val(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<Monitor>,
}

impl MonitoredMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<Monitor>) -> Self {
        Self { inner, monitor }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.monitor.clone()
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn register(&self, _consumer: &MemoryConsumer) {
        self.inner.register(_consumer)
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        self.inner.unregister(_consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.monitor.grow(additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.monitor.shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.monitor.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}
