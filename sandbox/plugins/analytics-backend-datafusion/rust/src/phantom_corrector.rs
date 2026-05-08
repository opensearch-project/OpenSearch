/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Self-correcting phantom reservation based on observed batch sizes.
//!
//! The initial phantom is estimated from schema metadata. During execution,
//! actual `RecordBatch` sizes may differ. This module observes each batch
//! flowing through `CrossRtStream` and accumulates a correction delta that
//! the caller applies to the `MemoryReservation` periodically.
//!
//! # Thread safety
//!
//! All fields are atomic. The corrector is shared (`Arc`) between:
//! - `CrossRtStream::poll_next` (writes observations) — runs on IO runtime
//! - `api::stream_next` (reads + applies corrections) — same sequential caller
//!
//! No mutex needed. The sequential stream_next contract ensures apply is
//! never concurrent with itself.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

/// EMA alpha for batch size smoothing (0.4 = converges in ~3 samples).
const BATCH_EMA_ALPHA_X1000: u64 = 400;

/// Minimum batches before corrections start (EMA warmup).
const WARMUP_BATCHES: usize = 3;

/// Only correct every N batches to avoid churning the reservation.
const CORRECTION_INTERVAL: usize = 4;

/// Dead-band: don't correct if difference is within this fraction of current.
const CORRECTION_THRESHOLD: f64 = 0.10;

/// Self-correcting phantom observer. Lock-free, `Send + Sync`.
#[derive(Debug)]
pub struct PhantomCorrector {
    /// Number of concurrent batch slots in the pipeline that the phantom covers.
    /// = target_partitions × PARTITION_BUFFER_MULTIPLIER + 1 (output channel)
    batches_in_pipeline: usize,
    /// The per-batch bytes used in the original phantom estimate.
    estimated_batch_bytes: usize,
    /// EMA of actual batch bytes (×1000 fixed-point).
    ema_batch_bytes_x1000: AtomicU64,
    /// Number of batches observed.
    batches_observed: AtomicUsize,
    /// Accumulated correction delta (bytes). Positive = need more, negative = have excess.
    /// Read and reset by the caller via `take_pending_delta`.
    pending_delta: AtomicI64,
    /// Current effective phantom bytes (tracks applied corrections).
    current_phantom_bytes: AtomicI64,
}

impl PhantomCorrector {
    /// Create a new corrector.
    ///
    /// - `initial_phantom_bytes`: the phantom size reserved at query start
    /// - `estimated_batch_bytes`: `batch_size × avg_row_bytes` used in the estimate
    /// - `batches_in_pipeline`: how many batch-slots the phantom covers
    pub fn new(
        initial_phantom_bytes: usize,
        estimated_batch_bytes: usize,
        batches_in_pipeline: usize,
    ) -> Self {
        Self {
            batches_in_pipeline,
            estimated_batch_bytes,
            ema_batch_bytes_x1000: AtomicU64::new((estimated_batch_bytes as u64) * 1000),
            batches_observed: AtomicUsize::new(0),
            pending_delta: AtomicI64::new(0),
            current_phantom_bytes: AtomicI64::new(initial_phantom_bytes as i64),
        }
    }

    /// Create a corrector pre-seeded with measured batch bytes from cached
    /// parquet metadata. Skips the warmup phase since the EMA starts at
    /// the measured value — corrections begin from the first batch if
    /// actual sizes deviate from the cached measurement.
    ///
    /// Use this when `estimate_row_bytes_from_metadata` produced the initial
    /// phantom estimate. The corrector's EMA is seeded with the same value,
    /// so it only corrects if runtime batches differ from what metadata predicted.
    pub fn new_from_metadata(
        initial_phantom_bytes: usize,
        measured_batch_bytes: usize,
        batches_in_pipeline: usize,
    ) -> Self {
        Self {
            batches_in_pipeline,
            estimated_batch_bytes: measured_batch_bytes,
            ema_batch_bytes_x1000: AtomicU64::new((measured_batch_bytes as u64) * 1000),
            // Pre-warmed: skip warmup since we trust metadata-derived estimate
            batches_observed: AtomicUsize::new(WARMUP_BATCHES),
            pending_delta: AtomicI64::new(0),
            current_phantom_bytes: AtomicI64::new(initial_phantom_bytes as i64),
        }
    }

    /// Called per-batch from `CrossRtStream::poll_next`. Observes the actual
    /// batch memory size and periodically computes a correction delta.
    ///
    /// Cost: ~15ns (2 atomic ops + arithmetic). Only computes delta every
    /// `CORRECTION_INTERVAL` batches after warmup.
    #[inline]
    pub fn observe_batch(&self, actual_batch_bytes: usize) {
        // Update EMA
        let sample_x1000 = (actual_batch_bytes as u64) * 1000;
        let prev = self.ema_batch_bytes_x1000.load(Ordering::Relaxed);
        let new_ema =
            (BATCH_EMA_ALPHA_X1000 * sample_x1000 + (1000 - BATCH_EMA_ALPHA_X1000) * prev) / 1000;
        self.ema_batch_bytes_x1000.store(new_ema, Ordering::Relaxed);

        let count = self.batches_observed.fetch_add(1, Ordering::Relaxed) + 1;

        // Skip warmup and non-correction intervals
        if count < WARMUP_BATCHES || count % CORRECTION_INTERVAL != 0 {
            return;
        }

        // Compute what the phantom should be based on observed batch sizes
        let smoothed_batch_bytes = (new_ema / 1000) as i64;
        let ideal_phantom = smoothed_batch_bytes * self.batches_in_pipeline as i64;
        let current = self.current_phantom_bytes.load(Ordering::Relaxed);

        let diff = ideal_phantom - current;
        let threshold = (current as f64 * CORRECTION_THRESHOLD) as i64;

        if diff.abs() > threshold {
            self.pending_delta.fetch_add(diff, Ordering::Relaxed);
            self.current_phantom_bytes.fetch_add(diff, Ordering::Relaxed);
        }
    }

    /// Take the accumulated correction delta. Returns 0 if no correction pending.
    /// Called by `stream_next` after delivering a batch to Java.
    #[inline]
    pub fn take_pending_delta(&self) -> i64 {
        self.pending_delta.swap(0, Ordering::Relaxed)
    }
}

// Send + Sync is auto-derived since all fields are atomic.
// Explicit assertion for documentation:
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn check() {
        assert_send_sync::<PhantomCorrector>();
    }
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_correction_during_warmup() {
        let c = PhantomCorrector::new(1000, 100, 10);
        // First 3 observations: no delta
        c.observe_batch(200);
        c.observe_batch(200);
        assert_eq!(c.take_pending_delta(), 0);
    }

    #[test]
    fn correction_after_warmup_and_interval() {
        // estimated: 100 bytes/batch × 10 slots = 1000 phantom
        // actual: 200 bytes/batch → ideal phantom = 2000
        let c = PhantomCorrector::new(1000, 100, 10);
        // Feed enough observations to trigger: WARMUP(3) + needs to hit INTERVAL(4)
        for _ in 0..4 {
            c.observe_batch(200);
        }
        let delta = c.take_pending_delta();
        // Should be positive (need more phantom)
        assert!(delta > 0, "expected positive delta, got {}", delta);
    }

    #[test]
    fn correction_shrinks_on_smaller_batches() {
        // estimated: 200 bytes/batch × 10 = 2000 phantom
        // actual: 50 bytes/batch → ideal = 500
        let c = PhantomCorrector::new(2000, 200, 10);
        for _ in 0..8 {
            c.observe_batch(50);
        }
        let delta = c.take_pending_delta();
        // Should be negative (excess phantom)
        assert!(delta < 0, "expected negative delta, got {}", delta);
    }

    #[test]
    fn no_correction_within_deadband() {
        // estimated: 100 bytes/batch × 10 = 1000
        // actual: 105 bytes/batch → ideal = 1050, diff = 50, threshold = 100 (10%)
        // 50 < 100 → no correction
        let c = PhantomCorrector::new(1000, 100, 10);
        for _ in 0..8 {
            c.observe_batch(105);
        }
        let delta = c.take_pending_delta();
        assert_eq!(delta, 0);
    }

    #[test]
    fn take_resets_delta() {
        let c = PhantomCorrector::new(1000, 100, 10);
        for _ in 0..8 {
            c.observe_batch(300);
        }
        let first = c.take_pending_delta();
        assert!(first != 0);
        let second = c.take_pending_delta();
        assert_eq!(second, 0);
    }
}
