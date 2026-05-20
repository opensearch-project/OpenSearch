/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::io::{Result, Write};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::{Duration, Instant};

// TODO: Make this value dynamic based on resource availability (e.g., adjust ±x% based on IOPS pressure)
const MIN_PAUSE_CHECK_MSEC: f64 = 20.0;
const BYTES_PER_MB: f64 = 1024.0 * 1024.0;
const MAX_MIN_PAUSE_CHECK_BYTES: usize = 1024 * 1024; // 1 MB
const MSEC_TO_SEC: f64 = 1000.0;

/// Configuration for rate limiting behavior.
struct RateLimiterConfig {
    /// Maximum throughput in megabytes per second
    mb_per_sec: f64,
    /// Minimum bytes to write before checking if pause is needed
    min_pause_check_bytes: usize,
}

/// A writer that rate-limits write operations to a specified throughput.
///
/// This writer wraps another writer and ensures that data is written at a maximum
/// rate specified in megabytes per second. It uses periodic pauses to maintain
/// the target rate, checking after a minimum number of bytes have been written.
///
/// # Rate Limiting Strategy
///
/// The rate limiter works by:
/// 1. Tracking bytes written since the last pause
/// 2. Periodically checking if enough time has elapsed for the bytes written
/// 3. Sleeping if the write rate exceeds the configured limit
///
/// The minimum pause check interval is calculated to avoid excessive overhead
/// from frequent time checks, defaulting to 25ms worth of data or 1MB, whichever
/// is smaller.
///
/// # Thread Safety
///
/// The rate limit can be updated dynamically via `set_mb_per_sec()`. The configuration
/// is protected by a `RwLock`, allowing concurrent reads while ensuring safe updates.
/// If the lock becomes poisoned (due to a panic in another thread), the writer will
/// gracefully degrade by skipping rate limiting rather than propagating the panic.
///
///
/// # Special Cases
///
/// - Setting `mb_per_sec` to `0.0` disables rate limiting entirely
/// - Negative values are rejected with an error
/// - Lock poisoning is handled gracefully by skipping rate limiting
pub struct RateLimitedWriter<W: Write> {
    inner: W,
    rate_limiter_config: Arc<RwLock<RateLimiterConfig>>,
    bytes_since_last_pause: usize,
    last_pause_time: Instant,
}

impl<W: Write> RateLimitedWriter<W> {
    /// Creates a new rate-limited writer with the specified throughput limit.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying writer to wrap
    /// * `mb_per_sec` - Maximum write rate in megabytes per second (must be non-negative)
    ///
    /// # Returns
    ///
    /// Returns `Ok(RateLimitedWriter)` on success, or an error if `mb_per_sec` is negative.
    ///
    ///
    /// # Errors
    ///
    /// Returns `Err` with `ErrorKind::InvalidInput` if `mb_per_sec` is negative.
    pub fn new(inner: W, mb_per_sec: f64) -> Result<Self> {
        if mb_per_sec < 0.0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("mbPerSec must be non-negative: got: {}", mb_per_sec),
            ));
        }

        let min_pause_check_bytes = Self::calculate_min_pause_check_bytes(mb_per_sec);
        Ok(Self {
            inner,
            rate_limiter_config: Arc::new(RwLock::new(RateLimiterConfig {
                mb_per_sec,
                min_pause_check_bytes,
            })),
            bytes_since_last_pause: 0,
            last_pause_time: Instant::now(),
        })
    }

    /// Updates the rate limit dynamically.
    ///
    /// This method allows changing the throughput limit while the writer is in use.
    /// The new rate takes effect immediately for subsequent write operations.
    ///
    /// # Arguments
    ///
    /// * `mb_per_sec` - New maximum write rate in megabytes per second (must be non-negative)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the rate is invalid or the lock is poisoned.
    ///
    ///
    /// # Errors
    ///
    /// Returns `Err` with:
    /// - `ErrorKind::InvalidInput` if `mb_per_sec` is negative
    /// - `ErrorKind::Other` if the internal lock is poisoned
    pub fn set_mb_per_sec(&mut self, mb_per_sec: f64) -> Result<()> {
        if mb_per_sec < 0.0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("mbPerSec must be non-negative: got: {}", mb_per_sec),
            ));
        }

        let min_pause_check_bytes = Self::calculate_min_pause_check_bytes(mb_per_sec);

        let mut config = self.rate_limiter_config.write().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to acquire write lock: {}", e),
            )
        })?;

        config.mb_per_sec = mb_per_sec;
        config.min_pause_check_bytes = min_pause_check_bytes;

        Ok(())
    }

    /// Calculates the minimum number of bytes to write before checking if a pause is needed.
    ///
    /// This is based on the configured rate and a minimum pause check interval to avoid
    /// excessive overhead from frequent time checks. The result is capped at 1MB.
    fn calculate_min_pause_check_bytes(mb_per_sec: f64) -> usize {
        let bytes = (MIN_PAUSE_CHECK_MSEC / MSEC_TO_SEC) * mb_per_sec * BYTES_PER_MB;
        std::cmp::min(MAX_MIN_PAUSE_CHECK_BYTES, bytes as usize)
    }

    /// Pauses execution if the write rate exceeds the configured limit.
    ///
    /// Calculates the target time for writing the given number of bytes based on
    /// the configured rate, and sleeps if insufficient time has elapsed since the
    /// last pause. If the lock is poisoned, rate limiting is skipped.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes written since the last pause
    fn pause(&mut self, bytes: usize) {
        let config = match self.rate_limiter_config.read() {
            Ok(config) => config,
            Err(_) => {
                // Lock is poisoned, skip rate limiting this time
                return;
            }
        };

        if config.mb_per_sec == 0.0 {
            return;
        }

        let elapsed = self.last_pause_time.elapsed().as_secs_f64();
        let target_time = bytes as f64 / (config.mb_per_sec * BYTES_PER_MB);

        if target_time > elapsed {
            let sleep_time = Duration::from_secs_f64(target_time - elapsed);
            sleep(sleep_time);
        }

        self.last_pause_time = Instant::now();
    }
}

impl<W: Write> Write for RateLimitedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes_since_last_pause += n;

        let current_min_pause_check_bytes = {
            match self.rate_limiter_config.read() {
                Ok(config) => config.min_pause_check_bytes,
                Err(_) => {
                    // Lock is poisoned, use a safe default
                    MAX_MIN_PAUSE_CHECK_BYTES
                }
            }
        };

        if self.bytes_since_last_pause > current_min_pause_check_bytes {
            self.pause(self.bytes_since_last_pause);
            self.bytes_since_last_pause = 0;
        }
        Ok(n)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}


