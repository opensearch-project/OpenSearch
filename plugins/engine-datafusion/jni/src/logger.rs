//! Logger module that re-exports from the shared vectorized-exec-spi crate.
//!
//! This module provides logging functions that call back to Java's logging framework
//! through the RustLoggerBridge class.

// Re-export everything from the shared crate's logger module
pub use vectorized_exec_spi::logger::*;
