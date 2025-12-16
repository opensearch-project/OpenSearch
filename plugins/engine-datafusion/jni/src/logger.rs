//! Logger module that re-exports from the shared opensearch-vectorized-spi crate.
//!
//! This module provides logging functions that call back to Java's logging framework
//! through the RustLoggerBridge class.

// Re-export init functions from the shared crate's logger module
pub use opensearch_vectorized_spi::logger::{init_logger, init_logger_from_env};

// Re-export macros from the shared crate
pub use opensearch_vectorized_spi::{rust_log_info, rust_log_error, rust_log_debug, rust_log_warn, rust_log_trace};
