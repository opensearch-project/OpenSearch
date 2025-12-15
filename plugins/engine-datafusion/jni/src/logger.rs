
// Re-export everything from the shared crate's logger module
pub use opensearch_vectorized_spi::logger::*;

// Re-export macros from the shared crate
pub use opensearch_vectorized_spi::{rust_log_info, rust_log_warn, rust_log_error, rust_log_debug, rust_log_trace};
