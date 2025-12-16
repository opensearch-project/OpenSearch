// Re-export init_logger from the shared crate's logger module
pub use opensearch_vectorized_spi::logger::init_logger;

// Re-export macros from the shared crate (only the ones actually used)
pub use opensearch_vectorized_spi::{rust_log_info, rust_log_error, rust_log_debug};
