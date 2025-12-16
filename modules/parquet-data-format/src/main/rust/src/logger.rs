// Re-export init_logger_from_env from the shared crate's logger module
pub use vectorized_exec_spi::logger::init_logger_from_env;

// Re-export macros from the shared crate (only the ones actually used)
pub use vectorized_exec_spi::{log_info, log_error, log_debug};
