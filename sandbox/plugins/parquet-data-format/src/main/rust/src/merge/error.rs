/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::error::Error;

/// Result type alias for merge operations.
pub type MergeResult<T> = Result<T, MergeError>;

/// Unified error type for all merge failures.
#[derive(Debug)]
pub enum MergeError {
    /// Error from the Arrow compute or array layer.
    Arrow(arrow::error::ArrowError),
    /// Error from the Parquet reader or writer.
    Parquet(parquet::errors::ParquetError),
    /// Filesystem or network IO error.
    Io(std::io::Error),
    /// Logic or invariant violation within the merge algorithm.
    Logic(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::Arrow(e) => write!(f, "Arrow error: {e}"),
            MergeError::Parquet(e) => write!(f, "Parquet error: {e}"),
            MergeError::Io(e) => write!(f, "IO error: {e}"),
            MergeError::Logic(s) => write!(f, "{s}"),
        }
    }
}

impl Error for MergeError {}

impl From<arrow::error::ArrowError> for MergeError {
    fn from(e: arrow::error::ArrowError) -> Self {
        MergeError::Arrow(e)
    }
}

impl From<parquet::errors::ParquetError> for MergeError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        MergeError::Parquet(e)
    }
}

impl From<std::io::Error> for MergeError {
    fn from(e: std::io::Error) -> Self {
        MergeError::Io(e)
    }
}
