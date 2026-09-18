/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The opensearch Parquet format version: the footer key and the current version. Lives here so
//! the writer (`opensearch-parquet-format`) and readers (the doc-values cursor) share one
//! definition and cannot drift. The footer key-value lookup itself stays with each caller; this
//! module is deliberately free of parquet types.

/// Parquet file-level metadata key for the opensearch-defined parquet format version.
pub const FORMAT_VERSION_KEY: &str = "opensearch.format_version";

/// Current parquet format version stamped by the writer: a plain monotonic integer starting at
/// 1, in a plugin-defined namespace — NOT comparable to Lucene or other format versions, and not
/// tied to any release version. Increment when the writer output or the reader's expectations
/// change. Must stay in sync with the Java constant
/// `ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION`.
pub const FORMAT_VERSION: &str = "1";

/// Sentinel for a file whose footer carries no parseable opensearch format version.
pub const FORMAT_VERSION_UNKNOWN: i64 = 0;

/// Parses a footer version stamp as a plain positive integer.
///
/// Returns [`FORMAT_VERSION_UNKNOWN`] when the input is empty, non-numeric, or not positive.
/// This is reporting only - it never rejects; the Java codec owns the accept/reject decision.
pub fn parse_format_version(raw: &str) -> i64 {
    match raw.parse::<i64>() {
        Ok(value) if value > 0 => value,
        _ => FORMAT_VERSION_UNKNOWN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_version_reads_the_plain_integer_stamp() {
        // "1" is what the writer stamps today; equals ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION.
        assert_eq!(parse_format_version(FORMAT_VERSION), 1);
        assert_eq!(parse_format_version("42"), 42);
        // Unstamped, malformed, non-positive, or dotted stamps report unknown, never a partial value.
        assert_eq!(parse_format_version(""), FORMAT_VERSION_UNKNOWN);
        assert_eq!(parse_format_version("0"), FORMAT_VERSION_UNKNOWN);
        assert_eq!(parse_format_version("-1"), FORMAT_VERSION_UNKNOWN);
        assert_eq!(parse_format_version("1.0.0.0"), FORMAT_VERSION_UNKNOWN);
    }
}
