/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared definition of the opensearch Parquet format-version key, the current version, and its
//! long encoding, plus the writer-generation key, used by both the writer
//! (`opensearch-parquet-format`) and readers (the doc-values cursor). Free of parquet types; the
//! footer lookup stays with each caller.

/// Parquet file-level metadata key for the opensearch-defined parquet format version.
pub const FORMAT_VERSION_KEY: &str = "opensearch.format_version";

/// Parquet file-level metadata key for the writer generation stamped into the footer. Shared by the
/// writer (`opensearch-parquet-format`), which stamps it, and the doc-values reader, which reads it
/// back to confirm a file was written alongside the Lucene segment that points at it, so the two
/// sides cannot drift on the key spelling.
pub const WRITER_GENERATION_KEY: &str = "opensearch.writer_generation";

/// Current parquet format version stamped by the writer. Plugin-defined namespace; not
/// comparable to Lucene or other format versions. Increment when the writer output or the
/// reader's expectations change. Must stay in sync with the Java constant
/// `ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION`.
pub const FORMAT_VERSION: &str = "1.0.0.0";

/// Sentinel for a file whose footer carries no parseable opensearch format version.
pub const FORMAT_VERSION_UNKNOWN: i64 = 0;

/// Long-encodes a `major.minor.patch[.build]` version string as
/// `major * 1_000_000 + minor * 1_000 + patch`, the same encoding as the Java constant
/// `ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION`, so the two are directly comparable.
///
/// Returns [`FORMAT_VERSION_UNKNOWN`] when the input is empty or not parseable. This is
/// reporting only - it never rejects; the Java codec owns the accept/reject decision.
pub fn encode_format_version(raw: &str) -> i64 {
    if raw.is_empty() {
        return FORMAT_VERSION_UNKNOWN;
    }
    let mut parts = raw.split('.');
    let mut encoded = 0i64;
    for scale in [1_000_000i64, 1_000, 1] {
        // A missing minor/patch reads as 0 ("1" and "1.0.0" encode identically); a present but
        // non-numeric or negative component makes the whole version unusable.
        let part = match parts.next() {
            None => break,
            Some(part) => part,
        };
        match part.parse::<i64>() {
            Ok(value) if value >= 0 => encoded += value * scale,
            _ => return FORMAT_VERSION_UNKNOWN,
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_format_version_is_long_encoded_like_the_java_constant() {
        // "1.0.0.0" is what the writer stamps; the fourth component is ignored, so this
        // equals ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION.
        assert_eq!(encode_format_version(FORMAT_VERSION), 1_000_000);
        assert_eq!(encode_format_version("2.3.4"), 2_003_004);
        // A missing minor/patch reads as zero, so "1" and "1.0.0" encode identically.
        assert_eq!(encode_format_version("1"), 1_000_000);
        // Unstamped / malformed report unknown, never a partial value.
        assert_eq!(encode_format_version(""), FORMAT_VERSION_UNKNOWN);
        assert_eq!(encode_format_version("1.x.0"), FORMAT_VERSION_UNKNOWN);
        assert_eq!(encode_format_version("-1.0.0"), FORMAT_VERSION_UNKNOWN);
    }
}
