/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

/**
 * Metadata extracted from a Parquet file after the native writer is closed.
 *
 * <p>Returned by {@link RustBridge#finalizeWriter(long)} and {@link RustBridge#getFileMetadata(String)}.
 * Contains the Parquet format version, total row count, the creator identifier string
 * embedded in the file footer, and the whole-file CRC32 checksum computed during write.
 *
 * @param version   Parquet format version number
 * @param numRows   total number of rows written to the file
 * @param createdBy creator string from the Parquet file footer metadata
 * @param crc32     whole-file CRC32 checksum (computed by the Rust writer during write)
 */
public record ParquetFileMetadata(int version, long numRows, String createdBy, long crc32) {
}
