/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.index.engine.dataformat.RowIdMapping;

/**
 * Result of a native Parquet merge. Bundles the row-ID mapping used to
 * remap row IDs in secondary data formats with the Parquet file metadata
 * (version, row count, {@code created_by}, CRC32) of the merged output file.
 */
public record MergeFilesResult(RowIdMapping rowIdMapping, ParquetFileMetadata metadata) {
}
