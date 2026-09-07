/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.util.Map;

/**
 * Result of a native Parquet merge. Bundles the per-generation row-ID mappings used to
 * remap row IDs in secondary data formats with the Parquet file metadata
 * (version, row count, {@code created_by}, CRC32) of the merged output file,
 * plus per-merge stats forwarded to the per-shard tracker.
 *
 * @param rowIdMappings           per-generation row-ID remapping tables for secondary data formats
 * @param metadata                Parquet file metadata + CRC32 of the merged output
 * @param flushAndSortChunkCount  count of flush+sort+chunk passes that ran during this merge
 * @param flushAndSortChunkTimeMs cumulative wall-clock millis spent in flush+sort+chunk passes
 * @param rowIdMappingMax         highest row_id assigned during this merge (= rows written)
 */
public record MergeFilesResult(Map<Long, RowIdMapping> rowIdMappings, ParquetFileMetadata metadata, long flushAndSortChunkCount,
    long flushAndSortChunkTimeMs, long rowIdMappingMax) {
}
