/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;

/**
 * Immutable point-in-time snapshot of shard-level Parquet statistics.
 * Produced by {@link ParquetShardStatsTracker#stats()}.
 * Supports serialization via {@link Writeable} and REST rendering via {@link ToXContentFragment}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetShardStats implements DataFormatShardStats, ToXContentFragment, Writeable {

    // Indexing
    private final long docsIndexedTotal;
    private final long docsIndexedFailures;
    private final long indexTimeMillis;

    // VSR Pipeline
    private final long vsrRotationsTotal;
    private final long vsrRotationWaitMillis;
    private final long vsrRowsCurrent;

    // Native Write
    private final long nativeWriteTotal;
    private final long nativeWriteTimeMillis;
    private final long nativeWriteFailures;
    private final long nativeFinalizeTotal;
    private final long nativeFinalizeTimeMillis;
    private final long nativeFinalizeFailures;
    private final long nativeSyncTotal;
    private final long nativeSyncTimeMillis;
    private final long nativeSyncFailures;

    // Sort
    private final long sortTotal;
    private final long sortTimeMillis;
    private final long sortInMemoryTotal;
    private final long sortStreamingTotal;

    // Merge
    private final long mergeTotal;
    private final long mergeTimeMillis;
    private final long mergeFailures;
    private final long mergeInputFilesTotal;
    private final long mergeOutputRowsTotal;

    // Rate Limiting
    private final long rateLimitPauseTimeMillis;
    private final long rateLimitBytesWritten;

    // Memory
    private final long arrowAllocatedBytes;
    private final long arrowMaxBytes;
    private final long rustWriterMemoryBytes;

    // Background Write
    private final long backgroundWriteTotal;
    private final long backgroundWriteWaitMillis;
    private final long backgroundWriteTimeouts;

    /**
     * Constructs a snapshot with all values.
     */
    public ParquetShardStats(
        long docsIndexedTotal,
        long docsIndexedFailures,
        long indexTimeMillis,
        long vsrRotationsTotal,
        long vsrRotationWaitMillis,
        long vsrRowsCurrent,
        long nativeWriteTotal,
        long nativeWriteTimeMillis,
        long nativeWriteFailures,
        long nativeFinalizeTotal,
        long nativeFinalizeTimeMillis,
        long nativeFinalizeFailures,
        long nativeSyncTotal,
        long nativeSyncTimeMillis,
        long nativeSyncFailures,
        long sortTotal,
        long sortTimeMillis,
        long sortInMemoryTotal,
        long sortStreamingTotal,
        long mergeTotal,
        long mergeTimeMillis,
        long mergeFailures,
        long mergeInputFilesTotal,
        long mergeOutputRowsTotal,
        long rateLimitPauseTimeMillis,
        long rateLimitBytesWritten,
        long arrowAllocatedBytes,
        long arrowMaxBytes,
        long rustWriterMemoryBytes,
        long backgroundWriteTotal,
        long backgroundWriteWaitMillis,
        long backgroundWriteTimeouts
    ) {
        this.docsIndexedTotal = docsIndexedTotal;
        this.docsIndexedFailures = docsIndexedFailures;
        this.indexTimeMillis = indexTimeMillis;
        this.vsrRotationsTotal = vsrRotationsTotal;
        this.vsrRotationWaitMillis = vsrRotationWaitMillis;
        this.vsrRowsCurrent = vsrRowsCurrent;
        this.nativeWriteTotal = nativeWriteTotal;
        this.nativeWriteTimeMillis = nativeWriteTimeMillis;
        this.nativeWriteFailures = nativeWriteFailures;
        this.nativeFinalizeTotal = nativeFinalizeTotal;
        this.nativeFinalizeTimeMillis = nativeFinalizeTimeMillis;
        this.nativeFinalizeFailures = nativeFinalizeFailures;
        this.nativeSyncTotal = nativeSyncTotal;
        this.nativeSyncTimeMillis = nativeSyncTimeMillis;
        this.nativeSyncFailures = nativeSyncFailures;
        this.sortTotal = sortTotal;
        this.sortTimeMillis = sortTimeMillis;
        this.sortInMemoryTotal = sortInMemoryTotal;
        this.sortStreamingTotal = sortStreamingTotal;
        this.mergeTotal = mergeTotal;
        this.mergeTimeMillis = mergeTimeMillis;
        this.mergeFailures = mergeFailures;
        this.mergeInputFilesTotal = mergeInputFilesTotal;
        this.mergeOutputRowsTotal = mergeOutputRowsTotal;
        this.rateLimitPauseTimeMillis = rateLimitPauseTimeMillis;
        this.rateLimitBytesWritten = rateLimitBytesWritten;
        this.arrowAllocatedBytes = arrowAllocatedBytes;
        this.arrowMaxBytes = arrowMaxBytes;
        this.rustWriterMemoryBytes = rustWriterMemoryBytes;
        this.backgroundWriteTotal = backgroundWriteTotal;
        this.backgroundWriteWaitMillis = backgroundWriteWaitMillis;
        this.backgroundWriteTimeouts = backgroundWriteTimeouts;
    }

    public ParquetShardStats(StreamInput in) throws IOException {
        // Indexing
        this.docsIndexedTotal = in.readVLong();
        this.docsIndexedFailures = in.readVLong();
        this.indexTimeMillis = in.readVLong();

        // VSR
        this.vsrRotationsTotal = in.readVLong();
        this.vsrRotationWaitMillis = in.readVLong();
        this.vsrRowsCurrent = in.readVLong();

        // Native Write
        this.nativeWriteTotal = in.readVLong();
        this.nativeWriteTimeMillis = in.readVLong();
        this.nativeWriteFailures = in.readVLong();
        this.nativeFinalizeTotal = in.readVLong();
        this.nativeFinalizeTimeMillis = in.readVLong();
        this.nativeFinalizeFailures = in.readVLong();
        this.nativeSyncTotal = in.readVLong();
        this.nativeSyncTimeMillis = in.readVLong();
        this.nativeSyncFailures = in.readVLong();

        // Sort
        this.sortTotal = in.readVLong();
        this.sortTimeMillis = in.readVLong();
        this.sortInMemoryTotal = in.readVLong();
        this.sortStreamingTotal = in.readVLong();

        // Merge
        this.mergeTotal = in.readVLong();
        this.mergeTimeMillis = in.readVLong();
        this.mergeFailures = in.readVLong();
        this.mergeInputFilesTotal = in.readVLong();
        this.mergeOutputRowsTotal = in.readVLong();

        // Rate Limiting
        this.rateLimitPauseTimeMillis = in.readVLong();
        this.rateLimitBytesWritten = in.readVLong();

        // Memory
        this.arrowAllocatedBytes = in.readVLong();
        this.arrowMaxBytes = in.readVLong();
        this.rustWriterMemoryBytes = in.readVLong();

        // Background Write
        this.backgroundWriteTotal = in.readVLong();
        this.backgroundWriteWaitMillis = in.readVLong();
        this.backgroundWriteTimeouts = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Indexing
        out.writeVLong(docsIndexedTotal);
        out.writeVLong(docsIndexedFailures);
        out.writeVLong(indexTimeMillis);

        // VSR
        out.writeVLong(vsrRotationsTotal);
        out.writeVLong(vsrRotationWaitMillis);
        out.writeVLong(vsrRowsCurrent);

        // Native Write
        out.writeVLong(nativeWriteTotal);
        out.writeVLong(nativeWriteTimeMillis);
        out.writeVLong(nativeWriteFailures);
        out.writeVLong(nativeFinalizeTotal);
        out.writeVLong(nativeFinalizeTimeMillis);
        out.writeVLong(nativeFinalizeFailures);
        out.writeVLong(nativeSyncTotal);
        out.writeVLong(nativeSyncTimeMillis);
        out.writeVLong(nativeSyncFailures);

        // Sort
        out.writeVLong(sortTotal);
        out.writeVLong(sortTimeMillis);
        out.writeVLong(sortInMemoryTotal);
        out.writeVLong(sortStreamingTotal);

        // Merge
        out.writeVLong(mergeTotal);
        out.writeVLong(mergeTimeMillis);
        out.writeVLong(mergeFailures);
        out.writeVLong(mergeInputFilesTotal);
        out.writeVLong(mergeOutputRowsTotal);

        // Rate Limiting
        out.writeVLong(rateLimitPauseTimeMillis);
        out.writeVLong(rateLimitBytesWritten);

        // Memory
        out.writeVLong(arrowAllocatedBytes);
        out.writeVLong(arrowMaxBytes);
        out.writeVLong(rustWriterMemoryBytes);

        // Background Write
        out.writeVLong(backgroundWriteTotal);
        out.writeVLong(backgroundWriteWaitMillis);
        out.writeVLong(backgroundWriteTimeouts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Indexing
        builder.startObject("indexing");
        builder.field("docs_indexed_total", docsIndexedTotal);
        builder.field("docs_indexed_failures", docsIndexedFailures);
        builder.field("index_time_millis", indexTimeMillis);
        builder.endObject();

        // VSR
        builder.startObject("vsr");
        builder.field("vsr_rotations_total", vsrRotationsTotal);
        builder.field("vsr_rotation_wait_millis", vsrRotationWaitMillis);
        builder.field("vsr_rows_current", vsrRowsCurrent);
        builder.endObject();

        // Native Write
        builder.startObject("native_write");
        builder.field("native_write_total", nativeWriteTotal);
        builder.field("native_write_time_millis", nativeWriteTimeMillis);
        builder.field("native_write_failures", nativeWriteFailures);
        builder.field("native_finalize_total", nativeFinalizeTotal);
        builder.field("native_finalize_time_millis", nativeFinalizeTimeMillis);
        builder.field("native_finalize_failures", nativeFinalizeFailures);
        builder.field("native_sync_total", nativeSyncTotal);
        builder.field("native_sync_time_millis", nativeSyncTimeMillis);
        builder.field("native_sync_failures", nativeSyncFailures);
        builder.endObject();

        // Sort
        builder.startObject("sort");
        builder.field("sort_total", sortTotal);
        builder.field("sort_time_millis", sortTimeMillis);
        builder.field("sort_in_memory_total", sortInMemoryTotal);
        builder.field("sort_streaming_total", sortStreamingTotal);
        builder.endObject();

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal);
        builder.field("merge_time_millis", mergeTimeMillis);
        builder.field("merge_failures", mergeFailures);
        builder.field("merge_input_files_total", mergeInputFilesTotal);
        builder.field("merge_output_rows_total", mergeOutputRowsTotal);
        builder.endObject();

        // Rate Limiting
        builder.startObject("rate_limit");
        builder.field("rate_limit_pause_time_millis", rateLimitPauseTimeMillis);
        builder.field("rate_limit_bytes_written", rateLimitBytesWritten);
        builder.endObject();

        // Memory
        builder.startObject("memory");
        builder.field("arrow_allocated_bytes", arrowAllocatedBytes);
        builder.field("arrow_max_bytes", arrowMaxBytes);
        builder.field("rust_writer_memory_bytes", rustWriterMemoryBytes);
        builder.endObject();

        // Background Write
        builder.startObject("background_write");
        builder.field("background_write_total", backgroundWriteTotal);
        builder.field("background_write_wait_millis", backgroundWriteWaitMillis);
        builder.field("background_write_timeouts", backgroundWriteTimeouts);
        builder.endObject();

        return builder;
    }

    /**
     * Returns a new snapshot that is the sum of this snapshot and another.
     * Used for aggregation across shards.
     */
    public ParquetShardStats add(ParquetShardStats other) {
        return new ParquetShardStats(
            this.docsIndexedTotal + other.docsIndexedTotal,
            this.docsIndexedFailures + other.docsIndexedFailures,
            this.indexTimeMillis + other.indexTimeMillis,
            this.vsrRotationsTotal + other.vsrRotationsTotal,
            this.vsrRotationWaitMillis + other.vsrRotationWaitMillis,
            this.vsrRowsCurrent + other.vsrRowsCurrent,
            this.nativeWriteTotal + other.nativeWriteTotal,
            this.nativeWriteTimeMillis + other.nativeWriteTimeMillis,
            this.nativeWriteFailures + other.nativeWriteFailures,
            this.nativeFinalizeTotal + other.nativeFinalizeTotal,
            this.nativeFinalizeTimeMillis + other.nativeFinalizeTimeMillis,
            this.nativeFinalizeFailures + other.nativeFinalizeFailures,
            this.nativeSyncTotal + other.nativeSyncTotal,
            this.nativeSyncTimeMillis + other.nativeSyncTimeMillis,
            this.nativeSyncFailures + other.nativeSyncFailures,
            this.sortTotal + other.sortTotal,
            this.sortTimeMillis + other.sortTimeMillis,
            this.sortInMemoryTotal + other.sortInMemoryTotal,
            this.sortStreamingTotal + other.sortStreamingTotal,
            this.mergeTotal + other.mergeTotal,
            this.mergeTimeMillis + other.mergeTimeMillis,
            this.mergeFailures + other.mergeFailures,
            this.mergeInputFilesTotal + other.mergeInputFilesTotal,
            this.mergeOutputRowsTotal + other.mergeOutputRowsTotal,
            this.rateLimitPauseTimeMillis + other.rateLimitPauseTimeMillis,
            this.rateLimitBytesWritten + other.rateLimitBytesWritten,
            this.arrowAllocatedBytes + other.arrowAllocatedBytes,
            this.arrowMaxBytes + other.arrowMaxBytes,
            this.rustWriterMemoryBytes + other.rustWriterMemoryBytes,
            this.backgroundWriteTotal + other.backgroundWriteTotal,
            this.backgroundWriteWaitMillis + other.backgroundWriteWaitMillis,
            this.backgroundWriteTimeouts + other.backgroundWriteTimeouts
        );
    }

    // --- Getters ---

    public long getDocsIndexedTotal() {
        return docsIndexedTotal;
    }

    public long getDocsIndexedFailures() {
        return docsIndexedFailures;
    }

    public long getIndexTimeMillis() {
        return indexTimeMillis;
    }

    public long getVsrRotationsTotal() {
        return vsrRotationsTotal;
    }

    public long getVsrRotationWaitMillis() {
        return vsrRotationWaitMillis;
    }

    public long getVsrRowsCurrent() {
        return vsrRowsCurrent;
    }

    public long getNativeWriteTotal() {
        return nativeWriteTotal;
    }

    public long getNativeWriteTimeMillis() {
        return nativeWriteTimeMillis;
    }

    public long getNativeWriteFailures() {
        return nativeWriteFailures;
    }

    public long getNativeFinalizeTotal() {
        return nativeFinalizeTotal;
    }

    public long getNativeFinalizeTimeMillis() {
        return nativeFinalizeTimeMillis;
    }

    public long getNativeFinalizeFailures() {
        return nativeFinalizeFailures;
    }

    public long getNativeSyncTotal() {
        return nativeSyncTotal;
    }

    public long getNativeSyncTimeMillis() {
        return nativeSyncTimeMillis;
    }

    public long getNativeSyncFailures() {
        return nativeSyncFailures;
    }

    public long getSortTotal() {
        return sortTotal;
    }

    public long getSortTimeMillis() {
        return sortTimeMillis;
    }

    public long getSortInMemoryTotal() {
        return sortInMemoryTotal;
    }

    public long getSortStreamingTotal() {
        return sortStreamingTotal;
    }

    public long getMergeTotal() {
        return mergeTotal;
    }

    public long getMergeTimeMillis() {
        return mergeTimeMillis;
    }

    public long getMergeFailures() {
        return mergeFailures;
    }

    public long getMergeInputFilesTotal() {
        return mergeInputFilesTotal;
    }

    public long getMergeOutputRowsTotal() {
        return mergeOutputRowsTotal;
    }

    public long getRateLimitPauseTimeMillis() {
        return rateLimitPauseTimeMillis;
    }

    public long getRateLimitBytesWritten() {
        return rateLimitBytesWritten;
    }

    public long getArrowAllocatedBytes() {
        return arrowAllocatedBytes;
    }

    public long getArrowMaxBytes() {
        return arrowMaxBytes;
    }

    public long getRustWriterMemoryBytes() {
        return rustWriterMemoryBytes;
    }

    public long getBackgroundWriteTotal() {
        return backgroundWriteTotal;
    }

    public long getBackgroundWriteWaitMillis() {
        return backgroundWriteWaitMillis;
    }

    public long getBackgroundWriteTimeouts() {
        return backgroundWriteTimeouts;
    }
}
