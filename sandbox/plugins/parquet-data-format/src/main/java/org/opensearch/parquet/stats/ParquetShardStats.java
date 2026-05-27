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
public class ParquetShardStats implements DataFormatShardStats<ParquetShardStats> {

    // Indexing
    private final long docsIndexedTotal;
    private final long indexTimeMillis;

    // VSR Pipeline
    private final long vsrRotationsTotal;

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

    // Merge
    private final long mergeTotal;
    private final long mergeTimeMillis;
    private final long mergeFailures;
    private final long mergeInputFilesTotal;
    private final long mergeOutputRowsTotal;

    // Background Write
    private final long backgroundWriteTotal;
    private final long backgroundWriteWaitMillis;
    private final long backgroundWriteTimeouts;
    private final long backgroundWriteFailures;

    /**
     * Returns an empty ParquetShardStats snapshot with all zero counters.
     * Used by transport actions when a shard does not have a Parquet primary delegate.
     */
    public static ParquetShardStats empty() {
        return new ParquetShardStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /**
     * Constructs a snapshot with all values.
     */
    public ParquetShardStats(
        long docsIndexedTotal,
        long indexTimeMillis,
        long vsrRotationsTotal,
        long nativeWriteTotal,
        long nativeWriteTimeMillis,
        long nativeWriteFailures,
        long nativeFinalizeTotal,
        long nativeFinalizeTimeMillis,
        long nativeFinalizeFailures,
        long nativeSyncTotal,
        long nativeSyncTimeMillis,
        long nativeSyncFailures,
        long mergeTotal,
        long mergeTimeMillis,
        long mergeFailures,
        long mergeInputFilesTotal,
        long mergeOutputRowsTotal,
        long backgroundWriteTotal,
        long backgroundWriteWaitMillis,
        long backgroundWriteTimeouts,
        long backgroundWriteFailures
    ) {
        this.docsIndexedTotal = docsIndexedTotal;
        this.indexTimeMillis = indexTimeMillis;
        this.vsrRotationsTotal = vsrRotationsTotal;
        this.nativeWriteTotal = nativeWriteTotal;
        this.nativeWriteTimeMillis = nativeWriteTimeMillis;
        this.nativeWriteFailures = nativeWriteFailures;
        this.nativeFinalizeTotal = nativeFinalizeTotal;
        this.nativeFinalizeTimeMillis = nativeFinalizeTimeMillis;
        this.nativeFinalizeFailures = nativeFinalizeFailures;
        this.nativeSyncTotal = nativeSyncTotal;
        this.nativeSyncTimeMillis = nativeSyncTimeMillis;
        this.nativeSyncFailures = nativeSyncFailures;
        this.mergeTotal = mergeTotal;
        this.mergeTimeMillis = mergeTimeMillis;
        this.mergeFailures = mergeFailures;
        this.mergeInputFilesTotal = mergeInputFilesTotal;
        this.mergeOutputRowsTotal = mergeOutputRowsTotal;
        this.backgroundWriteTotal = backgroundWriteTotal;
        this.backgroundWriteWaitMillis = backgroundWriteWaitMillis;
        this.backgroundWriteTimeouts = backgroundWriteTimeouts;
        this.backgroundWriteFailures = backgroundWriteFailures;
    }

    public ParquetShardStats(StreamInput in) throws IOException {
        // Indexing
        this.docsIndexedTotal = in.readVLong();
        this.indexTimeMillis = in.readVLong();

        // VSR
        this.vsrRotationsTotal = in.readVLong();

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

        // Merge
        this.mergeTotal = in.readVLong();
        this.mergeTimeMillis = in.readVLong();
        this.mergeFailures = in.readVLong();
        this.mergeInputFilesTotal = in.readVLong();
        this.mergeOutputRowsTotal = in.readVLong();

        // Background Write
        this.backgroundWriteTotal = in.readVLong();
        this.backgroundWriteWaitMillis = in.readVLong();
        this.backgroundWriteTimeouts = in.readVLong();
        this.backgroundWriteFailures = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Indexing
        out.writeVLong(docsIndexedTotal);
        out.writeVLong(indexTimeMillis);

        // VSR
        out.writeVLong(vsrRotationsTotal);

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

        // Merge
        out.writeVLong(mergeTotal);
        out.writeVLong(mergeTimeMillis);
        out.writeVLong(mergeFailures);
        out.writeVLong(mergeInputFilesTotal);
        out.writeVLong(mergeOutputRowsTotal);

        // Background Write
        out.writeVLong(backgroundWriteTotal);
        out.writeVLong(backgroundWriteWaitMillis);
        out.writeVLong(backgroundWriteTimeouts);
        out.writeVLong(backgroundWriteFailures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Indexing
        builder.startObject("indexing");
        builder.field("docs_indexed_total", docsIndexedTotal);
        builder.field("index_time_millis", indexTimeMillis);
        builder.endObject();

        // VSR
        builder.startObject("vsr");
        builder.field("vsr_rotations_total", vsrRotationsTotal);
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

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal);
        builder.field("merge_time_millis", mergeTimeMillis);
        builder.field("merge_failures", mergeFailures);
        builder.field("merge_input_files_total", mergeInputFilesTotal);
        builder.field("merge_output_rows_total", mergeOutputRowsTotal);
        builder.endObject();

        // Background Write
        builder.startObject("background_write");
        builder.field("background_write_total", backgroundWriteTotal);
        builder.field("background_write_wait_millis", backgroundWriteWaitMillis);
        builder.field("background_write_timeouts", backgroundWriteTimeouts);
        builder.field("background_write_failures", backgroundWriteFailures);
        builder.endObject();

        return builder;
    }

    /**
     * Returns a new snapshot that is the sum of this snapshot and another.
     * Used for aggregation across shards.
     */
    @Override
    public ParquetShardStats add(ParquetShardStats other) {
        return new ParquetShardStats(
            this.docsIndexedTotal + other.docsIndexedTotal,
            this.indexTimeMillis + other.indexTimeMillis,
            this.vsrRotationsTotal + other.vsrRotationsTotal,
            this.nativeWriteTotal + other.nativeWriteTotal,
            this.nativeWriteTimeMillis + other.nativeWriteTimeMillis,
            this.nativeWriteFailures + other.nativeWriteFailures,
            this.nativeFinalizeTotal + other.nativeFinalizeTotal,
            this.nativeFinalizeTimeMillis + other.nativeFinalizeTimeMillis,
            this.nativeFinalizeFailures + other.nativeFinalizeFailures,
            this.nativeSyncTotal + other.nativeSyncTotal,
            this.nativeSyncTimeMillis + other.nativeSyncTimeMillis,
            this.nativeSyncFailures + other.nativeSyncFailures,
            this.mergeTotal + other.mergeTotal,
            this.mergeTimeMillis + other.mergeTimeMillis,
            this.mergeFailures + other.mergeFailures,
            this.mergeInputFilesTotal + other.mergeInputFilesTotal,
            this.mergeOutputRowsTotal + other.mergeOutputRowsTotal,
            this.backgroundWriteTotal + other.backgroundWriteTotal,
            this.backgroundWriteWaitMillis + other.backgroundWriteWaitMillis,
            this.backgroundWriteTimeouts + other.backgroundWriteTimeouts,
            this.backgroundWriteFailures + other.backgroundWriteFailures
        );
    }
}
