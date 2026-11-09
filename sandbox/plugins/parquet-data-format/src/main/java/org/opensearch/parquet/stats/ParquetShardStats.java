/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.Nullable;
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
    // Writes rejected by the parquet_native_write pool when its bounded queue is full.
    private final long nativeWriteRejections;
    private final long nativeFinalizeTotal;
    private final long nativeFinalizeTimeMillis;
    private final long nativeFinalizeFailures;

    // Merge
    private final long mergeTotal;
    private final long mergeTimeMillis;
    private final long mergeFailures;
    private final long mergeInputFilesTotal;
    private final long mergeOutputRowsTotal;
    // Per-merge: cumulative count + millis for the flush+sort+chunk pass that runs inside each merge.
    private final long flushAndSortChunkTotal;
    private final long flushAndSortChunkTimeMillis;
    // Highest row_id assigned during any merge of this shard (max-of-maxes when cross-aggregated).
    private final long rowIdMappingMax;

    // Background Write
    private final long backgroundWriteTotal;
    private final long backgroundWriteWaitMillis;
    private final long backgroundWriteTimeouts;
    private final long backgroundWriteFailures;

    // Native Runtime (node-level only, null on shard trackers and cross-node aggregation)
    @Nullable
    private final ParquetNativeRuntimeStats nativeRuntime;

    // Ingest thread-pool snapshot (node-level only, null on shard trackers and cross-node aggregation)
    @Nullable
    private final ParquetIngestPoolStats ingestPool;

    /**
     * Returns an empty ParquetShardStats snapshot with all zero counters.
     * Used by transport actions when a shard does not have a Parquet primary delegate.
     */
    public static ParquetShardStats empty() {
        return new ParquetShardStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
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
        long nativeWriteRejections,
        long nativeFinalizeTotal,
        long nativeFinalizeTimeMillis,
        long nativeFinalizeFailures,
        long mergeTotal,
        long mergeTimeMillis,
        long mergeFailures,
        long mergeInputFilesTotal,
        long mergeOutputRowsTotal,
        long flushAndSortChunkTotal,
        long flushAndSortChunkTimeMillis,
        long rowIdMappingMax,
        long backgroundWriteTotal,
        long backgroundWriteWaitMillis,
        long backgroundWriteTimeouts,
        long backgroundWriteFailures
    ) {
        this(
            docsIndexedTotal,
            indexTimeMillis,
            vsrRotationsTotal,
            nativeWriteTotal,
            nativeWriteTimeMillis,
            nativeWriteFailures,
            nativeWriteRejections,
            nativeFinalizeTotal,
            nativeFinalizeTimeMillis,
            nativeFinalizeFailures,
            mergeTotal,
            mergeTimeMillis,
            mergeFailures,
            mergeInputFilesTotal,
            mergeOutputRowsTotal,
            flushAndSortChunkTotal,
            flushAndSortChunkTimeMillis,
            rowIdMappingMax,
            backgroundWriteTotal,
            backgroundWriteWaitMillis,
            backgroundWriteTimeouts,
            backgroundWriteFailures,
            null,
            null
        );
    }

    /**
     * Constructs a snapshot with all values including optional native runtime stats.
     */
    public ParquetShardStats(
        long docsIndexedTotal,
        long indexTimeMillis,
        long vsrRotationsTotal,
        long nativeWriteTotal,
        long nativeWriteTimeMillis,
        long nativeWriteFailures,
        long nativeWriteRejections,
        long nativeFinalizeTotal,
        long nativeFinalizeTimeMillis,
        long nativeFinalizeFailures,
        long mergeTotal,
        long mergeTimeMillis,
        long mergeFailures,
        long mergeInputFilesTotal,
        long mergeOutputRowsTotal,
        long flushAndSortChunkTotal,
        long flushAndSortChunkTimeMillis,
        long rowIdMappingMax,
        long backgroundWriteTotal,
        long backgroundWriteWaitMillis,
        long backgroundWriteTimeouts,
        long backgroundWriteFailures,
        @Nullable ParquetNativeRuntimeStats nativeRuntime,
        @Nullable ParquetIngestPoolStats ingestPool
    ) {
        this.docsIndexedTotal = docsIndexedTotal;
        this.indexTimeMillis = indexTimeMillis;
        this.vsrRotationsTotal = vsrRotationsTotal;
        this.nativeWriteTotal = nativeWriteTotal;
        this.nativeWriteTimeMillis = nativeWriteTimeMillis;
        this.nativeWriteFailures = nativeWriteFailures;
        this.nativeWriteRejections = nativeWriteRejections;
        this.nativeFinalizeTotal = nativeFinalizeTotal;
        this.nativeFinalizeTimeMillis = nativeFinalizeTimeMillis;
        this.nativeFinalizeFailures = nativeFinalizeFailures;
        this.mergeTotal = mergeTotal;
        this.mergeTimeMillis = mergeTimeMillis;
        this.mergeFailures = mergeFailures;
        this.mergeInputFilesTotal = mergeInputFilesTotal;
        this.mergeOutputRowsTotal = mergeOutputRowsTotal;
        this.flushAndSortChunkTotal = flushAndSortChunkTotal;
        this.flushAndSortChunkTimeMillis = flushAndSortChunkTimeMillis;
        this.rowIdMappingMax = rowIdMappingMax;
        this.backgroundWriteTotal = backgroundWriteTotal;
        this.backgroundWriteWaitMillis = backgroundWriteWaitMillis;
        this.backgroundWriteTimeouts = backgroundWriteTimeouts;
        this.backgroundWriteFailures = backgroundWriteFailures;
        this.nativeRuntime = nativeRuntime;
        this.ingestPool = ingestPool;
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
        this.nativeWriteRejections = in.readVLong();
        this.nativeFinalizeTotal = in.readVLong();
        this.nativeFinalizeTimeMillis = in.readVLong();
        this.nativeFinalizeFailures = in.readVLong();

        // Merge
        this.mergeTotal = in.readVLong();
        this.mergeTimeMillis = in.readVLong();
        this.mergeFailures = in.readVLong();
        this.mergeInputFilesTotal = in.readVLong();
        this.mergeOutputRowsTotal = in.readVLong();
        this.flushAndSortChunkTotal = in.readVLong();
        this.flushAndSortChunkTimeMillis = in.readVLong();
        this.rowIdMappingMax = in.readVLong();

        // Background Write
        this.backgroundWriteTotal = in.readVLong();
        this.backgroundWriteWaitMillis = in.readVLong();
        this.backgroundWriteTimeouts = in.readVLong();
        this.backgroundWriteFailures = in.readVLong();

        // Native Runtime
        this.nativeRuntime = in.readOptionalWriteable(ParquetNativeRuntimeStats::new);
        this.ingestPool = in.readOptionalWriteable(ParquetIngestPoolStats::new);
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
        out.writeVLong(nativeWriteRejections);
        out.writeVLong(nativeFinalizeTotal);
        out.writeVLong(nativeFinalizeTimeMillis);
        out.writeVLong(nativeFinalizeFailures);

        // Merge
        out.writeVLong(mergeTotal);
        out.writeVLong(mergeTimeMillis);
        out.writeVLong(mergeFailures);
        out.writeVLong(mergeInputFilesTotal);
        out.writeVLong(mergeOutputRowsTotal);
        out.writeVLong(flushAndSortChunkTotal);
        out.writeVLong(flushAndSortChunkTimeMillis);
        out.writeVLong(rowIdMappingMax);

        // Background Write
        out.writeVLong(backgroundWriteTotal);
        out.writeVLong(backgroundWriteWaitMillis);
        out.writeVLong(backgroundWriteTimeouts);
        out.writeVLong(backgroundWriteFailures);

        // Native Runtime
        out.writeOptionalWriteable(nativeRuntime);
        out.writeOptionalWriteable(ingestPool);
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
        builder.field("native_write_rejections", nativeWriteRejections);
        builder.field("native_finalize_total", nativeFinalizeTotal);
        builder.field("native_finalize_time_millis", nativeFinalizeTimeMillis);
        builder.field("native_finalize_failures", nativeFinalizeFailures);
        builder.endObject();

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal);
        builder.field("merge_time_millis", mergeTimeMillis);
        builder.field("merge_failures", mergeFailures);
        builder.field("merge_input_files_total", mergeInputFilesTotal);
        builder.field("merge_output_rows_total", mergeOutputRowsTotal);
        builder.field("flush_and_sort_chunk_total", flushAndSortChunkTotal);
        builder.field("flush_and_sort_chunk_time_millis", flushAndSortChunkTimeMillis);
        builder.field("row_id_mapping_max", rowIdMappingMax);
        builder.endObject();

        // Background Write
        builder.startObject("background_write");
        builder.field("background_write_total", backgroundWriteTotal);
        builder.field("background_write_wait_millis", backgroundWriteWaitMillis);
        builder.field("background_write_timeouts", backgroundWriteTimeouts);
        builder.field("background_write_failures", backgroundWriteFailures);
        builder.endObject();

        // Native Runtime (only present at node-level aggregate)
        if (nativeRuntime != null) {
            nativeRuntime.toXContent(builder, params);
        }

        // Ingest thread-pool (only present at node-level aggregate)
        if (ingestPool != null) {
            ingestPool.toXContent(builder, params);
        }

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
            this.nativeWriteRejections + other.nativeWriteRejections,
            this.nativeFinalizeTotal + other.nativeFinalizeTotal,
            this.nativeFinalizeTimeMillis + other.nativeFinalizeTimeMillis,
            this.nativeFinalizeFailures + other.nativeFinalizeFailures,
            this.mergeTotal + other.mergeTotal,
            this.mergeTimeMillis + other.mergeTimeMillis,
            this.mergeFailures + other.mergeFailures,
            this.mergeInputFilesTotal + other.mergeInputFilesTotal,
            this.mergeOutputRowsTotal + other.mergeOutputRowsTotal,
            this.flushAndSortChunkTotal + other.flushAndSortChunkTotal,
            this.flushAndSortChunkTimeMillis + other.flushAndSortChunkTimeMillis,
            // row_id_mapping_max composes as max-of-maxes (sum is meaningless for a max).
            Math.max(this.rowIdMappingMax, other.rowIdMappingMax),
            this.backgroundWriteTotal + other.backgroundWriteTotal,
            this.backgroundWriteWaitMillis + other.backgroundWriteWaitMillis,
            this.backgroundWriteTimeouts + other.backgroundWriteTimeouts,
            this.backgroundWriteFailures + other.backgroundWriteFailures,
            null,  // drop nativeRuntime on cross-node aggregation
            null   // drop ingestPool on cross-node aggregation
        );
    }

    /**
     * Returns a copy of this with the node-level runtime and ingest-pool stats attached. Used by
     * {@link ParquetStatsProvider} to decorate the per-node aggregate; either argument may be null.
     */
    public ParquetShardStats withNodeStats(ParquetNativeRuntimeStats runtime, ParquetIngestPoolStats pool) {
        return new ParquetShardStats(
            docsIndexedTotal,
            indexTimeMillis,
            vsrRotationsTotal,
            nativeWriteTotal,
            nativeWriteTimeMillis,
            nativeWriteFailures,
            nativeWriteRejections,
            nativeFinalizeTotal,
            nativeFinalizeTimeMillis,
            nativeFinalizeFailures,
            mergeTotal,
            mergeTimeMillis,
            mergeFailures,
            mergeInputFilesTotal,
            mergeOutputRowsTotal,
            flushAndSortChunkTotal,
            flushAndSortChunkTimeMillis,
            rowIdMappingMax,
            backgroundWriteTotal,
            backgroundWriteWaitMillis,
            backgroundWriteTimeouts,
            backgroundWriteFailures,
            runtime,
            pool
        );
    }
}
