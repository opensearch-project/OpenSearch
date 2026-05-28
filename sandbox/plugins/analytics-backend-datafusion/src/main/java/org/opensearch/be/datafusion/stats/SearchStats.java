/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Cumulative indexed-search execution metrics, accumulated by the native
 * {@code SEARCH_STATS} static across all partition stream lifetimes since
 * process start.
 *
 * <p>Mirrors Rust's {@code SearchStatsRepr}: one lifecycle counter,
 * 18 raw counts and 11 elapsed times in milliseconds.
 */
public class SearchStats implements Writeable, ToXContentFragment {

    /** Number of partition streams that have completed. */
    public final long queriesCompleted;

    /** Cumulative rows emitted from {@code IndexedExec::poll_next}. */
    public final long outputRows;
    /** Cumulative rows that matched the indexed predicate (pre-residual). */
    public final long rowsMatched;
    /** Cumulative rows pruned by parquet page-index pruning. */
    public final long rowsPrunedByPageIndex;
    /** Cumulative row groups processed. */
    public final long rowGroupsProcessed;
    /** Cumulative row groups skipped via row-group pruning. */
    public final long rowGroupsSkipped;
    /** Cumulative parquet pages eliminated by the page-level pruner. */
    public final long pagesPruned;
    /** Cumulative parquet pages considered by the page-level pruner. */
    public final long pagesTotal;
    /** Cumulative {@code prune_rg} calls that could not apply page-level pruning. */
    public final long pagePruningUnavailable;
    /** Cumulative FFM round-trips into the Java backend collector. */
    public final long ffmCollectorCalls;
    /** Cumulative output {@code RecordBatch}es produced. */
    public final long batchesProduced;
    /** Cumulative input {@code RecordBatch}es received from the parquet stream. */
    public final long parquetBatchesReceived;
    /** Cumulative row groups whose {@code PositionMap} was {@code Identity}. */
    public final long positionMapIdentity;
    /** Cumulative row groups whose {@code PositionMap} was {@code Bitmap}. */
    public final long positionMapBitmap;
    /** Cumulative row groups whose {@code PositionMap} was {@code Runs}. */
    public final long positionMapRuns;
    /** Cumulative row groups built with row-granular {@code RowSelection}. */
    public final long minSkipRunRowGranular;
    /** Cumulative row groups built with block-granular {@code RowSelection}. */
    public final long minSkipRunBlockGranular;
    /** Cumulative {@code Poll::Pending} returns from the prefetch receiver. */
    public final long prefetchWaitCount;
    /** Cumulative batches fed into the output coalescer. */
    public final long batchesPreCoalesce;

    /** Cumulative wall-clock time spent in {@code poll_next}, in milliseconds. */
    public final long elapsedComputeMs;
    /** Cumulative time spent inside the index search, in milliseconds. */
    public final long indexTimeMs;
    /** Cumulative time spent reading parquet, in milliseconds. */
    public final long parquetTimeMs;
    /** Cumulative time the poll thread blocked on prefetch, in milliseconds. */
    public final long prefetchWaitTimeMs;
    /** Cumulative time spent in the output coalescer, in milliseconds. */
    public final long coalesceTimeMs;
    /** Cumulative time spent in {@code build_mask}, in milliseconds. */
    public final long buildMaskTimeMs;
    /** Cumulative time spent in {@code filter_record_batch}, in milliseconds. */
    public final long filterRecordBatchTimeMs;
    /** Cumulative time spent in evaluator {@code on_batch_mask}, in milliseconds. */
    public final long onBatchMaskTimeMs;
    /** Cumulative time spent slicing and downcasting the current mask, in milliseconds. */
    public final long maskSliceTimeMs;
    /** Cumulative time spent in {@code finalize_batch} projection fix-up, in milliseconds. */
    public final long projectionFixupTimeMs;
    /** Cumulative time spent polling the inner parquet stream, in milliseconds. */
    public final long parquetPollTimeMs;

    /**
     * Construct from explicit field values. Argument order matches the Rust
     * {@code SearchStatsRepr} layout: lifecycle, then 18 counts, then 11 times.
     */
    public SearchStats(
        long queriesCompleted,
        long outputRows,
        long rowsMatched,
        long rowsPrunedByPageIndex,
        long rowGroupsProcessed,
        long rowGroupsSkipped,
        long pagesPruned,
        long pagesTotal,
        long pagePruningUnavailable,
        long ffmCollectorCalls,
        long batchesProduced,
        long parquetBatchesReceived,
        long positionMapIdentity,
        long positionMapBitmap,
        long positionMapRuns,
        long minSkipRunRowGranular,
        long minSkipRunBlockGranular,
        long prefetchWaitCount,
        long batchesPreCoalesce,
        long elapsedComputeMs,
        long indexTimeMs,
        long parquetTimeMs,
        long prefetchWaitTimeMs,
        long coalesceTimeMs,
        long buildMaskTimeMs,
        long filterRecordBatchTimeMs,
        long onBatchMaskTimeMs,
        long maskSliceTimeMs,
        long projectionFixupTimeMs,
        long parquetPollTimeMs
    ) {
        this.queriesCompleted = queriesCompleted;
        this.outputRows = outputRows;
        this.rowsMatched = rowsMatched;
        this.rowsPrunedByPageIndex = rowsPrunedByPageIndex;
        this.rowGroupsProcessed = rowGroupsProcessed;
        this.rowGroupsSkipped = rowGroupsSkipped;
        this.pagesPruned = pagesPruned;
        this.pagesTotal = pagesTotal;
        this.pagePruningUnavailable = pagePruningUnavailable;
        this.ffmCollectorCalls = ffmCollectorCalls;
        this.batchesProduced = batchesProduced;
        this.parquetBatchesReceived = parquetBatchesReceived;
        this.positionMapIdentity = positionMapIdentity;
        this.positionMapBitmap = positionMapBitmap;
        this.positionMapRuns = positionMapRuns;
        this.minSkipRunRowGranular = minSkipRunRowGranular;
        this.minSkipRunBlockGranular = minSkipRunBlockGranular;
        this.prefetchWaitCount = prefetchWaitCount;
        this.batchesPreCoalesce = batchesPreCoalesce;
        this.elapsedComputeMs = elapsedComputeMs;
        this.indexTimeMs = indexTimeMs;
        this.parquetTimeMs = parquetTimeMs;
        this.prefetchWaitTimeMs = prefetchWaitTimeMs;
        this.coalesceTimeMs = coalesceTimeMs;
        this.buildMaskTimeMs = buildMaskTimeMs;
        this.filterRecordBatchTimeMs = filterRecordBatchTimeMs;
        this.onBatchMaskTimeMs = onBatchMaskTimeMs;
        this.maskSliceTimeMs = maskSliceTimeMs;
        this.projectionFixupTimeMs = projectionFixupTimeMs;
        this.parquetPollTimeMs = parquetPollTimeMs;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public SearchStats(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(),
            in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(),
            in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(),
            in.readVLong(), in.readVLong(), in.readVLong(),
            in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(),
            in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(queriesCompleted);
        out.writeVLong(outputRows);
        out.writeVLong(rowsMatched);
        out.writeVLong(rowsPrunedByPageIndex);
        out.writeVLong(rowGroupsProcessed);
        out.writeVLong(rowGroupsSkipped);
        out.writeVLong(pagesPruned);
        out.writeVLong(pagesTotal);
        out.writeVLong(pagePruningUnavailable);
        out.writeVLong(ffmCollectorCalls);
        out.writeVLong(batchesProduced);
        out.writeVLong(parquetBatchesReceived);
        out.writeVLong(positionMapIdentity);
        out.writeVLong(positionMapBitmap);
        out.writeVLong(positionMapRuns);
        out.writeVLong(minSkipRunRowGranular);
        out.writeVLong(minSkipRunBlockGranular);
        out.writeVLong(prefetchWaitCount);
        out.writeVLong(batchesPreCoalesce);
        out.writeVLong(elapsedComputeMs);
        out.writeVLong(indexTimeMs);
        out.writeVLong(parquetTimeMs);
        out.writeVLong(prefetchWaitTimeMs);
        out.writeVLong(coalesceTimeMs);
        out.writeVLong(buildMaskTimeMs);
        out.writeVLong(filterRecordBatchTimeMs);
        out.writeVLong(onBatchMaskTimeMs);
        out.writeVLong(maskSliceTimeMs);
        out.writeVLong(projectionFixupTimeMs);
        out.writeVLong(parquetPollTimeMs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_stats");
        builder.field("queries_completed", queriesCompleted);
        builder.field("output_rows", outputRows);
        builder.field("rows_matched", rowsMatched);
        builder.field("rows_pruned_by_page_index", rowsPrunedByPageIndex);
        builder.field("row_groups_processed", rowGroupsProcessed);
        builder.field("row_groups_skipped", rowGroupsSkipped);
        builder.field("pages_pruned", pagesPruned);
        builder.field("pages_total", pagesTotal);
        builder.field("page_pruning_unavailable", pagePruningUnavailable);
        builder.field("ffm_collector_calls", ffmCollectorCalls);
        builder.field("batches_produced", batchesProduced);
        builder.field("parquet_batches_received", parquetBatchesReceived);
        builder.field("position_map_identity", positionMapIdentity);
        builder.field("position_map_bitmap", positionMapBitmap);
        builder.field("position_map_runs", positionMapRuns);
        builder.field("min_skip_run_row_granular", minSkipRunRowGranular);
        builder.field("min_skip_run_block_granular", minSkipRunBlockGranular);
        builder.field("prefetch_wait_count", prefetchWaitCount);
        builder.field("batches_pre_coalesce", batchesPreCoalesce);
        builder.field("elapsed_compute_ms", elapsedComputeMs);
        builder.field("index_time_ms", indexTimeMs);
        builder.field("parquet_time_ms", parquetTimeMs);
        builder.field("prefetch_wait_time_ms", prefetchWaitTimeMs);
        builder.field("coalesce_time_ms", coalesceTimeMs);
        builder.field("build_mask_time_ms", buildMaskTimeMs);
        builder.field("filter_record_batch_time_ms", filterRecordBatchTimeMs);
        builder.field("on_batch_mask_time_ms", onBatchMaskTimeMs);
        builder.field("mask_slice_time_ms", maskSliceTimeMs);
        builder.field("projection_fixup_time_ms", projectionFixupTimeMs);
        builder.field("parquet_poll_time_ms", parquetPollTimeMs);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchStats that = (SearchStats) o;
        return queriesCompleted == that.queriesCompleted
            && outputRows == that.outputRows
            && rowsMatched == that.rowsMatched
            && rowsPrunedByPageIndex == that.rowsPrunedByPageIndex
            && rowGroupsProcessed == that.rowGroupsProcessed
            && rowGroupsSkipped == that.rowGroupsSkipped
            && pagesPruned == that.pagesPruned
            && pagesTotal == that.pagesTotal
            && pagePruningUnavailable == that.pagePruningUnavailable
            && ffmCollectorCalls == that.ffmCollectorCalls
            && batchesProduced == that.batchesProduced
            && parquetBatchesReceived == that.parquetBatchesReceived
            && positionMapIdentity == that.positionMapIdentity
            && positionMapBitmap == that.positionMapBitmap
            && positionMapRuns == that.positionMapRuns
            && minSkipRunRowGranular == that.minSkipRunRowGranular
            && minSkipRunBlockGranular == that.minSkipRunBlockGranular
            && prefetchWaitCount == that.prefetchWaitCount
            && batchesPreCoalesce == that.batchesPreCoalesce
            && elapsedComputeMs == that.elapsedComputeMs
            && indexTimeMs == that.indexTimeMs
            && parquetTimeMs == that.parquetTimeMs
            && prefetchWaitTimeMs == that.prefetchWaitTimeMs
            && coalesceTimeMs == that.coalesceTimeMs
            && buildMaskTimeMs == that.buildMaskTimeMs
            && filterRecordBatchTimeMs == that.filterRecordBatchTimeMs
            && onBatchMaskTimeMs == that.onBatchMaskTimeMs
            && maskSliceTimeMs == that.maskSliceTimeMs
            && projectionFixupTimeMs == that.projectionFixupTimeMs
            && parquetPollTimeMs == that.parquetPollTimeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            queriesCompleted,
            outputRows, rowsMatched, rowsPrunedByPageIndex, rowGroupsProcessed, rowGroupsSkipped,
            pagesPruned, pagesTotal, pagePruningUnavailable, ffmCollectorCalls, batchesProduced,
            parquetBatchesReceived, positionMapIdentity, positionMapBitmap, positionMapRuns,
            minSkipRunRowGranular, minSkipRunBlockGranular, prefetchWaitCount, batchesPreCoalesce,
            elapsedComputeMs, indexTimeMs, parquetTimeMs, prefetchWaitTimeMs, coalesceTimeMs,
            buildMaskTimeMs, filterRecordBatchTimeMs, onBatchMaskTimeMs, maskSliceTimeMs,
            projectionFixupTimeMs, parquetPollTimeMs
        );
    }
}
