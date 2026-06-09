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
 * Node-level cumulative search execution counters exposed under the
 * {@code search_stats} key. Each field is a process-global running total
 * summed across all partitions and queries since startup; values are never
 * reset between calls.
 */
public class SearchStats implements Writeable, ToXContentFragment {

    public final long listingTableScan;
    public final long singleCollectorScan;
    public final long bitmapTreeScan;
    public final long delegationCalls;
    public final long rgProcessed;
    public final long rgSkipped;
    public final long parquetScanTotalTimeMs;
    public final long parquetScanUntilDataTimeMs;
    public final long parquetProcessingTimeMs;
    public final long prefetchWaitTimeMs;
    public final long prefetchWaitCount;
    public final long elapsedComputeMs;
    public final long buildMaskTimeMs;
    public final long onBatchMaskTimeMs;
    public final long filterRecordBatchTimeMs;
    public final long objectStoreReadTimeMs;

    public SearchStats(
        long listingTableScan,
        long singleCollectorScan,
        long bitmapTreeScan,
        long delegationCalls,
        long rgProcessed,
        long rgSkipped,
        long parquetScanTotalTimeMs,
        long parquetScanUntilDataTimeMs,
        long parquetProcessingTimeMs,
        long prefetchWaitTimeMs,
        long prefetchWaitCount,
        long elapsedComputeMs,
        long buildMaskTimeMs,
        long onBatchMaskTimeMs,
        long filterRecordBatchTimeMs,
        long objectStoreReadTimeMs
    ) {
        this.listingTableScan = listingTableScan;
        this.singleCollectorScan = singleCollectorScan;
        this.bitmapTreeScan = bitmapTreeScan;
        this.delegationCalls = delegationCalls;
        this.rgProcessed = rgProcessed;
        this.rgSkipped = rgSkipped;
        this.parquetScanTotalTimeMs = parquetScanTotalTimeMs;
        this.parquetScanUntilDataTimeMs = parquetScanUntilDataTimeMs;
        this.parquetProcessingTimeMs = parquetProcessingTimeMs;
        this.prefetchWaitTimeMs = prefetchWaitTimeMs;
        this.prefetchWaitCount = prefetchWaitCount;
        this.elapsedComputeMs = elapsedComputeMs;
        this.buildMaskTimeMs = buildMaskTimeMs;
        this.onBatchMaskTimeMs = onBatchMaskTimeMs;
        this.filterRecordBatchTimeMs = filterRecordBatchTimeMs;
        this.objectStoreReadTimeMs = objectStoreReadTimeMs;
    }

    public SearchStats(StreamInput in) throws IOException {
        this.listingTableScan = in.readVLong();
        this.singleCollectorScan = in.readVLong();
        this.bitmapTreeScan = in.readVLong();
        this.delegationCalls = in.readVLong();
        this.rgProcessed = in.readVLong();
        this.rgSkipped = in.readVLong();
        this.parquetScanTotalTimeMs = in.readVLong();
        this.parquetScanUntilDataTimeMs = in.readVLong();
        this.parquetProcessingTimeMs = in.readVLong();
        this.prefetchWaitTimeMs = in.readVLong();
        this.prefetchWaitCount = in.readVLong();
        this.elapsedComputeMs = in.readVLong();
        this.buildMaskTimeMs = in.readVLong();
        this.onBatchMaskTimeMs = in.readVLong();
        this.filterRecordBatchTimeMs = in.readVLong();
        this.objectStoreReadTimeMs = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(listingTableScan);
        out.writeVLong(singleCollectorScan);
        out.writeVLong(bitmapTreeScan);
        out.writeVLong(delegationCalls);
        out.writeVLong(rgProcessed);
        out.writeVLong(rgSkipped);
        out.writeVLong(parquetScanTotalTimeMs);
        out.writeVLong(parquetScanUntilDataTimeMs);
        out.writeVLong(parquetProcessingTimeMs);
        out.writeVLong(prefetchWaitTimeMs);
        out.writeVLong(prefetchWaitCount);
        out.writeVLong(elapsedComputeMs);
        out.writeVLong(buildMaskTimeMs);
        out.writeVLong(onBatchMaskTimeMs);
        out.writeVLong(filterRecordBatchTimeMs);
        out.writeVLong(objectStoreReadTimeMs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_stats");
        builder.field("listing_table_scan", listingTableScan);
        builder.field("single_collector_scan", singleCollectorScan);
        builder.field("bitmap_tree_scan", bitmapTreeScan);
        builder.field("delegation_calls", delegationCalls);
        builder.field("rg_processed", rgProcessed);
        builder.field("rg_skipped", rgSkipped);
        builder.field("parquet_scan_total_time_ms", parquetScanTotalTimeMs);
        builder.field("parquet_scan_until_data_time_ms", parquetScanUntilDataTimeMs);
        builder.field("parquet_processing_time_ms", parquetProcessingTimeMs);
        builder.field("prefetch_wait_time_ms", prefetchWaitTimeMs);
        builder.field("prefetch_wait_count", prefetchWaitCount);
        builder.field("elapsed_compute_ms", elapsedComputeMs);
        builder.field("build_mask_time_ms", buildMaskTimeMs);
        builder.field("on_batch_mask_time_ms", onBatchMaskTimeMs);
        builder.field("filter_record_batch_time_ms", filterRecordBatchTimeMs);
        builder.field("object_store_read_time_ms", objectStoreReadTimeMs);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchStats that = (SearchStats) o;
        return listingTableScan == that.listingTableScan
            && singleCollectorScan == that.singleCollectorScan
            && bitmapTreeScan == that.bitmapTreeScan
            && delegationCalls == that.delegationCalls
            && rgProcessed == that.rgProcessed
            && rgSkipped == that.rgSkipped
            && parquetScanTotalTimeMs == that.parquetScanTotalTimeMs
            && parquetScanUntilDataTimeMs == that.parquetScanUntilDataTimeMs
            && parquetProcessingTimeMs == that.parquetProcessingTimeMs
            && prefetchWaitTimeMs == that.prefetchWaitTimeMs
            && prefetchWaitCount == that.prefetchWaitCount
            && elapsedComputeMs == that.elapsedComputeMs
            && buildMaskTimeMs == that.buildMaskTimeMs
            && onBatchMaskTimeMs == that.onBatchMaskTimeMs
            && filterRecordBatchTimeMs == that.filterRecordBatchTimeMs
            && objectStoreReadTimeMs == that.objectStoreReadTimeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            listingTableScan,
            singleCollectorScan,
            bitmapTreeScan,
            delegationCalls,
            rgProcessed,
            rgSkipped,
            parquetScanTotalTimeMs,
            parquetScanUntilDataTimeMs,
            parquetProcessingTimeMs,
            prefetchWaitTimeMs,
            prefetchWaitCount,
            elapsedComputeMs,
            buildMaskTimeMs,
            onBatchMaskTimeMs,
            filterRecordBatchTimeMs,
            objectStoreReadTimeMs
        );
    }
}
