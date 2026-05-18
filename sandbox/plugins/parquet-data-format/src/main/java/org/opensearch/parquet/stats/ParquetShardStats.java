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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Shard-level statistics collector for the Parquet data format plugin.
 * Uses LongAdder for high-throughput counters and AtomicLong for gauges.
 * Serves as both the live collector and the serializable snapshot.
 */
@ExperimentalApi
public class ParquetShardStats implements ToXContentFragment, Writeable {

    // Indexing counters
    private final LongAdder docsIndexedTotal = new LongAdder();
    private final LongAdder docsIndexedFailures = new LongAdder();
    private final LongAdder indexTimeMillis = new LongAdder();

    // VSR Pipeline counters + gauge
    private final LongAdder vsrRotationsTotal = new LongAdder();
    private final LongAdder vsrRotationWaitMillis = new LongAdder();
    private final AtomicLong vsrRowsCurrent = new AtomicLong();

    // Native Write counters
    private final LongAdder nativeWriteTotal = new LongAdder();
    private final LongAdder nativeWriteTimeMillis = new LongAdder();
    private final LongAdder nativeWriteFailures = new LongAdder();
    private final LongAdder nativeFinalizeTotal = new LongAdder();
    private final LongAdder nativeFinalizeTimeMillis = new LongAdder();
    private final LongAdder nativeFinalizeFailures = new LongAdder();
    private final LongAdder nativeSyncTotal = new LongAdder();
    private final LongAdder nativeSyncTimeMillis = new LongAdder();
    private final LongAdder nativeSyncFailures = new LongAdder();

    // Sort counters
    private final LongAdder sortTotal = new LongAdder();
    private final LongAdder sortTimeMillis = new LongAdder();
    private final LongAdder sortInMemoryTotal = new LongAdder();
    private final LongAdder sortStreamingTotal = new LongAdder();

    // Merge counters
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeFailures = new LongAdder();
    private final LongAdder mergeInputFilesTotal = new LongAdder();
    private final LongAdder mergeOutputRowsTotal = new LongAdder();

    // Rate Limiting counters
    private final LongAdder rateLimitPauseTimeMillis = new LongAdder();
    private final LongAdder rateLimitBytesWritten = new LongAdder();

    // Memory gauges
    private final AtomicLong arrowAllocatedBytes = new AtomicLong();
    private final AtomicLong arrowMaxBytes = new AtomicLong();
    private final AtomicLong rustWriterMemoryBytes = new AtomicLong();

    // Background Write counters
    private final LongAdder backgroundWriteTotal = new LongAdder();
    private final LongAdder backgroundWriteWaitMillis = new LongAdder();
    private final LongAdder backgroundWriteTimeouts = new LongAdder();

    public ParquetShardStats() {}

    public ParquetShardStats(StreamInput in) throws IOException {
        // Indexing
        docsIndexedTotal.add(in.readVLong());
        docsIndexedFailures.add(in.readVLong());
        indexTimeMillis.add(in.readVLong());

        // VSR
        vsrRotationsTotal.add(in.readVLong());
        vsrRotationWaitMillis.add(in.readVLong());
        vsrRowsCurrent.set(in.readVLong());

        // Native Write
        nativeWriteTotal.add(in.readVLong());
        nativeWriteTimeMillis.add(in.readVLong());
        nativeWriteFailures.add(in.readVLong());
        nativeFinalizeTotal.add(in.readVLong());
        nativeFinalizeTimeMillis.add(in.readVLong());
        nativeFinalizeFailures.add(in.readVLong());
        nativeSyncTotal.add(in.readVLong());
        nativeSyncTimeMillis.add(in.readVLong());
        nativeSyncFailures.add(in.readVLong());

        // Sort
        sortTotal.add(in.readVLong());
        sortTimeMillis.add(in.readVLong());
        sortInMemoryTotal.add(in.readVLong());
        sortStreamingTotal.add(in.readVLong());

        // Merge
        mergeTotal.add(in.readVLong());
        mergeTimeMillis.add(in.readVLong());
        mergeFailures.add(in.readVLong());
        mergeInputFilesTotal.add(in.readVLong());
        mergeOutputRowsTotal.add(in.readVLong());

        // Rate Limiting
        rateLimitPauseTimeMillis.add(in.readVLong());
        rateLimitBytesWritten.add(in.readVLong());

        // Memory
        arrowAllocatedBytes.set(in.readVLong());
        arrowMaxBytes.set(in.readVLong());
        rustWriterMemoryBytes.set(in.readVLong());

        // Background Write
        backgroundWriteTotal.add(in.readVLong());
        backgroundWriteWaitMillis.add(in.readVLong());
        backgroundWriteTimeouts.add(in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Indexing
        out.writeVLong(docsIndexedTotal.sum());
        out.writeVLong(docsIndexedFailures.sum());
        out.writeVLong(indexTimeMillis.sum());

        // VSR
        out.writeVLong(vsrRotationsTotal.sum());
        out.writeVLong(vsrRotationWaitMillis.sum());
        out.writeVLong(vsrRowsCurrent.get());

        // Native Write
        out.writeVLong(nativeWriteTotal.sum());
        out.writeVLong(nativeWriteTimeMillis.sum());
        out.writeVLong(nativeWriteFailures.sum());
        out.writeVLong(nativeFinalizeTotal.sum());
        out.writeVLong(nativeFinalizeTimeMillis.sum());
        out.writeVLong(nativeFinalizeFailures.sum());
        out.writeVLong(nativeSyncTotal.sum());
        out.writeVLong(nativeSyncTimeMillis.sum());
        out.writeVLong(nativeSyncFailures.sum());

        // Sort
        out.writeVLong(sortTotal.sum());
        out.writeVLong(sortTimeMillis.sum());
        out.writeVLong(sortInMemoryTotal.sum());
        out.writeVLong(sortStreamingTotal.sum());

        // Merge
        out.writeVLong(mergeTotal.sum());
        out.writeVLong(mergeTimeMillis.sum());
        out.writeVLong(mergeFailures.sum());
        out.writeVLong(mergeInputFilesTotal.sum());
        out.writeVLong(mergeOutputRowsTotal.sum());

        // Rate Limiting
        out.writeVLong(rateLimitPauseTimeMillis.sum());
        out.writeVLong(rateLimitBytesWritten.sum());

        // Memory
        out.writeVLong(arrowAllocatedBytes.get());
        out.writeVLong(arrowMaxBytes.get());
        out.writeVLong(rustWriterMemoryBytes.get());

        // Background Write
        out.writeVLong(backgroundWriteTotal.sum());
        out.writeVLong(backgroundWriteWaitMillis.sum());
        out.writeVLong(backgroundWriteTimeouts.sum());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Indexing
        builder.startObject("indexing");
        builder.field("docs_indexed_total", docsIndexedTotal.sum());
        builder.field("docs_indexed_failures", docsIndexedFailures.sum());
        builder.field("index_time_millis", indexTimeMillis.sum());
        builder.endObject();

        // VSR
        builder.startObject("vsr");
        builder.field("vsr_rotations_total", vsrRotationsTotal.sum());
        builder.field("vsr_rotation_wait_millis", vsrRotationWaitMillis.sum());
        builder.field("vsr_rows_current", vsrRowsCurrent.get());
        builder.endObject();

        // Native Write
        builder.startObject("native_write");
        builder.field("native_write_total", nativeWriteTotal.sum());
        builder.field("native_write_time_millis", nativeWriteTimeMillis.sum());
        builder.field("native_write_failures", nativeWriteFailures.sum());
        builder.field("native_finalize_total", nativeFinalizeTotal.sum());
        builder.field("native_finalize_time_millis", nativeFinalizeTimeMillis.sum());
        builder.field("native_finalize_failures", nativeFinalizeFailures.sum());
        builder.field("native_sync_total", nativeSyncTotal.sum());
        builder.field("native_sync_time_millis", nativeSyncTimeMillis.sum());
        builder.field("native_sync_failures", nativeSyncFailures.sum());
        builder.endObject();

        // Sort
        builder.startObject("sort");
        builder.field("sort_total", sortTotal.sum());
        builder.field("sort_time_millis", sortTimeMillis.sum());
        builder.field("sort_in_memory_total", sortInMemoryTotal.sum());
        builder.field("sort_streaming_total", sortStreamingTotal.sum());
        builder.endObject();

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal.sum());
        builder.field("merge_time_millis", mergeTimeMillis.sum());
        builder.field("merge_failures", mergeFailures.sum());
        builder.field("merge_input_files_total", mergeInputFilesTotal.sum());
        builder.field("merge_output_rows_total", mergeOutputRowsTotal.sum());
        builder.endObject();

        // Rate Limiting
        builder.startObject("rate_limit");
        builder.field("rate_limit_pause_time_millis", rateLimitPauseTimeMillis.sum());
        builder.field("rate_limit_bytes_written", rateLimitBytesWritten.sum());
        builder.endObject();

        // Memory
        builder.startObject("memory");
        builder.field("arrow_allocated_bytes", arrowAllocatedBytes.get());
        builder.field("arrow_max_bytes", arrowMaxBytes.get());
        builder.field("rust_writer_memory_bytes", rustWriterMemoryBytes.get());
        builder.endObject();

        // Background Write
        builder.startObject("background_write");
        builder.field("background_write_total", backgroundWriteTotal.sum());
        builder.field("background_write_wait_millis", backgroundWriteWaitMillis.sum());
        builder.field("background_write_timeouts", backgroundWriteTimeouts.sum());
        builder.endObject();

        return builder;
    }

    /**
     * Returns this instance as a snapshot. Since LongAdder.sum() provides a point-in-time
     * view and the class implements Writeable, it can serialize its own current state.
     */
    public ParquetShardStats snapshot() {
        return this;
    }

    // --- Indexing methods ---

    public void addDocsIndexed(long n) {
        docsIndexedTotal.add(n);
    }

    public void incDocsIndexedFailures() {
        docsIndexedFailures.increment();
    }

    public void addIndexTimeMillis(long ms) {
        indexTimeMillis.add(ms);
    }

    // --- VSR Pipeline methods ---

    public void incVsrRotations() {
        vsrRotationsTotal.increment();
    }

    public void addVsrRotationWaitMillis(long ms) {
        vsrRotationWaitMillis.add(ms);
    }

    public void setVsrRowsCurrent(long rows) {
        vsrRowsCurrent.set(rows);
    }

    // --- Native Write methods ---

    public void incNativeWriteTotal() {
        nativeWriteTotal.increment();
    }

    public void addNativeWriteTimeMillis(long ms) {
        nativeWriteTimeMillis.add(ms);
    }

    public void incNativeWriteFailures() {
        nativeWriteFailures.increment();
    }

    public void incNativeFinalizeTotal() {
        nativeFinalizeTotal.increment();
    }

    public void addNativeFinalizeTimeMillis(long ms) {
        nativeFinalizeTimeMillis.add(ms);
    }

    public void incNativeFinalizeFailures() {
        nativeFinalizeFailures.increment();
    }

    public void incNativeSyncTotal() {
        nativeSyncTotal.increment();
    }

    public void addNativeSyncTimeMillis(long ms) {
        nativeSyncTimeMillis.add(ms);
    }

    public void incNativeSyncFailures() {
        nativeSyncFailures.increment();
    }

    // --- Sort methods ---

    public void incSortTotal() {
        sortTotal.increment();
    }

    public void addSortTimeMillis(long ms) {
        sortTimeMillis.add(ms);
    }

    public void incSortInMemoryTotal() {
        sortInMemoryTotal.increment();
    }

    public void incSortStreamingTotal() {
        sortStreamingTotal.increment();
    }

    // --- Merge methods ---

    public void incMergeTotal() {
        mergeTotal.increment();
    }

    public void addMergeTimeMillis(long ms) {
        mergeTimeMillis.add(ms);
    }

    public void incMergeFailures() {
        mergeFailures.increment();
    }

    public void addMergeInputFilesTotal(long n) {
        mergeInputFilesTotal.add(n);
    }

    public void addMergeOutputRowsTotal(long n) {
        mergeOutputRowsTotal.add(n);
    }

    // --- Rate Limiting methods ---

    public void addRateLimitPauseTimeMillis(long ms) {
        rateLimitPauseTimeMillis.add(ms);
    }

    public void addRateLimitBytesWritten(long bytes) {
        rateLimitBytesWritten.add(bytes);
    }

    // --- Memory gauge methods ---

    public void setArrowAllocatedBytes(long bytes) {
        arrowAllocatedBytes.set(bytes);
    }

    public void setArrowMaxBytes(long bytes) {
        arrowMaxBytes.set(bytes);
    }

    public void setRustWriterMemoryBytes(long bytes) {
        rustWriterMemoryBytes.set(bytes);
    }

    // --- Background Write methods ---

    public void incBackgroundWriteTotal() {
        backgroundWriteTotal.increment();
    }

    public void addBackgroundWriteWaitMillis(long ms) {
        backgroundWriteWaitMillis.add(ms);
    }

    public void incBackgroundWriteTimeouts() {
        backgroundWriteTimeouts.increment();
    }
}
