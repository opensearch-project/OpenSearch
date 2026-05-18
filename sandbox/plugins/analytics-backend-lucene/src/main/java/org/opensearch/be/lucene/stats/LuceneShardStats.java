/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.stats;

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
 * Shard-level statistics collector for the Lucene data format plugin.
 * Uses LongAdder for high-throughput counters and AtomicLong for gauges.
 * Serves as both the live collector and the serializable snapshot.
 */
@ExperimentalApi
public class LuceneShardStats implements ToXContentFragment, Writeable {

    // Indexing counters
    private final LongAdder docsIndexedTotal = new LongAdder();
    private final LongAdder docsIndexedFailures = new LongAdder();
    private final LongAdder indexTimeMillis = new LongAdder();

    // Flush counters
    private final LongAdder flushTotal = new LongAdder();
    private final LongAdder flushTimeMillis = new LongAdder();
    private final LongAdder flushForceMergeTimeMillis = new LongAdder();

    // Refresh counters
    private final LongAdder refreshTotal = new LongAdder();
    private final LongAdder refreshTimeMillis = new LongAdder();
    private final LongAdder refreshAddIndexesTimeMillis = new LongAdder();
    private final LongAdder refreshSegmentsIncorporatedTotal = new LongAdder();

    // Commit counters
    private final LongAdder commitTotal = new LongAdder();
    private final LongAdder commitTimeMillis = new LongAdder();

    // Merge counters
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeDocsTotal = new LongAdder();
    private final LongAdder mergeFailures = new LongAdder();

    // Delete counters
    private final LongAdder deleteTotal = new LongAdder();
    private final LongAdder deleteTimeMillis = new LongAdder();
    private final LongAdder deleteByGenerationTotal = new LongAdder();
    private final LongAdder deleteSharedWriterFallbackTotal = new LongAdder();

    // Memory gauges
    private final AtomicLong ramBufferBytesUsed = new AtomicLong();
    private final AtomicLong activeWriters = new AtomicLong();
    private final AtomicLong activeReaders = new AtomicLong();

    public LuceneShardStats() {}

    public LuceneShardStats(StreamInput in) throws IOException {
        // Indexing
        docsIndexedTotal.add(in.readVLong());
        docsIndexedFailures.add(in.readVLong());
        indexTimeMillis.add(in.readVLong());

        // Flush
        flushTotal.add(in.readVLong());
        flushTimeMillis.add(in.readVLong());
        flushForceMergeTimeMillis.add(in.readVLong());

        // Refresh
        refreshTotal.add(in.readVLong());
        refreshTimeMillis.add(in.readVLong());
        refreshAddIndexesTimeMillis.add(in.readVLong());
        refreshSegmentsIncorporatedTotal.add(in.readVLong());

        // Commit
        commitTotal.add(in.readVLong());
        commitTimeMillis.add(in.readVLong());

        // Merge
        mergeTotal.add(in.readVLong());
        mergeTimeMillis.add(in.readVLong());
        mergeDocsTotal.add(in.readVLong());
        mergeFailures.add(in.readVLong());

        // Delete
        deleteTotal.add(in.readVLong());
        deleteTimeMillis.add(in.readVLong());
        deleteByGenerationTotal.add(in.readVLong());
        deleteSharedWriterFallbackTotal.add(in.readVLong());

        // Memory
        ramBufferBytesUsed.set(in.readVLong());
        activeWriters.set(in.readVLong());
        activeReaders.set(in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Indexing
        out.writeVLong(docsIndexedTotal.sum());
        out.writeVLong(docsIndexedFailures.sum());
        out.writeVLong(indexTimeMillis.sum());

        // Flush
        out.writeVLong(flushTotal.sum());
        out.writeVLong(flushTimeMillis.sum());
        out.writeVLong(flushForceMergeTimeMillis.sum());

        // Refresh
        out.writeVLong(refreshTotal.sum());
        out.writeVLong(refreshTimeMillis.sum());
        out.writeVLong(refreshAddIndexesTimeMillis.sum());
        out.writeVLong(refreshSegmentsIncorporatedTotal.sum());

        // Commit
        out.writeVLong(commitTotal.sum());
        out.writeVLong(commitTimeMillis.sum());

        // Merge
        out.writeVLong(mergeTotal.sum());
        out.writeVLong(mergeTimeMillis.sum());
        out.writeVLong(mergeDocsTotal.sum());
        out.writeVLong(mergeFailures.sum());

        // Delete
        out.writeVLong(deleteTotal.sum());
        out.writeVLong(deleteTimeMillis.sum());
        out.writeVLong(deleteByGenerationTotal.sum());
        out.writeVLong(deleteSharedWriterFallbackTotal.sum());

        // Memory
        out.writeVLong(ramBufferBytesUsed.get());
        out.writeVLong(activeWriters.get());
        out.writeVLong(activeReaders.get());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Indexing
        builder.startObject("indexing");
        builder.field("docs_indexed_total", docsIndexedTotal.sum());
        builder.field("docs_indexed_failures", docsIndexedFailures.sum());
        builder.field("index_time_millis", indexTimeMillis.sum());
        builder.endObject();

        // Flush
        builder.startObject("flush");
        builder.field("flush_total", flushTotal.sum());
        builder.field("flush_time_millis", flushTimeMillis.sum());
        builder.field("flush_force_merge_time_millis", flushForceMergeTimeMillis.sum());
        builder.endObject();

        // Refresh
        builder.startObject("refresh");
        builder.field("refresh_total", refreshTotal.sum());
        builder.field("refresh_time_millis", refreshTimeMillis.sum());
        builder.field("refresh_add_indexes_time_millis", refreshAddIndexesTimeMillis.sum());
        builder.field("refresh_segments_incorporated_total", refreshSegmentsIncorporatedTotal.sum());
        builder.endObject();

        // Commit
        builder.startObject("commit");
        builder.field("commit_total", commitTotal.sum());
        builder.field("commit_time_millis", commitTimeMillis.sum());
        builder.endObject();

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal.sum());
        builder.field("merge_time_millis", mergeTimeMillis.sum());
        builder.field("merge_docs_total", mergeDocsTotal.sum());
        builder.field("merge_failures", mergeFailures.sum());
        builder.endObject();

        // Delete
        builder.startObject("delete");
        builder.field("delete_total", deleteTotal.sum());
        builder.field("delete_time_millis", deleteTimeMillis.sum());
        builder.field("delete_by_generation_total", deleteByGenerationTotal.sum());
        builder.field("delete_shared_writer_fallback_total", deleteSharedWriterFallbackTotal.sum());
        builder.endObject();

        // Memory
        builder.startObject("memory");
        builder.field("ram_buffer_bytes_used", ramBufferBytesUsed.get());
        builder.field("active_writers", activeWriters.get());
        builder.field("active_readers", activeReaders.get());
        builder.endObject();

        return builder;
    }

    /**
     * Returns this instance as a snapshot. Since LongAdder.sum() provides a point-in-time
     * view and the class implements Writeable, it can serialize its own current state.
     */
    public LuceneShardStats snapshot() {
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

    // --- Flush methods ---

    public void incFlushTotal() {
        flushTotal.increment();
    }

    public void addFlushTimeMillis(long ms) {
        flushTimeMillis.add(ms);
    }

    public void addFlushForceMergeTimeMillis(long ms) {
        flushForceMergeTimeMillis.add(ms);
    }

    // --- Refresh methods ---

    public void incRefreshTotal() {
        refreshTotal.increment();
    }

    public void addRefreshTimeMillis(long ms) {
        refreshTimeMillis.add(ms);
    }

    public void addRefreshAddIndexesTimeMillis(long ms) {
        refreshAddIndexesTimeMillis.add(ms);
    }

    public void incRefreshSegmentsIncorporatedTotal() {
        refreshSegmentsIncorporatedTotal.increment();
    }

    // --- Commit methods ---

    public void incCommitTotal() {
        commitTotal.increment();
    }

    public void addCommitTimeMillis(long ms) {
        commitTimeMillis.add(ms);
    }

    // --- Merge methods ---

    public void incMergeTotal() {
        mergeTotal.increment();
    }

    public void addMergeTimeMillis(long ms) {
        mergeTimeMillis.add(ms);
    }

    public void addMergeDocsTotal(long n) {
        mergeDocsTotal.add(n);
    }

    public void incMergeFailures() {
        mergeFailures.increment();
    }

    // --- Delete methods ---

    public void incDeleteTotal() {
        deleteTotal.increment();
    }

    public void addDeleteTimeMillis(long ms) {
        deleteTimeMillis.add(ms);
    }

    public void incDeleteByGenerationTotal() {
        deleteByGenerationTotal.increment();
    }

    public void incDeleteSharedWriterFallbackTotal() {
        deleteSharedWriterFallbackTotal.increment();
    }

    // --- Memory gauge methods ---

    public void setRamBufferBytesUsed(long bytes) {
        ramBufferBytesUsed.set(bytes);
    }

    public void setActiveWriters(long count) {
        activeWriters.set(count);
    }

    public void setActiveReaders(long count) {
        activeReaders.set(count);
    }
}
