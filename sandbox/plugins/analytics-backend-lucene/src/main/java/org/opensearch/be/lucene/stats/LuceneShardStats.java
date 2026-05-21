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
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;

/**
 * Immutable point-in-time snapshot of shard-level Lucene statistics.
 * Produced by {@link LuceneShardStatsTracker#stats()}.
 * Supports serialization via {@link Writeable} and REST rendering via {@link ToXContentFragment}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneShardStats implements DataFormatShardStats, ToXContentFragment, Writeable {

    // Indexing
    private final long docsIndexedTotal;
    private final long docsIndexedFailures;
    private final long indexTimeMillis;

    // Flush
    private final long flushTotal;
    private final long flushTimeMillis;
    private final long flushForceMergeTimeMillis;

    // Refresh
    private final long refreshTotal;
    private final long refreshTimeMillis;
    private final long refreshAddIndexesTimeMillis;
    private final long refreshSegmentsIncorporatedTotal;

    // Commit
    private final long commitTotal;
    private final long commitTimeMillis;

    // Merge
    private final long mergeTotal;
    private final long mergeTimeMillis;
    private final long mergeDocsTotal;
    private final long mergeFailures;

    // Delete
    private final long deleteTotal;
    private final long deleteTimeMillis;
    private final long deleteByGenerationTotal;
    private final long deleteSharedWriterFallbackTotal;

    // Memory
    private final long ramBufferBytesUsed;
    private final long activeWriters;
    private final long activeReaders;

    /**
     * Constructs a snapshot with all values.
     */
    public LuceneShardStats(
        long docsIndexedTotal,
        long docsIndexedFailures,
        long indexTimeMillis,
        long flushTotal,
        long flushTimeMillis,
        long flushForceMergeTimeMillis,
        long refreshTotal,
        long refreshTimeMillis,
        long refreshAddIndexesTimeMillis,
        long refreshSegmentsIncorporatedTotal,
        long commitTotal,
        long commitTimeMillis,
        long mergeTotal,
        long mergeTimeMillis,
        long mergeDocsTotal,
        long mergeFailures,
        long deleteTotal,
        long deleteTimeMillis,
        long deleteByGenerationTotal,
        long deleteSharedWriterFallbackTotal,
        long ramBufferBytesUsed,
        long activeWriters,
        long activeReaders
    ) {
        this.docsIndexedTotal = docsIndexedTotal;
        this.docsIndexedFailures = docsIndexedFailures;
        this.indexTimeMillis = indexTimeMillis;
        this.flushTotal = flushTotal;
        this.flushTimeMillis = flushTimeMillis;
        this.flushForceMergeTimeMillis = flushForceMergeTimeMillis;
        this.refreshTotal = refreshTotal;
        this.refreshTimeMillis = refreshTimeMillis;
        this.refreshAddIndexesTimeMillis = refreshAddIndexesTimeMillis;
        this.refreshSegmentsIncorporatedTotal = refreshSegmentsIncorporatedTotal;
        this.commitTotal = commitTotal;
        this.commitTimeMillis = commitTimeMillis;
        this.mergeTotal = mergeTotal;
        this.mergeTimeMillis = mergeTimeMillis;
        this.mergeDocsTotal = mergeDocsTotal;
        this.mergeFailures = mergeFailures;
        this.deleteTotal = deleteTotal;
        this.deleteTimeMillis = deleteTimeMillis;
        this.deleteByGenerationTotal = deleteByGenerationTotal;
        this.deleteSharedWriterFallbackTotal = deleteSharedWriterFallbackTotal;
        this.ramBufferBytesUsed = ramBufferBytesUsed;
        this.activeWriters = activeWriters;
        this.activeReaders = activeReaders;
    }

    public LuceneShardStats(StreamInput in) throws IOException {
        // Indexing
        this.docsIndexedTotal = in.readVLong();
        this.docsIndexedFailures = in.readVLong();
        this.indexTimeMillis = in.readVLong();

        // Flush
        this.flushTotal = in.readVLong();
        this.flushTimeMillis = in.readVLong();
        this.flushForceMergeTimeMillis = in.readVLong();

        // Refresh
        this.refreshTotal = in.readVLong();
        this.refreshTimeMillis = in.readVLong();
        this.refreshAddIndexesTimeMillis = in.readVLong();
        this.refreshSegmentsIncorporatedTotal = in.readVLong();

        // Commit
        this.commitTotal = in.readVLong();
        this.commitTimeMillis = in.readVLong();

        // Merge
        this.mergeTotal = in.readVLong();
        this.mergeTimeMillis = in.readVLong();
        this.mergeDocsTotal = in.readVLong();
        this.mergeFailures = in.readVLong();

        // Delete
        this.deleteTotal = in.readVLong();
        this.deleteTimeMillis = in.readVLong();
        this.deleteByGenerationTotal = in.readVLong();
        this.deleteSharedWriterFallbackTotal = in.readVLong();

        // Memory
        this.ramBufferBytesUsed = in.readVLong();
        this.activeWriters = in.readVLong();
        this.activeReaders = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Indexing
        out.writeVLong(docsIndexedTotal);
        out.writeVLong(docsIndexedFailures);
        out.writeVLong(indexTimeMillis);

        // Flush
        out.writeVLong(flushTotal);
        out.writeVLong(flushTimeMillis);
        out.writeVLong(flushForceMergeTimeMillis);

        // Refresh
        out.writeVLong(refreshTotal);
        out.writeVLong(refreshTimeMillis);
        out.writeVLong(refreshAddIndexesTimeMillis);
        out.writeVLong(refreshSegmentsIncorporatedTotal);

        // Commit
        out.writeVLong(commitTotal);
        out.writeVLong(commitTimeMillis);

        // Merge
        out.writeVLong(mergeTotal);
        out.writeVLong(mergeTimeMillis);
        out.writeVLong(mergeDocsTotal);
        out.writeVLong(mergeFailures);

        // Delete
        out.writeVLong(deleteTotal);
        out.writeVLong(deleteTimeMillis);
        out.writeVLong(deleteByGenerationTotal);
        out.writeVLong(deleteSharedWriterFallbackTotal);

        // Memory
        out.writeVLong(ramBufferBytesUsed);
        out.writeVLong(activeWriters);
        out.writeVLong(activeReaders);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Indexing
        builder.startObject("indexing");
        builder.field("docs_indexed_total", docsIndexedTotal);
        builder.field("docs_indexed_failures", docsIndexedFailures);
        builder.field("index_time_millis", indexTimeMillis);
        builder.endObject();

        // Flush
        builder.startObject("flush");
        builder.field("flush_total", flushTotal);
        builder.field("flush_time_millis", flushTimeMillis);
        builder.field("flush_force_merge_time_millis", flushForceMergeTimeMillis);
        builder.endObject();

        // Refresh
        builder.startObject("refresh");
        builder.field("refresh_total", refreshTotal);
        builder.field("refresh_time_millis", refreshTimeMillis);
        builder.field("refresh_add_indexes_time_millis", refreshAddIndexesTimeMillis);
        builder.field("refresh_segments_incorporated_total", refreshSegmentsIncorporatedTotal);
        builder.endObject();

        // Commit
        builder.startObject("commit");
        builder.field("commit_total", commitTotal);
        builder.field("commit_time_millis", commitTimeMillis);
        builder.endObject();

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal);
        builder.field("merge_time_millis", mergeTimeMillis);
        builder.field("merge_docs_total", mergeDocsTotal);
        builder.field("merge_failures", mergeFailures);
        builder.endObject();

        // Delete
        builder.startObject("delete");
        builder.field("delete_total", deleteTotal);
        builder.field("delete_time_millis", deleteTimeMillis);
        builder.field("delete_by_generation_total", deleteByGenerationTotal);
        builder.field("delete_shared_writer_fallback_total", deleteSharedWriterFallbackTotal);
        builder.endObject();

        // Memory
        builder.startObject("memory");
        builder.field("ram_buffer_bytes_used", ramBufferBytesUsed);
        builder.field("active_writers", activeWriters);
        builder.field("active_readers", activeReaders);
        builder.endObject();

        return builder;
    }

    /**
     * Returns a new snapshot that is the sum of this snapshot and another.
     * Used for aggregation across shards.
     */
    public LuceneShardStats add(LuceneShardStats other) {
        return new LuceneShardStats(
            this.docsIndexedTotal + other.docsIndexedTotal,
            this.docsIndexedFailures + other.docsIndexedFailures,
            this.indexTimeMillis + other.indexTimeMillis,
            this.flushTotal + other.flushTotal,
            this.flushTimeMillis + other.flushTimeMillis,
            this.flushForceMergeTimeMillis + other.flushForceMergeTimeMillis,
            this.refreshTotal + other.refreshTotal,
            this.refreshTimeMillis + other.refreshTimeMillis,
            this.refreshAddIndexesTimeMillis + other.refreshAddIndexesTimeMillis,
            this.refreshSegmentsIncorporatedTotal + other.refreshSegmentsIncorporatedTotal,
            this.commitTotal + other.commitTotal,
            this.commitTimeMillis + other.commitTimeMillis,
            this.mergeTotal + other.mergeTotal,
            this.mergeTimeMillis + other.mergeTimeMillis,
            this.mergeDocsTotal + other.mergeDocsTotal,
            this.mergeFailures + other.mergeFailures,
            this.deleteTotal + other.deleteTotal,
            this.deleteTimeMillis + other.deleteTimeMillis,
            this.deleteByGenerationTotal + other.deleteByGenerationTotal,
            this.deleteSharedWriterFallbackTotal + other.deleteSharedWriterFallbackTotal,
            this.ramBufferBytesUsed + other.ramBufferBytesUsed,
            this.activeWriters + other.activeWriters,
            this.activeReaders + other.activeReaders
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

    public long getFlushTotal() {
        return flushTotal;
    }

    public long getFlushTimeMillis() {
        return flushTimeMillis;
    }

    public long getFlushForceMergeTimeMillis() {
        return flushForceMergeTimeMillis;
    }

    public long getRefreshTotal() {
        return refreshTotal;
    }

    public long getRefreshTimeMillis() {
        return refreshTimeMillis;
    }

    public long getRefreshAddIndexesTimeMillis() {
        return refreshAddIndexesTimeMillis;
    }

    public long getRefreshSegmentsIncorporatedTotal() {
        return refreshSegmentsIncorporatedTotal;
    }

    public long getCommitTotal() {
        return commitTotal;
    }

    public long getCommitTimeMillis() {
        return commitTimeMillis;
    }

    public long getMergeTotal() {
        return mergeTotal;
    }

    public long getMergeTimeMillis() {
        return mergeTimeMillis;
    }

    public long getMergeDocsTotal() {
        return mergeDocsTotal;
    }

    public long getMergeFailures() {
        return mergeFailures;
    }

    public long getDeleteTotal() {
        return deleteTotal;
    }

    public long getDeleteTimeMillis() {
        return deleteTimeMillis;
    }

    public long getDeleteByGenerationTotal() {
        return deleteByGenerationTotal;
    }

    public long getDeleteSharedWriterFallbackTotal() {
        return deleteSharedWriterFallbackTotal;
    }

    public long getRamBufferBytesUsed() {
        return ramBufferBytesUsed;
    }

    public long getActiveWriters() {
        return activeWriters;
    }

    public long getActiveReaders() {
        return activeReaders;
    }
}
