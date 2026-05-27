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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;

/**
 * Immutable point-in-time snapshot of shard-level Lucene statistics.
 * Produced by {@link LuceneShardStatsTracker#stats()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneShardStats implements DataFormatShardStats<LuceneShardStats> {

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

    // Merge
    private final long mergeTotal;
    private final long mergeTimeMillis;
    private final long mergeFailures;

    // Commit
    private final long commitTotal;
    private final long commitTimeMillis;

    // Delete
    private final long deleteTotal;
    private final long deleteTimeMillis;

    /**
     * Returns an empty LuceneShardStats snapshot with all zero counters.
     * Used by transport actions when a shard does not have a Lucene secondary delegate.
     */
    public static LuceneShardStats empty() {
        return new LuceneShardStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

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
        long mergeTotal,
        long mergeTimeMillis,
        long mergeFailures,
        long commitTotal,
        long commitTimeMillis,
        long deleteTotal,
        long deleteTimeMillis
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
        this.mergeTotal = mergeTotal;
        this.mergeTimeMillis = mergeTimeMillis;
        this.mergeFailures = mergeFailures;
        this.commitTotal = commitTotal;
        this.commitTimeMillis = commitTimeMillis;
        this.deleteTotal = deleteTotal;
        this.deleteTimeMillis = deleteTimeMillis;
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

        // Merge
        this.mergeTotal = in.readVLong();
        this.mergeTimeMillis = in.readVLong();
        this.mergeFailures = in.readVLong();

        // Commit
        this.commitTotal = in.readVLong();
        this.commitTimeMillis = in.readVLong();

        // Delete
        this.deleteTotal = in.readVLong();
        this.deleteTimeMillis = in.readVLong();
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

        // Merge
        out.writeVLong(mergeTotal);
        out.writeVLong(mergeTimeMillis);
        out.writeVLong(mergeFailures);

        // Commit
        out.writeVLong(commitTotal);
        out.writeVLong(commitTimeMillis);

        // Delete
        out.writeVLong(deleteTotal);
        out.writeVLong(deleteTimeMillis);
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

        // Merge
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal);
        builder.field("merge_time_millis", mergeTimeMillis);
        builder.field("merge_failures", mergeFailures);
        builder.endObject();

        // Commit
        builder.startObject("commit");
        builder.field("commit_total", commitTotal);
        builder.field("commit_time_millis", commitTimeMillis);
        builder.endObject();

        // Delete
        builder.startObject("delete");
        builder.field("delete_total", deleteTotal);
        builder.field("delete_time_millis", deleteTimeMillis);
        builder.endObject();

        return builder;
    }

    /**
     * Returns a new snapshot that is the sum of this snapshot and another.
     * Used for aggregation across shards.
     */
    @Override
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
            this.mergeTotal + other.mergeTotal,
            this.mergeTimeMillis + other.mergeTimeMillis,
            this.mergeFailures + other.mergeFailures,
            this.commitTotal + other.commitTotal,
            this.commitTimeMillis + other.commitTimeMillis,
            this.deleteTotal + other.deleteTotal,
            this.deleteTimeMillis + other.deleteTimeMillis
        );
    }
}
