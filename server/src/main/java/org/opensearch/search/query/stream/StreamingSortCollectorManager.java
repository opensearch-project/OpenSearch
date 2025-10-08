/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;

/**
 * CollectorManager for streaming sorted search.
 * Manages a single SortedShardTopDocsStreamer per shard to avoid
 * per-segment collection boundaries and enable progressive result emission.
 *
 * @opensearch.internal
 */
public class StreamingSortCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private final SearchContext searchContext;
    private final int numHits;
    private final Sort sort;
    private final boolean trackMaxScore;
    private final SortedShardTopDocsStreamer streamer;
    private volatile boolean collectorCreated = false;
    private final Object collectorLock = new Object();

    public StreamingSortCollectorManager(
        SearchContext searchContext,
        int numHits,
        Sort sort,
        boolean trackMaxScore,
        int batchDocThreshold,
        TimeValue timeInterval,
        boolean firstHitImmediate,
        boolean enableCoalescing
    ) {
        this.searchContext = searchContext;
        this.numHits = numHits;
        this.sort = sort;
        this.trackMaxScore = trackMaxScore;
        this.streamer = new SortedShardTopDocsStreamer(
            numHits,
            sort,
            trackMaxScore,
            batchDocThreshold,
            timeInterval,
            firstHitImmediate,
            enableCoalescing
        );

        // Initialize the streamer immediately
        this.streamer.onStart(searchContext);
    }

    @Override
    public Collector newCollector() throws IOException {
        // Enforce single collector per shard for streaming (thread-safe)
        synchronized (collectorLock) {
            if (collectorCreated) {
                throw new IllegalStateException(
                    "Streaming search requires exactly one collector per shard. "
                        + "Multiple collectors detected - this indicates concurrent segment search is enabled. "
                        + "Please disable concurrent segment search for streaming queries."
                );
            }

            collectorCreated = true;
            return new StreamingCollector();
        }
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        if (collectors == null || collectors.isEmpty()) {
            throw new IllegalStateException("No collectors provided for streaming search reduction");
        }

        if (collectors.size() != 1) {
            throw new IllegalStateException(
                "Streaming search requires exactly one collector per shard, got: "
                    + collectors.size()
                    + ". This indicates concurrent segment search is enabled, which is incompatible with streaming search."
            );
        }

        // Verify the collector is our expected type
        Collector collector = collectors.iterator().next();
        if (!(collector instanceof StreamingCollector)) {
            throw new IllegalStateException("Unexpected collector type for streaming search: " + collector.getClass().getName());
        }

        // Finalize the aggregator
        streamer.onFinish();

        // Return a ReduceableSearchResult wrapping our final TopDocs
        return new StreamingReduceableSearchResult(streamer);
    }

    /**
     * Get the streamer managed by this collector manager.
     *
     * @return the streaming shard streamer
     */
    public SortedShardTopDocsStreamer getStreamer() {
        return streamer;
    }

    /**
     * Get the sort configuration.
     *
     * @return the sort
     */
    public Sort getSort() {
        return sort;
    }

    /**
     * Collector implementation that delegates to the streaming streamer.
     */
    private class StreamingCollector implements Collector {

        @Override
        public LeafCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext context) throws IOException {
            return streamer.newLeafCollector(context, null);
        }

        @Override
        public ScoreMode scoreMode() {
            // For sorted search, we need complete scoring if:
            // 1. We're tracking max score, OR
            // 2. The sort includes score as a sort field
            if (trackMaxScore) {
                return ScoreMode.COMPLETE;
            }

            // Check if sort includes score
            for (org.apache.lucene.search.SortField sortField : sort.getSort()) {
                if (sortField.getType() == org.apache.lucene.search.SortField.Type.SCORE) {
                    return ScoreMode.COMPLETE;
                }
            }

            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    /**
     * ReduceableSearchResult implementation for streaming sorted results.
     */
    private static class StreamingReduceableSearchResult implements ReduceableSearchResult {
        private final SortedShardTopDocsStreamer streamer;

        StreamingReduceableSearchResult(SortedShardTopDocsStreamer streamer) {
            this.streamer = streamer;
        }

        @Override
        public void reduce(QuerySearchResult result) throws IOException {
            // Set the final TopDocsAndMaxScore on the query result
            org.opensearch.common.lucene.search.TopDocsAndMaxScore finalResult = streamer.buildFinalTopDocs();
            result.topDocs(finalResult, null);
        }

        /**
         * Get access to the underlying streamer for metrics.
         */
        public SortedShardTopDocsStreamer getStreamer() {
            return streamer;
        }
    }
}
