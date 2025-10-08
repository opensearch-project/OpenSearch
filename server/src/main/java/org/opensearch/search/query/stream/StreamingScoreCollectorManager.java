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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;

/**
 * CollectorManager for streaming unsorted search.
 *
 * @opensearch.internal
 */
public class StreamingScoreCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private final SearchContext searchContext;
    private final int numHits;
    private final boolean trackMaxScore;
    private final UnsortedShardTopDocsStreamer streamer;
    private volatile boolean collectorCreated = false;
    private final Object collectorLock = new Object();

    public StreamingScoreCollectorManager(
        SearchContext searchContext,
        int numHits,
        boolean trackMaxScore,
        int batchDocThreshold,
        TimeValue timeInterval,
        boolean firstHitImmediate,
        boolean enableCoalescing
    ) {
        this.searchContext = searchContext;
        this.numHits = numHits;
        this.trackMaxScore = trackMaxScore;
        this.streamer = new UnsortedShardTopDocsStreamer(
            numHits,
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

        // Finalize the streamer
        streamer.onFinish();

        // Return a ReduceableSearchResult wrapping our final TopDocs
        return new StreamingReduceableSearchResult(streamer);
    }

    /**
     * Get the streamer managed by this collector manager.
     *
     * @return the streaming shard streamer
     */
    public UnsortedShardTopDocsStreamer getStreamer() {
        return streamer;
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
            // For unsorted (score-based) search, we always need COMPLETE scoring
            // since we're ranking by score
            return ScoreMode.COMPLETE;
        }
    }

    /**
     * ReduceableSearchResult implementation for streaming results.
     */
    private static class StreamingReduceableSearchResult implements ReduceableSearchResult {
        private final UnsortedShardTopDocsStreamer streamer;

        StreamingReduceableSearchResult(UnsortedShardTopDocsStreamer streamer) {
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
        public UnsortedShardTopDocsStreamer getStreamer() {
            return streamer;
        }
    }
}
