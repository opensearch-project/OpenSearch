/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.stream.StreamingScoreCollectorManager;

import java.io.IOException;

/**
 * Streaming collector context for unsorted (score-based) search.
 * Uses the new shard-level streaming aggregator architecture to eliminate
 * per-segment collection boundaries and enable progressive result emission.
 *
 * This implementation replaces the previous per-segment batch emission approach
 * with a shard-level aggregator that maintains Top-K state across all segments
 * and emits progressive results during collection.
 */
public class StreamingUnsortedCollectorContext extends TopDocsCollectorContext {

    private static final Logger logger = LogManager.getLogger(StreamingUnsortedCollectorContext.class);

    private final SearchContext searchContext;

    public StreamingUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
    }

    public StreamingUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // Legacy single collector path - delegate to manager approach
        CollectorManager<?, ReduceableSearchResult> manager = createManager(null);
        return manager.newCollector();
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        // Get streaming configuration from search context
        int batchDocThreshold = searchContext.getStreamingBatchSize();
        TimeValue timeInterval = getTimeInterval();
        boolean firstHitImmediate = getFirstHitImmediate();
        boolean enableCoalescing = getEnableCoalescing();
        boolean trackMaxScore = false; // Unsorted mode typically doesn't need max score

        return new StreamingScoreCollectorManager(
            searchContext,
            numHits(),
            trackMaxScore,
            batchDocThreshold,
            timeInterval,
            firstHitImmediate,
            enableCoalescing
        );
    }

    @Override
    public void postProcess(org.opensearch.search.query.QuerySearchResult result) throws IOException {
        if (result.hasConsumedTopDocs()) {
            return;
        }

        if (result.topDocs() == null) {
            ScoreDoc[] scoreDocs = new ScoreDoc[0];
            TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
            result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    /**
     * Get the time interval for emission guards from search context.
     */
    private TimeValue getTimeInterval() {
        // Default to 100ms if not configured
        return searchContext.getStreamingTimeInterval() != null ? searchContext.getStreamingTimeInterval() : TimeValue.timeValueMillis(100);
    }

    /**
     * Get the first-hit immediate emission setting.
     */
    private boolean getFirstHitImmediate() {
        // Default to true for better TTFB
        return searchContext.getStreamingFirstHitImmediate() != null ? searchContext.getStreamingFirstHitImmediate() : true;
    }

    /**
     * Get the coalescing setting.
     */
    private boolean getEnableCoalescing() {
        // Default to true to reduce duplicate emissions
        return searchContext.getStreamingEnableCoalescing() != null ? searchContext.getStreamingEnableCoalescing() : true;
    }
}
