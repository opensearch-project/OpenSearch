/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.stream.StreamingSortCollectorManager;

import java.io.IOException;

/**
 * Streaming collector context for sorted search.
 * Uses the new shard-level streaming aggregator architecture to eliminate
 * per-segment collection boundaries and enable progressive result emission.
 *
 * This implementation replaces the previous Lucene TopScoreDocCollectorManager approach
 * with a shard-level aggregator that maintains Top-K state across all segments
 * and emits progressive results during collection.
 */
public class StreamingSortedCollectorContext extends TopDocsCollectorContext {

    private final Sort sort;
    private final CircuitBreaker circuitBreaker;
    private final SearchContext searchContext;

    public StreamingSortedCollectorContext(String profilerName, int numHits, SearchContext searchContext) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.sort = Sort.RELEVANCE;
        this.circuitBreaker = null;
    }

    public StreamingSortedCollectorContext(String profilerName, int numHits, SearchContext searchContext, Sort sort) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.sort = sort != null ? sort : Sort.RELEVANCE;
        this.circuitBreaker = null;
    }

    public StreamingSortedCollectorContext(
        String profilerName,
        int numHits,
        SearchContext searchContext,
        Sort sort,
        CircuitBreaker breaker
    ) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.sort = sort != null ? sort : Sort.RELEVANCE;
        this.circuitBreaker = breaker;
    }

    public StreamingSortedCollectorContext(String profilerName, int numHits, SearchContext searchContext, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.sort = Sort.RELEVANCE;
        this.circuitBreaker = breaker;
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
        boolean trackMaxScore = needsScores();

        return new StreamingSortCollectorManager(
            searchContext,
            numHits(),
            sort,
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

    /**
     * Determine if scores are needed based on the sort configuration.
     */
    private boolean needsScores() {
        if (sort == null || sort == Sort.RELEVANCE) {
            return true;
        }

        // Check if any sort field requires scores
        for (org.apache.lucene.search.SortField sortField : sort.getSort()) {
            if (sortField.getType() == org.apache.lucene.search.SortField.Type.SCORE) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get the sort configuration.
     */
    public Sort getSort() {
        return sort;
    }
}
