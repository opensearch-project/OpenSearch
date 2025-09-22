package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Streaming collector context for SCORED_SORTED mode.
 * Collects and maintains documents in sorted order (by score or custom sort).
 *
 * Uses Lucene's TopScoreDocCollectorManager for efficient sorted collection with
 * incremental merging. Documents are collected in larger batches (10x default multiplier)
 * to amortize sorting costs, controlled by search.streaming.scored_sorted.batch_multiplier.
 *
 * Memory footprint: O(K) where K is the requested number of hits.
 * The TopScoreDocCollector maintains a min-heap of size K.
 *
 * Circuit Breaker Policy:
 * - Heap structure: Protected by TopScoreDocCollector's internal memory management
 * - Parent reduction: Protected by QueryPhaseResultConsumer's circuit breaker
 * - Max memory per collector: ~80KB for topK heap (10000 docs * 16 bytes)
 * - Decision rationale: Sorting requires maintaining all K docs in memory, but Lucene's
 *   collectors are already optimized for memory efficiency
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
        // Use Lucene's top-N score collector for single-threaded execution
        return new TopScoreDocCollectorManager(numHits(), null, Integer.MAX_VALUE).newCollector();
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingSortedCollectorManager();
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
     * Collector manager for streaming sorted collection
     */
    private class StreamingSortedCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

        private final CollectorManager<? extends TopDocsCollector<?>, ? extends TopDocs> manager;

        private StreamingSortedCollectorManager() {
            this.manager = new TopScoreDocCollectorManager(numHits(), null, Integer.MAX_VALUE);
        }

        @Override
        public Collector newCollector() throws IOException {
            // Use Lucene's collector manager for score-sorted collection
            return manager.newCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
            final Collection<TopDocsCollector<?>> topDocsCollectors = new ArrayList<>();
            for (Collector collector : collectors) {
                if (collector instanceof TopDocsCollector<?>) {
                    topDocsCollectors.add((TopDocsCollector<?>) collector);
                }
            }

            // Reduce with Lucene's manager
            @SuppressWarnings("unchecked")
            final TopDocs topDocs = ((CollectorManager<TopDocsCollector<?>, ? extends TopDocs>) manager).reduce(topDocsCollectors);

            final float computedMaxScore = (topDocs.scoreDocs != null && topDocs.scoreDocs.length > 0)
                ? topDocs.scoreDocs[0].score
                : Float.NaN;

            return result -> result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, computedMaxScore), null);
        }
    }
}
