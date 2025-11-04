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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Streaming collector context for NO_SCORING mode.
 * Collects documents without scoring for fastest emission.
 *
 * Implements memory-bounded collection using a "firstK" pattern where only the first K
 * documents are retained for the final result. Documents are collected in batches
 * controlled by search.streaming.batch_size setting (default: 10, max: 100).
 *
 * Memory footprint: O(K + batchSize) where K is the requested number of hits.
 *
 * Circuit Breaker Policy:
 * - Batch buffers: No CB checks as they're strictly bounded (10-100 docs) and cleared after emission
 * - FirstK list: Protected by parent QueryPhaseResultConsumer's circuit breaker during final reduction
 * - Max memory per collector: ~8KB for batch (100 docs * 16 bytes) + ~80KB for firstK (10000 docs * 16 bytes)
 * - Decision rationale: The overhead of CB checks (atomic operations) would exceed the memory saved
 *   for such small, bounded allocations that are immediately released
 */
public class StreamingUnsortedCollectorContext extends TopDocsCollectorContext {

    private static final Logger logger = LogManager.getLogger(StreamingUnsortedCollectorContext.class);

    private final CircuitBreaker circuitBreaker;
    private final SearchContext searchContext;

    public StreamingUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.circuitBreaker = breaker;
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For NO_SCORING mode, we don't need scoring
        return new StreamingUnsortedCollector();
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingUnsortedCollectorManager();
    }

    @Override
    public void postProcess(org.opensearch.search.query.QuerySearchResult result) throws IOException {
        // Ensure topDocs is present to avoid serialization NPEs
        if (!result.hasTopDocs()) {
            ScoreDoc[] scoreDocs = new ScoreDoc[0];
            TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
            result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    /**
     * Collector manager for streaming unsorted collection
     */
    private class StreamingUnsortedCollectorManager implements CollectorManager<StreamingUnsortedCollector, ReduceableSearchResult> {

        @Override
        public StreamingUnsortedCollector newCollector() throws IOException {
            return new StreamingUnsortedCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingUnsortedCollector> collectors) throws IOException {
            List<ScoreDoc> mergedFirstK = new ArrayList<>();
            int totalHits = 0;

            for (StreamingUnsortedCollector collector : collectors) {
                mergedFirstK.addAll(collector.getFirstKDocs());
                totalHits += collector.getTotalHitsCount();
            }

            if (mergedFirstK.size() > numHits()) {
                mergedFirstK = mergedFirstK.subList(0, numHits());
            }

            ScoreDoc[] scoreDocs = mergedFirstK.toArray(new ScoreDoc[0]);
            TopDocs topDocs = new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);

            return result -> result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    /**
     * Collector that actually collects documents without scoring
     */
    private class StreamingUnsortedCollector implements Collector {

        private final int batchSize = Math.max(1, searchContext != null ? searchContext.getStreamingBatchSize() : 10);
        private final List<ScoreDoc> currentBatch = new ArrayList<>(batchSize);
        private final List<ScoreDoc> firstK = new ArrayList<>(numHits());
        private int totalHitsCount = 0;

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES; // No scoring needed for NO_SCORING mode
        }

        @Override
        public LeafCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    totalHitsCount++;
                    ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, Float.NaN);

                    currentBatch.add(scoreDoc);
                    if (currentBatch.size() >= batchSize) {
                        emitCurrentBatch(false);
                        currentBatch.clear();
                    }

                    if (firstK.size() < numHits()) {
                        firstK.add(scoreDoc);
                    }
                }
            };
        }

        public List<ScoreDoc> getFirstKDocs() {
            return firstK;
        }

        public int getTotalHitsCount() {
            return totalHitsCount;
        }

        /**
         * Emit current batch of collected documents through streaming channel
         */
        private void emitCurrentBatch(boolean isFinal) {
            if (currentBatch.isEmpty()) return;

            try {
                // Create partial result
                QuerySearchResult partial = new QuerySearchResult();
                TopDocs topDocs = new TopDocs(
                    new TotalHits(currentBatch.size(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                    currentBatch.toArray(new ScoreDoc[0])
                );
                partial.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
                partial.setPartial(!isFinal);

                if (searchContext != null && searchContext.getStreamChannelListener() != null) {
                    searchContext.getStreamChannelListener().onStreamResponse(partial, isFinal);
                }

                if (!isFinal) {
                    currentBatch.clear();
                }
            } catch (Exception e) {
                logger.trace("Failed to emit streaming batch", e);
            }
        }
    }
}
