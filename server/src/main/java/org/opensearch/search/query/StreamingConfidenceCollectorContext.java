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
 * Streaming collector context for CONFIDENCE_BASED mode.
 * Collects documents with scores for progressive confidence-based emission.
 *
 * Implements memory-bounded collection using a "topK" pattern where the best K
 * documents by score are retained. Documents are collected in batches controlled
 * by search.streaming.batch_size setting (default: 10, max: 100).
 *
 * Memory footprint: O(K + batchSize) where K is the requested number of hits.
 *
 * Circuit Breaker Policy:
 * - Batch buffers: No CB checks as they're strictly bounded (10-100 docs) and cleared after emission
 * - TopK list: Protected by parent QueryPhaseResultConsumer's circuit breaker during final reduction
 * - Max memory per collector: ~8KB for batch (100 docs * 16 bytes) + ~80KB for topK (10000 docs * 16 bytes)
 * - Decision rationale: The overhead of CB checks (atomic operations) would exceed the memory saved
 *   for such small, bounded allocations that are immediately released
 */
public class StreamingConfidenceCollectorContext extends TopDocsCollectorContext {

    private static final Logger logger = LogManager.getLogger(StreamingConfidenceCollectorContext.class);

    private final CircuitBreaker circuitBreaker;
    private final SearchContext searchContext;

    public StreamingConfidenceCollectorContext(String profilerName, int numHits, SearchContext searchContext) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingConfidenceCollectorContext(String profilerName, int numHits, SearchContext searchContext, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.circuitBreaker = breaker;
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For CONFIDENCE_BASED mode, we need scoring but no sorting (for now)
        return new StreamingConfidenceCollector();
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingConfidenceCollectorManager();
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
     * Collector manager for streaming confidence-based collection
     */
    private class StreamingConfidenceCollectorManager implements CollectorManager<StreamingConfidenceCollector, ReduceableSearchResult> {

        @Override
        public StreamingConfidenceCollector newCollector() throws IOException {
            return new StreamingConfidenceCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingConfidenceCollector> collectors) throws IOException {
            // Merge topK across collectors and cap to K
            List<ScoreDoc> mergedTopK = new ArrayList<>();
            float maxScore = Float.NEGATIVE_INFINITY;
            int totalHitsCount = 0;

            for (StreamingConfidenceCollector collector : collectors) {
                totalHitsCount += collector.getTotalHitsCount();
                for (ScoreDoc d : collector.getTopKDocs()) {
                    mergedTopK.add(d);
                    if (!Float.isNaN(d.score) && d.score > maxScore) {
                        maxScore = d.score;
                    }
                }
            }

            if (mergedTopK.size() > numHits()) {
                mergedTopK.sort((a, b) -> Float.compare(b.score, a.score));
                mergedTopK = mergedTopK.subList(0, numHits());
            }

            ScoreDoc[] scoreDocs = mergedTopK.toArray(new ScoreDoc[0]);
            TotalHits totalHits = new TotalHits(totalHitsCount, TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);

            float finalMaxScore = (maxScore > Float.NEGATIVE_INFINITY) ? maxScore : Float.NaN;
            return result -> result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, finalMaxScore), null);
        }
    }

    /**
     * Collector for confidence-based document collection.
     */
    private class StreamingConfidenceCollector implements Collector {

        private final int batchSize = Math.max(1, searchContext != null ? searchContext.getStreamingBatchSize() : 10);
        private final List<ScoreDoc> currentBatch = new ArrayList<>(batchSize);
        private final List<ScoreDoc> topK = new ArrayList<>(numHits());
        private int totalHitsCount = 0;

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE; // Need scores for CONFIDENCE_BASED mode
        }

        @Override
        public LeafCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                private Scorable scorer;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    this.scorer = scorer;
                }

                @Override
                public void collect(int doc) throws IOException {
                    totalHitsCount++;
                    float score = this.scorer.score();
                    ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, score);

                    currentBatch.add(scoreDoc);
                    if (currentBatch.size() >= batchSize) {
                        emitCurrentBatch(false);
                        currentBatch.clear();
                    }

                    if (topK.size() < numHits()) {
                        topK.add(scoreDoc);
                    } else {
                        int minIdx = 0;
                        float minScore = topK.get(0).score;
                        for (int i = 1; i < topK.size(); i++) {
                            if (topK.get(i).score < minScore) {
                                minScore = topK.get(i).score;
                                minIdx = i;
                            }
                        }
                        if (score > minScore) {
                            topK.set(minIdx, scoreDoc);
                        }
                    }
                }
            };
        }

        public List<ScoreDoc> getTopKDocs() {
            return topK;
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
            } catch (Exception e) {
                logger.trace("Failed to emit streaming batch", e);
            }
        }
    }
}
