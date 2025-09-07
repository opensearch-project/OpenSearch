package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streaming collector context for SCORED_UNSORTED mode.
 * Collects documents with scores but without sorting for fast emission with relevance.
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
public class StreamingScoredUnsortedCollectorContext extends TopDocsCollectorContext {

    private final CircuitBreaker circuitBreaker;
    private final SearchContext searchContext;

    public StreamingScoredUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingScoredUnsortedCollectorContext(String profilerName, int numHits, SearchContext searchContext, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.searchContext = searchContext;
        this.circuitBreaker = breaker;
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For SCORED_UNSORTED mode, we need scoring but no sorting
        return new StreamingScoredUnsortedCollector();
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingScoredUnsortedCollectorManager();
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
     * Collector manager for streaming scored unsorted collection
     */
    private class StreamingScoredUnsortedCollectorManager
        implements
            CollectorManager<StreamingScoredUnsortedCollector, ReduceableSearchResult> {

        @Override
        public StreamingScoredUnsortedCollector newCollector() throws IOException {
            return new StreamingScoredUnsortedCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingScoredUnsortedCollector> collectors) throws IOException {
            // Keep top K by score across collectors (min-heap behavior simulated by linear merge due to small K)
            List<ScoreDoc> mergedTopK = new ArrayList<>();
            int totalHits = 0;
            float maxScore = Float.NEGATIVE_INFINITY;

            for (StreamingScoredUnsortedCollector collector : collectors) {
                List<ScoreDoc> topK = collector.getTopKDocs();
                totalHits += collector.getTotalHitsCount();
                for (ScoreDoc d : topK) {
                    mergedTopK.add(d);
                    if (!Float.isNaN(d.score) && d.score > maxScore) {
                        maxScore = d.score;
                    }
                }
            }

            // If more than K, keep highest scores only
            if (mergedTopK.size() > numHits()) {
                mergedTopK.sort((a, b) -> Float.compare(b.score, a.score));
                mergedTopK = mergedTopK.subList(0, numHits());
            }

            ScoreDoc[] scoreDocs = mergedTopK.toArray(new ScoreDoc[0]);
            TopDocs topDocs = new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
            float finalMaxScore = (maxScore > Float.NEGATIVE_INFINITY) ? maxScore : Float.NaN;

            return result -> result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, finalMaxScore), null);
        }
    }

    /**
     * Collector that actually collects documents with scores but no sorting
     */
    private class StreamingScoredUnsortedCollector implements Collector {

        private final int batchSize = Math.max(1, searchContext != null ? searchContext.getStreamingBatchSize() : 10);
        private final List<ScoreDoc> currentBatch = new ArrayList<>(batchSize);
        private final List<ScoreDoc> topK = new ArrayList<>(numHits());
        private int totalHitsCount = 0;

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE; // Need scores for SCORED_UNSORTED mode
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
                // Silently ignore - streaming is best effort
            }
        }
    }
}
