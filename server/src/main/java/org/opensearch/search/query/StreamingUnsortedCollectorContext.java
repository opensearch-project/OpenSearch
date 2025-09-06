package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streaming collector context for NO_SCORING mode.
 * Collects documents without scoring for fastest emission.
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
                public void setScorer(Scorable scorer) throws IOException {
                }

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
