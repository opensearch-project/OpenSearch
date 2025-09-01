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
    
    private final AtomicInteger totalCollected = new AtomicInteger(0);
    private final CircuitBreaker circuitBreaker;
    private static final long SCORE_DOC_BYTES = 24L;
    private final AtomicLong memoryUsed = new AtomicLong(0);
    private final SearchContext searchContext; // Add SearchContext access

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
        // CRITICAL: Check if already consumed before accessing topDocs()
        if (result.hasConsumedTopDocs()) {
            // Result already consumed, nothing to do
            return;
        }

        // For single-threaded execution path, ensure TopDocs is set
        if (result.topDocs() == null) {
            // Create a basic TopDocs if none exists
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
            List<ScoreDoc> allDocs = new ArrayList<>();

            // DEBUG: Log reduction details
            logger.debug("StreamingUnsortedCollectorManager.reduce() - numHits: {}, collectors.size: {}", 
                        numHits(), collectors.size());

            // Combine all collected documents from all collectors
            for (StreamingUnsortedCollector collector : collectors) {
                List<ScoreDoc> collectorDocs = collector.getCollectedDocs();
                logger.debug("Collector returned {} docs", collectorDocs.size());
                allDocs.addAll(collectorDocs);
            }

            logger.debug("Total docs collected: {}", allDocs.size());

            // Limit to numHits if we collected more
            if (allDocs.size() > numHits()) {
                logger.debug("Limiting from {} to {} docs", allDocs.size(), numHits());
                allDocs = allDocs.subList(0, numHits());
            }

            // Create TopDocs with Float.NaN scores for NO_SCORING mode
            ScoreDoc[] scoreDocs = allDocs.toArray(new ScoreDoc[0]);
            TotalHits totalHits = new TotalHits(allDocs.size(), TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);

            logger.debug("Final TopDocs created with {} docs", allDocs.size());

            // Return a ReduceableSearchResult that can set the TopDocs
            return result -> {
                // CRITICAL: Set the TopDocs in the QuerySearchResult
                result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
            };
        }
    }

    /**
     * Collector that actually collects documents without scoring
     */
    private class StreamingUnsortedCollector implements Collector {

        private final List<ScoreDoc> collectedDocs = new ArrayList<>();
        private final List<ScoreDoc> allCollectedDocs = new ArrayList<>(); // Keep track of all docs for final result

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES; // No scoring needed for NO_SCORING mode
        }

        @Override
        public LeafCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // No scoring needed for NO_SCORING mode
                }

                @Override
                public void collect(int doc) throws IOException {
                    // DEBUG: Verify streaming collector is being used
                    logger.debug("StreamingUnsortedCollector.collect() called with doc: {}", doc);
                    
                    if (collectedDocs.size() < numHits()) {
                        // Collect document with Float.NaN score for NO_SCORING mode
                        ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, Float.NaN);
                        collectedDocs.add(scoreDoc);
                        allCollectedDocs.add(scoreDoc); // Keep track of all docs for final result
                        totalCollected.incrementAndGet();

                        // NEW: Emit batch when threshold reached (every 10 docs)
                        if (collectedDocs.size() % 10 == 0) {
                            emitCurrentBatch(false);
                        }

                        // NEW: Add memory check every 100 docs
                        if (circuitBreaker != null && collectedDocs.size() % 100 == 0) {
                            long bytesNeeded = collectedDocs.size() * SCORE_DOC_BYTES;
                            long bytesToAdd = bytesNeeded - memoryUsed.get();
                            try {
                                circuitBreaker.addEstimateBytesAndMaybeBreak(bytesToAdd, "streaming_collector");
                                memoryUsed.set(bytesNeeded);
                            } catch (CircuitBreakingException e) {
                                // Clean up and rethrow
                                collectedDocs.clear();
                                throw e;
                            }
                        }
                    } else {
                        logger.debug("Skipping doc {} - already have {} docs", doc, collectedDocs.size());
                    }
                }
            };
        }

        public List<ScoreDoc> getCollectedDocs() {
            logger.debug("StreamingUnsortedCollector.getCollectedDocs() - returning {} docs", allCollectedDocs.size());
            return allCollectedDocs;
        }

        /**
         * Emit current batch of collected documents through streaming channel
         */
        private void emitCurrentBatch(boolean isFinal) {
            if (collectedDocs.isEmpty()) return;
            
            try {
                // Create partial result
                QuerySearchResult partial = new QuerySearchResult();
                TopDocs topDocs = new TopDocs(
                    new TotalHits(collectedDocs.size(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                    collectedDocs.toArray(new ScoreDoc[0])
                );
                partial.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
                partial.setPartial(!isFinal);

                // Emit through listener if available
                if (searchContext != null && searchContext.getStreamChannelListener() != null) {
                    logger.info("🚀 STREAMING: About to emit batch of {} docs, isFinal: {}", collectedDocs.size(), isFinal);
                    searchContext.getStreamChannelListener().onStreamResponse(partial, isFinal);
                    logger.info("✅ STREAMING: Successfully emitted batch of {} docs, isFinal: {}", collectedDocs.size(), isFinal);
                } else {
                    logger.warn("⚠️ STREAMING: No stream channel listener available for emission - test environment");
                }
                
                // Clear the batch to avoid accumulating too many docs
                if (!isFinal) {
                    collectedDocs.clear();
                }
            } catch (Exception e) {
                logger.error("Failed to emit batch", e);
            }
        }
    }
}
