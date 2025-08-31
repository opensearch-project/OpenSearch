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

    private final AtomicInteger totalCollected = new AtomicInteger(0);
    private final CircuitBreaker circuitBreaker;
    private static final long SCORE_DOC_BYTES = 24L;
    private final AtomicLong memoryUsed = new AtomicLong(0);

    public StreamingUnsortedCollectorContext(String profilerName, int numHits) {
        super(profilerName, numHits);
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingUnsortedCollectorContext(String profilerName, int numHits, CircuitBreaker breaker) {
        super(profilerName, numHits);
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
            System.out.println("DEBUG: StreamingUnsortedCollectorManager.reduce() - numHits: " + numHits() + 
                             ", collectors.size: " + collectors.size());

            // Combine all collected documents from all collectors
            for (StreamingUnsortedCollector collector : collectors) {
                List<ScoreDoc> collectorDocs = collector.getCollectedDocs();
                System.out.println("DEBUG: Collector returned " + collectorDocs.size() + " docs");
                allDocs.addAll(collectorDocs);
            }

            System.out.println("DEBUG: Total docs collected: " + allDocs.size());

            // Limit to numHits if we collected more
            if (allDocs.size() > numHits()) {
                System.out.println("DEBUG: Limiting from " + allDocs.size() + " to " + numHits() + " docs");
                allDocs = allDocs.subList(0, numHits());
            }

            // Create TopDocs with Float.NaN scores for NO_SCORING mode
            ScoreDoc[] scoreDocs = allDocs.toArray(new ScoreDoc[0]);
            TotalHits totalHits = new TotalHits(allDocs.size(), TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);

            System.out.println("DEBUG: Final TopDocs created with " + allDocs.size() + " docs");

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
                    // DEBUG: Log collection details
                    System.out.println("DEBUG: StreamingUnsortedCollector.collect() - doc: " + doc + 
                                     ", context.docBase: " + context.docBase + 
                                     ", collectedDocs.size: " + collectedDocs.size() + 
                                     ", numHits: " + numHits());
                    
                    if (collectedDocs.size() < numHits()) {
                        // Collect document with Float.NaN score for NO_SCORING mode
                        ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, Float.NaN);
                        collectedDocs.add(scoreDoc);
                        totalCollected.incrementAndGet();

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
                        System.out.println("DEBUG: Skipping doc " + doc + " - already have " + collectedDocs.size() + " docs");
                    }
                }
            };
        }

        public List<ScoreDoc> getCollectedDocs() {
            System.out.println("DEBUG: StreamingUnsortedCollector.getCollectedDocs() - returning " + collectedDocs.size() + " docs");
            return collectedDocs;
        }
    }
}
