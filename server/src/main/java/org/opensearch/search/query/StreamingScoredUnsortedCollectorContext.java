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
 * Streaming collector context for SCORED_UNSORTED mode.
 * Collects documents with scores but no sorting for faster emission.
 */
public class StreamingScoredUnsortedCollectorContext extends TopDocsCollectorContext {

    private final AtomicInteger totalCollected = new AtomicInteger(0);
    private final CircuitBreaker circuitBreaker;
    private static final long SCORE_DOC_BYTES = 24L;
    private final AtomicLong memoryUsed = new AtomicLong(0);

    public StreamingScoredUnsortedCollectorContext(String profilerName, int numHits) {
        super(profilerName, numHits);
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingScoredUnsortedCollectorContext(String profilerName, int numHits, CircuitBreaker breaker) {
        super(profilerName, numHits);
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
            List<ScoreDoc> allDocs = new ArrayList<>();
            float maxScore = Float.NEGATIVE_INFINITY;

            // Combine all collected documents from all collectors
            for (StreamingScoredUnsortedCollector collector : collectors) {
                List<ScoreDoc> collectorDocs = collector.getCollectedDocs();
                allDocs.addAll(collectorDocs);

                // Track max score
                for (ScoreDoc doc : collectorDocs) {
                    if (!Float.isNaN(doc.score) && doc.score > maxScore) {
                        maxScore = doc.score;
                    }
                }
            }

            // NO SORTING for SCORED_UNSORTED mode - keep in encounter order

            // Limit to numHits if we collected more
            if (allDocs.size() > numHits()) {
                allDocs = allDocs.subList(0, numHits());
            }

            // Create TopDocs with actual scores but no sorting
            ScoreDoc[] scoreDocs = allDocs.toArray(new ScoreDoc[0]);
            TotalHits totalHits = new TotalHits(allDocs.size(), TotalHits.Relation.EQUAL_TO);

            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);

            // Use actual maxScore if we found any, otherwise Float.NaN
            float finalMaxScore = (maxScore > Float.NEGATIVE_INFINITY) ? maxScore : Float.NaN;

            // Return a ReduceableSearchResult that can set the TopDocs
            return result -> {
                // CRITICAL: Set the TopDocs in the QuerySearchResult
                result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, finalMaxScore), null);
            };
        }
    }

    /**
     * Collector that actually collects documents with scores but no sorting
     */
    private class StreamingScoredUnsortedCollector implements Collector {

        private final List<ScoreDoc> collectedDocs = new ArrayList<>();

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
                    // Scoring needed for SCORED_UNSORTED mode
                    this.scorer = scorer;
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (collectedDocs.size() < numHits()) {
                        // Get actual score from scorer
                        float score = this.scorer.score();
                        ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, score);
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
                    }
                }
            };
        }

        public List<ScoreDoc> getCollectedDocs() {
            return collectedDocs;
        }
    }
}
