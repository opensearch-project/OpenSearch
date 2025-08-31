package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.DocValueFormat;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streaming collector context for SCORED_SORTED mode.
 * Collects documents with scores and proper sorting.
 */
public class StreamingSortedCollectorContext extends TopDocsCollectorContext {

    private final Sort sort;
    private final AtomicInteger totalCollected = new AtomicInteger(0);
    private final CircuitBreaker circuitBreaker;
    private static final long SCORE_DOC_BYTES = 24L;
    private final AtomicLong memoryUsed = new AtomicLong(0);

    public StreamingSortedCollectorContext(String profilerName, int numHits) {
        super(profilerName, numHits);
        // Default to relevance if no sort specified
        this.sort = Sort.RELEVANCE;
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingSortedCollectorContext(String profilerName, int numHits, Sort sort) {
        super(profilerName, numHits);
        this.sort = sort != null ? sort : Sort.RELEVANCE;
        this.circuitBreaker = null; // Will work but no protection
    }

    public StreamingSortedCollectorContext(String profilerName, int numHits, Sort sort, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.sort = sort != null ? sort : Sort.RELEVANCE;
        this.circuitBreaker = breaker;
    }

    public StreamingSortedCollectorContext(String profilerName, int numHits, CircuitBreaker breaker) {
        super(profilerName, numHits);
        this.sort = Sort.RELEVANCE;
        this.circuitBreaker = breaker;
    }

    @Override
    public Collector create(Collector in) throws IOException {
        // For SCORED_SORTED mode, we need scoring, just collect docs with scores
        return new StreamingSortedCollector();
    }

    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingSortedCollectorManager();
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
            TopFieldDocs topDocs = new TopFieldDocs(totalHits, scoreDocs, sort.getSort());
            result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    /**
     * Collector manager for streaming sorted collection
     */
    private class StreamingSortedCollectorManager implements CollectorManager<StreamingSortedCollector, ReduceableSearchResult> {

        @Override
        public StreamingSortedCollector newCollector() throws IOException {
            return new StreamingSortedCollector();
        }

        @Override
        public ReduceableSearchResult reduce(Collection<StreamingSortedCollector> collectors) throws IOException {
            List<FieldDoc> allDocs = new ArrayList<>();
            float maxScore = Float.NEGATIVE_INFINITY;

            // Combine all collected documents from all collectors
            for (StreamingSortedCollector collector : collectors) {
                List<FieldDoc> collectorDocs = collector.getCollectedDocs();
                allDocs.addAll(collectorDocs);

                // Track max score
                for (FieldDoc doc : collectorDocs) {
                    if (!Float.isNaN(doc.score) && doc.score > maxScore) {
                        maxScore = doc.score;
                    }
                }
            }

            // Sort by score (descending) for SCORED_SORTED mode
            allDocs.sort((a, b) -> Float.compare(b.score, a.score));

            // Limit to numHits if we collected more
            if (allDocs.size() > numHits()) {
                allDocs = allDocs.subList(0, numHits());
            }

            // Create TopFieldDocs with actual scores and sorting
            FieldDoc[] fieldDocs = allDocs.toArray(new FieldDoc[0]);
            TotalHits totalHits = new TotalHits(allDocs.size(), TotalHits.Relation.EQUAL_TO);

            TopFieldDocs topDocs = new TopFieldDocs(totalHits, fieldDocs, sort.getSort());

            // Use actual maxScore if we found any, otherwise Float.NaN
            float finalMaxScore = (maxScore > Float.NEGATIVE_INFINITY) ? maxScore : Float.NaN;

            // Return a ReduceableSearchResult that can set the TopDocs
            return result -> {
                // CRITICAL: Set the TopDocs in the QuerySearchResult
                // For sorted results, we need to provide sort value formats
                // Since we're using score as the sort value, we'll use Float format
                DocValueFormat[] sortValueFormats = new DocValueFormat[] { DocValueFormat.RAW };
                result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, finalMaxScore), sortValueFormats);
            };
        }
    }

    /**
     * Collector that actually collects documents with scores for sorting
     */
    private class StreamingSortedCollector implements Collector {

        private final List<FieldDoc> collectedDocs = new ArrayList<>();

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE; // Need scores for sorting
        }

        @Override
        public LeafCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                private Scorable scorer;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // Scoring needed for SCORED_SORTED mode
                    this.scorer = scorer;
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (collectedDocs.size() < numHits()) {
                        // Get actual score from scorer
                        float score = this.scorer.score();
                        
                        // For sorted mode, we need to create FieldDoc with sort values
                        // For now, create a simple FieldDoc with the score as the sort value
                        // In a real implementation, we'd extract actual field values
                        Object[] sortValues = new Object[] { score };
                        FieldDoc fieldDoc = new FieldDoc(doc + context.docBase, score, sortValues);
                        collectedDocs.add(fieldDoc);
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

        public List<FieldDoc> getCollectedDocs() {
            return collectedDocs;
        }
    }
}
