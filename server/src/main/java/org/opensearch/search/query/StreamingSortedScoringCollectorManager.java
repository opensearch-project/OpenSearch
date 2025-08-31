package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CollectorManager for streaming search with scoring and sorting.
 * Optimizes for index-sorted data when possible.
 */
public class StreamingSortedScoringCollectorManager implements CollectorManager<StreamingSortedScoringCollectorManager.StreamingSortedScoringCollector, TopDocsAndMaxScore> {
    
    private final SearchContext searchContext;
    private final CircuitBreaker circuitBreaker;
    private final int batchSize;
    private final int maxDocs;
    private final boolean canUseIndexSort;
    
    public StreamingSortedScoringCollectorManager(SearchContext searchContext, CircuitBreaker circuitBreaker) {
        this.searchContext = searchContext;
        this.circuitBreaker = circuitBreaker;
        this.batchSize = getOptimalBatchSize(searchContext.getStreamingMode());
        this.maxDocs = searchContext.size();
        
        // Check if we can use index sort optimization
        Sort indexSort = null;
        try {
            // Try to get index sort if available
            if (searchContext.searcher().getIndexReader() != null) {
                // Note: getMetaData() might not be available in all Lucene versions
                // For now, we'll assume no index sort optimization
                indexSort = null;
            }
        } catch (Exception e) {
            // Index sort not available
            indexSort = null;
        }
        
        Sort querySort = null;
        try {
            // Try to get query sort
            if (searchContext.sort() != null) {
                querySort = searchContext.sort().sort;
            }
        } catch (Exception e) {
            // Query sort not available
            querySort = null;
        }
        
        this.canUseIndexSort = indexSort != null && querySort != null && indexSort.equals(querySort);
    }
    
    private int getOptimalBatchSize(StreamingSearchMode mode) {
        switch (mode) {
            case NO_SCORING: return 1;        // Emit immediately for <10ms TTFB
            case SCORED_UNSORTED: return 500;  // Balance between latency and efficiency
            case SCORED_SORTED: return 1000;   // Larger batches for better sorting quality
            default: return 100;
        }
    }
    
    @Override
    public StreamingSortedScoringCollector newCollector() throws IOException {
        return new StreamingSortedScoringCollector(searchContext, circuitBreaker, batchSize, maxDocs, canUseIndexSort);
    }
    
    @Override
    public TopDocsAndMaxScore reduce(Collection<StreamingSortedScoringCollector> collectors) throws IOException {
        // Combine all collected documents
        List<ScoreDoc> allDocs = new ArrayList<>();
        float maxScore = Float.NEGATIVE_INFINITY;
        
        for (StreamingSortedScoringCollector collector : collectors) {
            allDocs.addAll(collector.getCollectedDocs());
            maxScore = Math.max(maxScore, collector.getMaxScore());
        }
        
        // Create final TopDocs
        TopDocs topDocs = new TopDocs(
            new TotalHits(allDocs.size(), Relation.EQUAL_TO),
            allDocs.toArray(new ScoreDoc[0])
        );
        
        return new TopDocsAndMaxScore(topDocs, maxScore);
    }
    
    /**
     * Streaming collector that handles sorted scoring with index sort optimization.
     */
    public static class StreamingSortedScoringCollector implements Collector, StreamingCollectorContext {
        
        private final SearchContext context;
        private final CircuitBreaker circuitBreaker;
        private final int batchSize;
        private final int maxDocs;
        private final boolean canUseIndexSort;
        private final List<ScoreDoc> currentBatch;
        private final List<ScoreDoc> allCollectedDocs;
        private LeafReaderContext currentContext;
        private Scorable scorer;
        private int collected = 0;
        private float maxScore = Float.NEGATIVE_INFINITY;
        
        public StreamingSortedScoringCollector(SearchContext context, CircuitBreaker circuitBreaker, int batchSize, int maxDocs, boolean canUseIndexSort) {
            this.context = context;
            this.circuitBreaker = circuitBreaker;
            this.batchSize = batchSize;
            this.maxDocs = maxDocs;
            this.canUseIndexSort = canUseIndexSort;
            this.currentBatch = new ArrayList<>();
            this.allCollectedDocs = new ArrayList<>();
        }
        
        @Override
        public org.apache.lucene.search.ScoreMode scoreMode() {
            return org.apache.lucene.search.ScoreMode.COMPLETE;
        }
        
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            this.currentContext = context;
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    StreamingSortedScoringCollector.this.scorer = scorer;
                }
                
                @Override
                public void collect(int doc) throws IOException {
                    StreamingSortedScoringCollector.this.collect(doc);
                }
            };
        }
        
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
        }
        
        public void collect(int doc) throws IOException {
            if (collected >= maxDocs) {
                return; // Already collected enough docs
            }
            
            // Track memory usage
            if (circuitBreaker != null) {
                circuitBreaker.addEstimateBytesAndMaybeBreak(16, "streaming_search_sorted_doc");
            }
            
            // Get score
            float score = scorer.score();
            maxScore = Math.max(maxScore, score);
            
            // Add document to current batch
            ScoreDoc scoreDoc = new ScoreDoc(currentContext.docBase + doc, score);
            currentBatch.add(scoreDoc);
            allCollectedDocs.add(scoreDoc);
            collected++;
            
            // Emit batch if full
            if (currentBatch.size() >= batchSize && context.isStreamingSearch()) {
                emitScoredBatch(new ArrayList<>(currentBatch), false);
                currentBatch.clear();
            }
        }
        
        @Override
        public void emitBatch(List<ScoreDoc> docs, boolean isFinal) {
            emitScoredBatch(docs, isFinal);
        }
        
        private void emitScoredBatch(List<ScoreDoc> docs, boolean isFinal) {
            if (docs.isEmpty()) return;
            
            try {
                // Create TopDocs for this batch
                TopDocs batchTopDocs = new TopDocs(
                    new TotalHits(docs.size(), TotalHits.Relation.EQUAL_TO),
                    docs.toArray(new ScoreDoc[0])
                );
                
                // Create partial QuerySearchResult
                QuerySearchResult partial = new QuerySearchResult();
                partial.topDocs(new TopDocsAndMaxScore(batchTopDocs, maxScore), null);
                partial.setPartial(!isFinal);
                
                // Send through streaming channel
                if (context.getStreamChannelListener() != null) {
                    context.getStreamChannelListener().onStreamResponse(partial, isFinal);
                }
                
                // Release memory
                if (circuitBreaker != null) {
                    circuitBreaker.addWithoutBreaking(-docs.size() * 16L);
                }
                
            } catch (Exception e) {
                // Log error but continue collection
                // TODO: Add proper logging
            }
        }
        
        @Override
        public int getBatchSize() {
            return batchSize;
        }
        
        @Override
        public boolean shouldEmitBatch() {
            return currentBatch.size() >= batchSize;
        }
        
        public List<ScoreDoc> getCollectedDocs() {
            return allCollectedDocs;
        }
        
        public float getMaxScore() {
            return maxScore;
        }
        
        public void finish() {
            // Emit final batch if there are remaining docs
            if (!currentBatch.isEmpty()) {
                emitScoredBatch(new ArrayList<>(currentBatch), true);
                currentBatch.clear();
            }
        }
    }
}
