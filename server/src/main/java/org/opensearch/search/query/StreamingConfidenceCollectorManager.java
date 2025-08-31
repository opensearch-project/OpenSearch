package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SimpleCollector;
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
 * CollectorManager for streaming search with confidence-based emission.
 * Emits batches when confidence thresholds are met.
 */
public class StreamingConfidenceCollectorManager implements CollectorManager<StreamingConfidenceCollectorManager.StreamingConfidenceCollector, TopDocsAndMaxScore> {
    
    private final SearchContext searchContext;
    private final CircuitBreaker circuitBreaker;
    private final int batchSize;
    private final int maxDocs;
    
    public StreamingConfidenceCollectorManager(SearchContext searchContext, CircuitBreaker circuitBreaker) {
        this.searchContext = searchContext;
        this.circuitBreaker = circuitBreaker;
        this.batchSize = 100; // Default batch size
        this.maxDocs = searchContext.size();
    }
    
    @Override
    public StreamingConfidenceCollector newCollector() throws IOException {
        return new StreamingConfidenceCollector(searchContext, circuitBreaker, batchSize, maxDocs);
    }
    
    @Override
    public TopDocsAndMaxScore reduce(Collection<StreamingConfidenceCollector> collectors) throws IOException {
        // Combine all collected documents
        List<ScoreDoc> allDocs = new ArrayList<>();
        float maxScore = Float.NEGATIVE_INFINITY;
        
        for (StreamingConfidenceCollector collector : collectors) {
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
     * Streaming collector that emits batches based on confidence thresholds.
     */
    public static class StreamingConfidenceCollector implements Collector, StreamingCollectorContext {
        
        private final SearchContext context;
        private final CircuitBreaker circuitBreaker;
        private final int batchSize;
        private final int maxDocs;
        private final List<ScoreDoc> currentBatch;
        private final List<ScoreDoc> allCollectedDocs;
        private LeafReaderContext currentContext;
        private Scorable scorer;
        private int collected = 0;
        private float maxScore = Float.NEGATIVE_INFINITY;
        private float lastEmittedScore = Float.NEGATIVE_INFINITY;
        
        public StreamingConfidenceCollector(SearchContext context, CircuitBreaker circuitBreaker, int batchSize, int maxDocs) {
            this.context = context;
            this.circuitBreaker = circuitBreaker;
            this.batchSize = batchSize;
            this.maxDocs = maxDocs;
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
                    StreamingConfidenceCollector.this.scorer = scorer;
                }
                
                @Override
                public void collect(int doc) throws IOException {
                    StreamingConfidenceCollector.this.collect(doc);
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
                circuitBreaker.addEstimateBytesAndMaybeBreak(16, "streaming_search_confidence_doc");
            }
            
            // Get score
            float score = scorer.score();
            maxScore = Math.max(maxScore, score);
            
            // Add document to current batch
            ScoreDoc scoreDoc = new ScoreDoc(currentContext.docBase + doc, score);
            currentBatch.add(scoreDoc);
            allCollectedDocs.add(scoreDoc);
            collected++;
            
            // Check confidence-based emission conditions
            boolean shouldEmit = false;
            
            // Emit if batch is full
            if (currentBatch.size() >= batchSize) {
                shouldEmit = true;
            }
            
            // Emit if score has dropped significantly (confidence threshold)
            if (score < lastEmittedScore * 0.8f && currentBatch.size() >= batchSize / 2) {
                shouldEmit = true;
            }
            
            // Emit if we have enough docs and context is streaming
            if (shouldEmit && context.isStreamingSearch()) {
                emitConfidenceBatch(new ArrayList<>(currentBatch), false);
                currentBatch.clear();
                lastEmittedScore = score;
            }
        }
        
        @Override
        public void emitBatch(List<ScoreDoc> docs, boolean isFinal) {
            emitConfidenceBatch(docs, isFinal);
        }
        
        private void emitConfidenceBatch(List<ScoreDoc> docs, boolean isFinal) {
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
                emitConfidenceBatch(new ArrayList<>(currentBatch), true);
                currentBatch.clear();
            }
        }
    }
}
