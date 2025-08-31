package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

/**
 * Simple TopDocsCollectorContext for streaming search.
 * This provides basic streaming functionality without complex generic type handling.
 */
public class StreamingTopDocsCollectorContext extends TopDocsCollectorContext {
    
    private final StreamingSearchMode mode;
    private final SearchContext searchContext;
    
    public StreamingTopDocsCollectorContext(StreamingSearchMode mode, SearchContext searchContext) {
        super("streaming_" + mode.name().toLowerCase(), searchContext.size());
        this.mode = mode;
        this.searchContext = searchContext;
    }
    
    @Override
    public Collector create(Collector in) throws IOException {
        // For now, return a simple collector that just counts documents
        // This is a placeholder - the real streaming logic will be implemented later
        return new SimpleStreamingCollector(searchContext, mode);
    }
    
    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        // Return a simple manager that creates streaming collectors
        return new CollectorManager<SimpleStreamingCollector, ReduceableSearchResult>() {
            @Override
            public SimpleStreamingCollector newCollector() throws IOException {
                return new SimpleStreamingCollector(searchContext, mode);
            }
            
            @Override
            public ReduceableSearchResult reduce(Collection<SimpleStreamingCollector> collectors) throws IOException {
                // Finish all collectors to emit final batches
                for (SimpleStreamingCollector collector : collectors) {
                    collector.finish();
                }
                
                // Return a result that combines all collected documents
                return new ReduceableSearchResult() {
                    @Override
                    public void reduce(QuerySearchResult result) throws IOException {
                        // Combine all collected documents from all collectors
                        List<ScoreDoc> allDocs = new ArrayList<>();
                        for (SimpleStreamingCollector collector : collectors) {
                            allDocs.addAll(collector.getCollectedDocs());
                        }
                        
                        if (allDocs.isEmpty()) {
                            // Create empty TopDocs if no documents collected
                            TopDocs emptyTopDocs = new TopDocs(
                                new TotalHits(0, Relation.EQUAL_TO),
                                new ScoreDoc[0]
                            );
                            result.topDocs(new TopDocsAndMaxScore(emptyTopDocs, Float.NaN), null);
                        } else {
                            // Create TopDocs with all collected documents
                            TopDocs combinedTopDocs = new TopDocs(
                                new TotalHits(allDocs.size(), Relation.EQUAL_TO),
                                allDocs.toArray(new ScoreDoc[0])
                            );
                            
                            // Calculate max score across all collectors
                            float maxScore = Float.NEGATIVE_INFINITY;
                            for (ScoreDoc doc : allDocs) {
                                if (!Float.isNaN(doc.score)) {
                                    maxScore = Math.max(maxScore, doc.score);
                                }
                            }
                            
                            result.topDocs(new TopDocsAndMaxScore(combinedTopDocs, maxScore), null);
                        }
                        
                        // Mark as not partial for final result
                        result.setPartial(false);
                    }
                };
            }
        };
    }
    
    protected ReduceableSearchResult reduceWith(TopDocs topDocs, float maxScore, Integer terminatedAfter) throws IOException {
        // For streaming, this method is not used
        return new ReduceableSearchResult() {
            @Override
            public void reduce(QuerySearchResult result) throws IOException {
                result.topDocs(new TopDocsAndMaxScore(topDocs, maxScore), null);
            }
        };
    }
    
    /**
     * Simple streaming collector that implements batch emission.
     * This is the core streaming implementation.
     */
    private static class SimpleStreamingCollector implements Collector {
        private final SearchContext context;
        private final StreamingSearchMode mode;
        private final int batchSize;
        private final List<ScoreDoc> currentBatch;
        private final List<ScoreDoc> allCollectedDocs;
        private LeafReaderContext currentContext;
        private Scorable scorer;
        private int docCount = 0;
        private float maxScore = Float.NEGATIVE_INFINITY;
        
        public SimpleStreamingCollector(SearchContext context, StreamingSearchMode mode) {
            this.context = context;
            this.mode = mode;
            this.batchSize = 100; // Default batch size
            this.currentBatch = new ArrayList<>();
            this.allCollectedDocs = new ArrayList<>();
        }
        
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            this.currentContext = context;
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    SimpleStreamingCollector.this.scorer = scorer;
                }
                
                @Override
                public void collect(int doc) throws IOException {
                    docCount++;
                    
                    // Get score if needed
                    float score = Float.NaN;
                    if (mode != StreamingSearchMode.NO_SCORING && scorer != null) {
                        try {
                            score = scorer.score();
                            maxScore = Math.max(maxScore, score);
                        } catch (Exception e) {
                            // Score not available, use NaN
                        }
                    }
                    
                    // Add document to current batch
                    ScoreDoc scoreDoc = new ScoreDoc(currentContext.docBase + doc, score);
                    currentBatch.add(scoreDoc);
                    allCollectedDocs.add(scoreDoc);
                    
                    // Emit batch if full - check streaming mode safely
                    if (currentBatch.size() >= batchSize) {
                        // Only emit if streaming is enabled and we're in a streaming context
                        if (isStreamingEnabled()) {
                            emitBatch(new ArrayList<>(currentBatch), false);
                            currentBatch.clear();
                        }
                    }
                }
            };
        }
        
        @Override
        public ScoreMode scoreMode() {
            return mode == StreamingSearchMode.NO_SCORING ? 
                ScoreMode.COMPLETE_NO_SCORES : ScoreMode.COMPLETE;
        }
        
        /**
         * Emit a batch of documents through the streaming channel.
         * Handles both test and production environments gracefully.
         */
        private void emitBatch(List<ScoreDoc> docs, boolean isFinal) {
            if (docs.isEmpty()) return;
            
            try {
                // Check if we have a streaming channel listener
                if (context.getStreamChannelListener() != null) {
                    // Production environment - emit through streaming channel
                    createAndEmitStreamingBatch(docs, isFinal);
                } else {
                    // Test environment - just log for now
                    System.out.println("Streaming collector: Would emit batch of " + docs.size() + " docs, isFinal: " + isFinal);
                }
            } catch (Exception e) {
                // Log error but continue collection - don't break the search
                System.err.println("Error emitting streaming batch: " + e.getMessage());
            }
        }
        
        /**
         * Create and emit a streaming batch through the streaming channel.
         */
        private void createAndEmitStreamingBatch(List<ScoreDoc> docs, boolean isFinal) throws IOException {
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
            context.getStreamChannelListener().onStreamResponse(partial, isFinal);
        }
        
        /**
         * Get all collected documents for final result.
         */
        public List<ScoreDoc> getCollectedDocs() {
            return allCollectedDocs;
        }
        
        /**
         * Check if streaming is enabled safely.
         */
        private boolean isStreamingEnabled() {
            try {
                return context.isStreamingSearch() && context.getStreamingMode() != null;
            } catch (Exception e) {
                // If we can't determine streaming status, assume it's not enabled
                return false;
            }
        }
        
        /**
         * Finish collection and emit final batch.
         */
        public void finish() {
            // Emit final batch if there are remaining docs
            if (!currentBatch.isEmpty()) {
                emitBatch(new ArrayList<>(currentBatch), true);
                currentBatch.clear();
            }
        }
    }
}
