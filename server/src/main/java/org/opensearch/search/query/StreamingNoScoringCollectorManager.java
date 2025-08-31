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
 * CollectorManager for streaming search without scoring.
 * Emits batches of document IDs as they are collected.
 */
public class StreamingNoScoringCollectorManager implements CollectorManager<StreamingNoScoringCollectorManager.StreamingNoScoringCollector, TopDocsAndMaxScore> {
    
    private final SearchContext searchContext;
    private final CircuitBreaker circuitBreaker;
    private final int batchSize;
    private final int maxDocs;
    
    public StreamingNoScoringCollectorManager(SearchContext searchContext, CircuitBreaker circuitBreaker) {
        this.searchContext = searchContext;
        this.circuitBreaker = circuitBreaker;
        // For NO_SCORING mode, emit immediately (batch size = 1) for fastest TTFB
        // This is the key optimization - emit from first document itself
        this.batchSize = 1; // Emit immediately for NO_SCORING mode
        this.maxDocs = searchContext.size();
    }
    
    @Override
    public StreamingNoScoringCollector newCollector() throws IOException {
        return new StreamingNoScoringCollector(searchContext, circuitBreaker, batchSize, maxDocs);
    }
    
    @Override
    public TopDocsAndMaxScore reduce(Collection<StreamingNoScoringCollector> collectors) throws IOException {
        // Combine all collected documents
        List<ScoreDoc> allDocs = new ArrayList<>();
        int totalHitsCount = 0;
        
        for (StreamingNoScoringCollector collector : collectors) {
            allDocs.addAll(collector.getCollectedDocs());
            totalHitsCount += collector.getTotalHits();
        }
        
        // Create final TopDocs with accurate total hits count
        TopDocs topDocs = new TopDocs(
            new TotalHits(totalHitsCount, Relation.EQUAL_TO),
            allDocs.toArray(new ScoreDoc[0])
        );
        
        return new TopDocsAndMaxScore(topDocs, Float.NaN);
    }
    
    /**
     * Streaming collector that emits batches during collection.
     */
    public static class StreamingNoScoringCollector implements Collector, StreamingCollectorContext {
        
        private final SearchContext context;
        private final CircuitBreaker circuitBreaker;
        private final int batchSize;
        private final int maxDocs;
        private final List<ScoreDoc> currentBatch;
        private final List<ScoreDoc> allCollectedDocs;
        private LeafReaderContext currentContext;
        private int collected = 0;
        private int totalHits = 0;
        
        public StreamingNoScoringCollector(SearchContext context, CircuitBreaker circuitBreaker, int batchSize, int maxDocs) {
            this.context = context;
            this.circuitBreaker = circuitBreaker;
            this.batchSize = batchSize;
            this.maxDocs = maxDocs;
            this.currentBatch = new ArrayList<>();
            this.allCollectedDocs = new ArrayList<>();
        }
        
        @Override
        public org.apache.lucene.search.ScoreMode scoreMode() {
            return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
        }
        
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            this.currentContext = context;
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    // No scoring needed
                }
                
                @Override
                public void collect(int doc) throws IOException {
                    StreamingNoScoringCollector.this.collect(doc);
                }
            };
        }
        
        public void collect(int doc) throws IOException {
            // Always count total hits for accurate totalHits reporting
            totalHits++;
            
            if (collected >= maxDocs) {
                return; // Already collected enough docs, but keep counting for totalHits
            }
            
            // Track memory usage
            if (circuitBreaker != null) {
                circuitBreaker.addEstimateBytesAndMaybeBreak(8, "streaming_search_doc");
            }
            
            // Add document to current batch
            currentBatch.add(new ScoreDoc(currentContext.docBase + doc, Float.NaN));
            allCollectedDocs.add(new ScoreDoc(currentContext.docBase + doc, Float.NaN));
            collected++;
            
            // Emit batch if full
            if (currentBatch.size() >= batchSize && context.isStreamingSearch()) {
                emitBatch(new ArrayList<>(currentBatch), false);
                currentBatch.clear();
            }
        }
        
        @Override
        public void emitBatch(List<ScoreDoc> docs, boolean isFinal) {
            if (docs.isEmpty()) return;
            
            try {
                // Create partial QuerySearchResult
                QuerySearchResult partial = new QuerySearchResult();
                partial.setDocIds(extractDocIds(docs));
                partial.setPartial(!isFinal);
                
                // Send through streaming channel
                if (context.getStreamChannelListener() != null) {
                    context.getStreamChannelListener().onStreamResponse(partial, isFinal);
                }
                
                // Release memory
                if (circuitBreaker != null) {
                    circuitBreaker.addWithoutBreaking(-docs.size() * 8L);
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
        
        private List<Integer> extractDocIds(List<ScoreDoc> docs) {
            List<Integer> docIds = new ArrayList<>();
            for (ScoreDoc doc : docs) {
                docIds.add(doc.doc);
            }
            return docIds;
        }
        
        public List<ScoreDoc> getCollectedDocs() {
            return allCollectedDocs;
        }
        
        public int getTotalHits() {
            return totalHits;
        }
        
        public void finish() {
            // Emit final batch if there are remaining docs
            if (!currentBatch.isEmpty()) {
                emitBatch(new ArrayList<>(currentBatch), true);
                currentBatch.clear();
            }
            
            // For tests where StreamChannelListener is null, ensure we still have results
            // The reduce() method will return all collected docs
        }
    }
}
