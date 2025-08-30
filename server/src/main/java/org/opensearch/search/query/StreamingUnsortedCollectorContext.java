package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Streaming collector context for NO_SCORING mode.
 * Collects documents in encounter order without scoring or sorting.
 */
public class StreamingUnsortedCollectorContext extends TopDocsCollectorContext {
    
    private final AtomicInteger totalCollected = new AtomicInteger(0);
    
    public StreamingUnsortedCollectorContext(String profilerName, int numHits) {
        super(profilerName, numHits);
    }
    
    @Override
    public Collector create(Collector in) throws IOException {
        // For NO_SCORING mode, we don't need scoring, just collect docs in order
        return new StreamingUnsortedCollector();
    }
    
    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingUnsortedCollectorManager();
    }
    
    @Override
    public void postProcess(org.opensearch.search.query.QuerySearchResult result) throws IOException {
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
            
            // Combine all collected documents from all collectors
            for (StreamingUnsortedCollector collector : collectors) {
                allDocs.addAll(collector.getCollectedDocs());
            }
            
            // Limit to numHits if we collected more
            if (allDocs.size() > numHits()) {
                allDocs = allDocs.subList(0, numHits());
            }
            
            // Create TopDocs with Float.NaN scores since NO_SCORING
            ScoreDoc[] scoreDocs = allDocs.toArray(new ScoreDoc[0]);
            TotalHits totalHits = new TotalHits(allDocs.size(), TotalHits.Relation.EQUAL_TO);
            
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
            
            // Return a ReduceableSearchResult that can set the TopDocs
            return result -> {
                // CRITICAL: Set the TopDocs in the QuerySearchResult
                result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
            };
        }
    }
    
    /**
     * Collector that actually collects documents in encounter order
     */
    private class StreamingUnsortedCollector implements Collector {
        
        private final List<ScoreDoc> collectedDocs = new ArrayList<>();
        
        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
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
                    if (collectedDocs.size() < numHits()) {
                        // Collect document with Float.NaN score since NO_SCORING
                        ScoreDoc scoreDoc = new ScoreDoc(doc + context.docBase, Float.NaN);
                        collectedDocs.add(scoreDoc);
                        totalCollected.incrementAndGet();
                    }
                }
            };
        }
        
        public List<ScoreDoc> getCollectedDocs() {
            return collectedDocs;
        }
    }
}
