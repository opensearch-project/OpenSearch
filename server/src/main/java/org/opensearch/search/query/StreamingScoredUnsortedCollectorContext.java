package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.Collection;

/**
 * Streaming collector context for SCORED_UNSORTED mode.
 * Collects documents with scores but no sorting for faster emission.
 */
public class StreamingScoredUnsortedCollectorContext extends TopDocsCollectorContext {
    
    public StreamingScoredUnsortedCollectorContext(String profilerName, int numHits) {
        super(profilerName, numHits);
    }
    
    @Override
    public Collector create(Collector in) throws IOException {
        // For SCORED_UNSORTED mode, use TopScoreDocCollectorManager for scored collection without sorting
        TopScoreDocCollectorManager manager = new TopScoreDocCollectorManager(numHits(), Integer.MAX_VALUE);
        return manager.newCollector();
    }
    
    @Override
    public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return new StreamingScoredUnsortedCollectorManager();
    }
    
    @Override
    public void postProcess(org.opensearch.search.query.QuerySearchResult result) throws IOException {
        // For single-threaded execution path, ensure TopDocs is set
        if (result.topDocs() == null) {
            // Create a basic TopDocs if none exists
            org.apache.lucene.search.ScoreDoc[] scoreDocs = new org.apache.lucene.search.ScoreDoc[0];
            org.apache.lucene.search.TotalHits totalHits = new org.apache.lucene.search.TotalHits(0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO);
            TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
            result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }
    
    /**
     * Collector manager for streaming scored unsorted collection
     */
    private class StreamingScoredUnsortedCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {
        
        @Override
        public Collector newCollector() throws IOException {
            // Use TopScoreDocCollectorManager for scored collection without sorting
            TopScoreDocCollectorManager manager = new TopScoreDocCollectorManager(numHits(), Integer.MAX_VALUE);
            return manager.newCollector();
        }
        
        @Override
        public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
            // For scored collection, we need to merge the results from all collectors
            // This is a simplified approach - in production we'd want more sophisticated merging
            
            // For now, we'll just return a placeholder result
            // In production, we'd want proper merging logic
            
            final TopDocs finalDocs = new TopDocs(
                new org.apache.lucene.search.TotalHits(0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                new org.apache.lucene.search.ScoreDoc[0]
            );
            
            // Return a ReduceableSearchResult that can set the TopDocs
            return result -> {
                // CRITICAL: Set the TopDocs in the QuerySearchResult
                result.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(finalDocs, Float.NaN), null);
            };
        }
    }
}
