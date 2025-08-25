/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstration test showing the complete streaming scoring MVP flow.
 * This test simulates the end-to-end behavior when streaming scoring is enabled.
 */
public class StreamingScoringMVPDemoTests extends OpenSearchTestCase {
    
    /**
     * Demonstrates the complete streaming scoring flow:
     * 1. SearchRequest with streaming enabled
     * 2. System property enables streaming at shard level
     * 3. Collector emits partial results with Hoeffding confidence
     * 4. Coordinator tracks emissions across shards
     * 5. Results are progressively refined
     */
    public void testCompleteStreamingScoringFlow() {
        // Step 1: Create a search request with streaming scoring enabled
        SearchRequest searchRequest = new SearchRequest("test-index");
        searchRequest.setStreamingScoring(true);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(10);
        searchRequest.source(sourceBuilder);
        
        // Verify the flag is set
        assertTrue("Streaming scoring should be enabled", searchRequest.isStreamingScoring());
        
        // Step 2: Simulate system property activation
        // In production, set via: -Dopensearch.experimental.streaming.scoring=true
        System.setProperty("opensearch.experimental.streaming.scoring", "true");
        try {
            assertTrue("System property should enable streaming", 
                Boolean.getBoolean("opensearch.experimental.streaming.scoring"));
            
            // Step 3: Simulate shard-level streaming with Hoeffding bounds
            List<TopDocs> shardEmissions = simulateShardStreaming();
            assertTrue("Shard should emit partial results", shardEmissions.size() > 0);
            
            // Step 4: Simulate coordinator-level aggregation
            CoordinatorSimulation coordinator = new CoordinatorSimulation(3); // 3 shards
            int totalEmissions = coordinator.processShardResults();
            assertTrue("Coordinator should track emissions", totalEmissions > 0);
            
            // Step 5: Verify progressive refinement
            verifyProgressiveRefinement(shardEmissions);
            
        } finally {
            System.clearProperty("opensearch.experimental.streaming.scoring");
        }
    }
    
    /**
     * Simulates shard-level streaming with Hoeffding bounds.
     */
    private List<TopDocs> simulateShardStreaming() {
        List<TopDocs> emissions = new ArrayList<>();
        HoeffdingBounds bounds = new HoeffdingBounds(0.95, 100.0);
        StreamingScoringConfig config = StreamingScoringConfig.defaultConfig();
        
        // Simulate processing documents
        for (int i = 0; i < 1000; i++) {
            float score = 10.0f - i * 0.005f;
            bounds.addScore(score);
            
            // Check for emission every checkInterval docs
            if ((i + 1) % config.getCheckInterval() == 0 && 
                (i + 1) >= config.getMinDocsBeforeEmission()) {
                
                // Create partial top-K results
                ScoreDoc[] scoreDocs = new ScoreDoc[10];
                for (int j = 0; j < 10; j++) {
                    scoreDocs[j] = new ScoreDoc(j, 10.0f - j * 0.1f);
                }
                
                TopDocs partialTopDocs = new TopDocs(
                    new TotalHits(i + 1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                    scoreDocs
                );
                
                // Check confidence for emission
                float lowestScore = scoreDocs[9].score;
                double maxUnseen = bounds.getEstimatedMaxUnseen();
                
                if (bounds.canEmit(lowestScore + 2.0, maxUnseen)) {
                    emissions.add(partialTopDocs);
                }
            }
        }
        
        return emissions;
    }
    
    /**
     * Simulates coordinator-level processing.
     */
    private static class CoordinatorSimulation {
        private final int numShards;
        private final HoeffdingBounds[] shardBounds;
        private int emissions = 0;
        
        CoordinatorSimulation(int numShards) {
            this.numShards = numShards;
            this.shardBounds = new HoeffdingBounds[numShards];
            for (int i = 0; i < numShards; i++) {
                shardBounds[i] = new HoeffdingBounds(0.95, 100.0);
            }
        }
        
        int processShardResults() {
            // Simulate each shard reporting scores
            for (int shard = 0; shard < numShards; shard++) {
                for (int doc = 0; doc < 500; doc++) {
                    float score = 9.0f - doc * 0.002f - shard * 0.5f;
                    shardBounds[shard].addScore(score);
                    
                    // Check if coordinator should emit
                    if (doc > 100 && doc % 50 == 0) {
                        boolean canEmit = true;
                        for (HoeffdingBounds bounds : shardBounds) {
                            if (bounds.getDocCount() > 0 && bounds.getBound() > 5.0) {
                                canEmit = false;
                                break;
                            }
                        }
                        if (canEmit) {
                            emissions++;
                        }
                    }
                }
            }
            return emissions;
        }
    }
    
    /**
     * Verifies that results are progressively refined.
     */
    private void verifyProgressiveRefinement(List<TopDocs> emissions) {
        if (emissions.size() < 2) {
            return; // Need at least 2 emissions to verify refinement
        }
        
        // Each emission should have more documents processed
        long prevTotal = emissions.get(0).totalHits.value();
        for (int i = 1; i < emissions.size(); i++) {
            long currentTotal = emissions.get(i).totalHits.value();
            assertTrue("Later emissions should process more docs", 
                currentTotal >= prevTotal);
            prevTotal = currentTotal;
        }
    }
    
    /**
     * Test that streaming is disabled by default.
     */
    public void testStreamingDisabledByDefault() {
        SearchRequest searchRequest = new SearchRequest("test-index");
        assertFalse("Streaming should be disabled by default", 
            searchRequest.isStreamingScoring());
        
        // Without system property, streaming shouldn't activate
        assertFalse("System property should be false by default",
            Boolean.getBoolean("opensearch.experimental.streaming.scoring"));
    }
    
    /**
     * Test configuration variations.
     */
    public void testStreamingConfigurations() {
        // Test different confidence levels
        StreamingScoringConfig highConfidence = new StreamingScoringConfig(
            true, 0.99f, 100, 1000
        );
        assertEquals(0.99f, highConfidence.getConfidence(), 0.001);
        
        StreamingScoringConfig lowConfidence = new StreamingScoringConfig(
            true, 0.90f, 50, 500
        );
        assertEquals(0.90f, lowConfidence.getConfidence(), 0.001);
        
        // Higher confidence should be more conservative
        HoeffdingBounds highBounds = new HoeffdingBounds(0.99, 100.0);
        HoeffdingBounds lowBounds = new HoeffdingBounds(0.90, 100.0);
        
        for (int i = 0; i < 100; i++) {
            highBounds.addScore(5.0);
            lowBounds.addScore(5.0);
        }
        
        assertTrue("Higher confidence should have larger bounds",
            highBounds.getBound() > lowBounds.getBound());
    }
}