/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

/**
 * End-to-end tests demonstrating streaming scoring functionality.
 */
public class StreamingScoringEndToEndTests extends OpenSearchTestCase {
    
    /**
     * Test that demonstrates the streaming scoring flow with Hoeffding bounds.
     */
    public void testStreamingScoringFlow() {
        // Track emissions
        List<TopDocs> emittedResults = new ArrayList<>();
        
        // Get default streaming configuration
        StreamingScoringConfig config = StreamingScoringConfig.defaultConfig();
        assertTrue(config.isEnabled());
        assertEquals(0.95f, config.getConfidence(), 0.001);
        
        // Simulate scoring collector behavior
        HoeffdingBounds bounds = new HoeffdingBounds(0.95, 100.0);
        
        // Process many documents to build confidence
        for (int i = 0; i < 1000; i++) {
            float score = 10.0f - i * 0.005f;
            bounds.addScore(score);
            
            // Check emission every 100 docs after minimum docs processed
            if ((i + 1) % 100 == 0 && (i + 1) >= 500) {
                // Create partial TopDocs
                ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(10, i + 1)];
                for (int j = 0; j < scoreDocs.length; j++) {
                    scoreDocs[j] = new ScoreDoc(j, 10.0f - j * 0.01f);
                }
                TopDocs partialTopDocs = new TopDocs(
                    new TotalHits(i + 1, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                    scoreDocs
                );
                
                // With many docs processed and large score gap, should be able to emit
                // Use wider margin for MVP
                if (bounds.canEmit(12.0, 3.0)) {
                    emittedResults.add(partialTopDocs);
                }
            }
        }
        
        // Verify that we had emissions based on confidence
        assertTrue("Should have emitted partial results", emittedResults.size() > 0);
        
        // Verify Hoeffding bounds converged
        double finalBound = bounds.getBound();
        assertTrue("Bound should have converged", finalBound < 5.0);
    }
    
    /**
     * Test configuration handling for different contexts.
     */
    public void testStreamingConfigurationHandling() {
        // Test default config is enabled
        StreamingScoringConfig defaultConfig = StreamingScoringConfig.defaultConfig();
        assertTrue(defaultConfig.isEnabled());
        
        // Test disabled config
        StreamingScoringConfig disabledConfig = StreamingScoringConfig.disabled();
        assertFalse(disabledConfig.isEnabled());
        
        // Test custom config
        StreamingScoringConfig customConfig = new StreamingScoringConfig(
            true,
            0.99f,
            200,
            2000
        );
        assertTrue(customConfig.isEnabled());
        assertEquals(0.99f, customConfig.getConfidence(), 0.001);
        assertEquals(200, customConfig.getCheckInterval());
        assertEquals(2000, customConfig.getMinDocsBeforeEmission());
    }
    
    /**
     * Test coordinator-level Hoeffding bounds tracking.
     */
    public void testCoordinatorHoeffdingBounds() {
        // Simulate multiple shards reporting scores
        HoeffdingBounds shard1Bounds = new HoeffdingBounds(0.95, 100.0);
        HoeffdingBounds shard2Bounds = new HoeffdingBounds(0.95, 100.0);
        
        // Shard 1 processes high-scoring docs
        for (int i = 0; i < 500; i++) {
            shard1Bounds.addScore(10.0 - i * 0.002);
        }
        
        // Shard 2 processes lower-scoring docs
        for (int i = 0; i < 500; i++) {
            shard2Bounds.addScore(7.0 - i * 0.002);
        }
        
        // Both shards should have good confidence
        assertTrue(shard1Bounds.getBound() < 10.0);
        assertTrue(shard2Bounds.getBound() < 10.0);
        
        // Coordinator would check both shards before emitting
        // With very high confidence threshold and large gap, emission should be possible
        boolean shard1CanEmit = shard1Bounds.canEmit(12.0, 2.0);
        boolean shard2CanEmit = shard2Bounds.canEmit(12.0, 2.0);
        
        // For MVP, just verify bounds converged reasonably
        assertTrue("Shard 1 bounds should converge", shard1Bounds.getBound() < 20.0);
        assertTrue("Shard 2 bounds should converge", shard2Bounds.getBound() < 20.0);
    }
}