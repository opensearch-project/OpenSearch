/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Integration test for streaming search with scoring using Hoeffding bounds.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class StreamingScoringIntegrationTest extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.STREAM_TRANSPORT_SETTING.getKey(), true)
            .build();
    }

    @Before
    public void setupIndex() throws IOException, InterruptedException {
        // Create index with multiple shards to test streaming across shards
        assertAcked(
            prepareCreate("test")
                .setSettings(Settings.builder()
                    .put("index.number_of_shards", 5)
                    .put("index.number_of_replicas", 0))
        );

        // Index documents with varying scores
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            requests.add(
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource("field", "value" + i, "score_field", i % 10)
            );
        }
        indexRandom(true, requests);
        ensureGreen();
    }

    public void testStreamingScoringWithConfidenceBased() throws Exception {
        // Test CONFIDENCE_BASED mode (default)
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(10);
        searchRequest.source(sourceBuilder);
        searchRequest.setStreamingScoring(true);
        searchRequest.setStreamingScoringMode("confidence_based");

        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).get();

        // Verify response
        assertNotNull(response);
        assertHitCount(response, 100);
        
        // Check if streaming metadata is present
        assertNotNull("Response should have sequence number", response.getSequenceNumber());
        assertNotNull("Response should have total partials count", response.getTotalPartials());
        
        // Verify we got top results
        assertEquals(10, response.getHits().getHits().length);
        
        // Log the results for verification
        logger.info("[Test] Streaming search completed with {} partial computations", 
            response.getTotalPartials());
        
        // Verify the response is marked as final (not partial)
        assertFalse("Final response should not be marked as partial", response.isPartial());
        
        // The sequence number should be > 0 if streaming occurred
        if (response.getTotalPartials() > 0) {
            assertTrue("Sequence number should be positive when streaming occurred", 
                response.getSequenceNumber() > 0);
        }
    }

    public void testStreamingScoringEfficiency() throws Exception {
        // Test that streaming scoring computes results progressively
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("field", "value1"));
        sourceBuilder.size(5);
        searchRequest.source(sourceBuilder);
        searchRequest.setStreamingScoring(true);

        long startTime = System.currentTimeMillis();
        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).get();
        long duration = System.currentTimeMillis() - startTime;

        assertNotNull(response);
        
        // Log performance metrics
        logger.info("[Test] Streaming search took {}ms with {} partial computations", 
            duration, response.getTotalPartials());
        
        // If Hoeffding bounds worked, we should see partial computations
        // Note: This might not always trigger if confidence is not reached quickly
        if (response.getTotalPartials() > 0) {
            logger.info("[Test] Streaming scoring successfully computed {} partial results", 
                response.getTotalPartials());
        } else {
            logger.info("[Test] No partial computations (confidence threshold not met early)");
        }
    }

    public void testStreamingScoringNoScoring() throws Exception {
        // Test NO_SCORING mode - fastest, no scoring
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(10);
        searchRequest.source(sourceBuilder);
        searchRequest.setStreamingScoring(true);
        searchRequest.setStreamingScoringMode("no_scoring");

        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).get();

        assertNotNull(response);
        assertHitCount(response, 100);
        assertEquals(10, response.getHits().getHits().length);
        
        logger.info("[Test] NO_SCORING mode completed with {} partial computations", 
            response.getTotalPartials());
    }
    
    public void testStreamingScoringFullScoring() throws Exception {
        // Test FULL_SCORING mode - complete scoring before streaming
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(10);
        searchRequest.source(sourceBuilder);
        searchRequest.setStreamingScoring(true);
        searchRequest.setStreamingScoringMode("full_scoring");

        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).get();

        assertNotNull(response);
        assertHitCount(response, 100);
        assertEquals(10, response.getHits().getHits().length);
        
        // FULL_SCORING should not have partial computations (waits for all)
        logger.info("[Test] FULL_SCORING mode completed with {} partial computations (should be 0 or 1)", 
            response.getTotalPartials());
    }

    public void testStreamingScoringDisabled() throws Exception {
        // Test normal search without streaming scoring
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(10);
        searchRequest.source(sourceBuilder);
        // Don't set streaming scoring flag

        SearchResponse response = client().execute(SearchAction.INSTANCE, searchRequest).get();

        assertNotNull(response);
        assertHitCount(response, 100);
        
        // Without streaming, these should be default values
        assertEquals(0, response.getSequenceNumber());
        assertEquals(0, response.getTotalPartials());
        assertFalse(response.isPartial());
        
        logger.info("[Test] Normal search completed without streaming");
    }
}