/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SearchResponseBuilderTests extends OpenSearchTestCase {

    public void testBuildWithNoResults() throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 42L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(42L, response.getTook().millis());
        assertNull(response.getAggregations());
        assertEquals(0, response.getTotalShards());
        assertEquals(0, response.getSuccessfulShards());
    }

    public void testBuildWithEmptyRequest() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 100L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(100L, response.getTook().millis());
        assertNull(response.getAggregations());
    }

    public void testBuildWithNullSource() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 50L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertNull(response.getAggregations());
        assertEquals(0, response.getTotalShards());
    }

    public void testBuildWithAggregationsButNoResults() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.avg("avg_price").field("price"));
        request.source(source);
        
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 75L);

        assertNotNull(response);
        assertEquals(75L, response.getTook().millis());
        assertNull(response.getAggregations());
    }

    public void testShardCountsWithNoAggregations() throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 10L);

        assertEquals(0, response.getTotalShards());
        assertEquals(0, response.getSuccessfulShards());
        assertEquals(0, response.getSkippedShards());
        assertEquals(0, response.getFailedShards());
    }

    public void testTimingPreserved() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response1 = SearchResponseBuilder.build(List.of(), request, registry, 0L);
        assertEquals(0L, response1.getTook().millis());

        SearchResponse response2 = SearchResponseBuilder.build(List.of(), request, registry, 999L);
        assertEquals(999L, response2.getTook().millis());
    }

    public void testEmptyHitsAlwaysReturned() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 10L);

        assertNotNull(response.getHits());
        assertEquals(0, response.getHits().getHits().length);
        assertNotNull(response.getHits().getTotalHits());
    }
}
