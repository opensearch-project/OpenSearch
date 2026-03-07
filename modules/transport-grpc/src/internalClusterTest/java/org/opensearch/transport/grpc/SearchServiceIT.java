/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.MinAggregation;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.ManagedChannel;

/**
 * Integration tests for the SearchService gRPC service.
 */
public class SearchServiceIT extends GrpcTransportBaseIT {

    /**
     * Tests the search operation via gRPC.
     */
    public void testSearchServiceSearch() throws Exception {
        // Create a test index
        String indexName = "test-search-index";
        createTestIndex(indexName);

        // Add a document to the index
        indexTestDocument(indexName, "1", DEFAULT_DOCUMENT_SOURCE);

        // Create a gRPC client
        try (NettyGrpcClient client = createGrpcClient()) {
            // Create a SearchService stub
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Create a search request
            SearchRequestBody requestBody = SearchRequestBody.newBuilder().setFrom(0).setSize(10).build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(indexName)
                .setSearchRequestBody(requestBody)
                .setQ("field1:value1")
                .build();

            // Execute the search request
            SearchResponse searchResponse = searchStub.search(searchRequest);

            // Verify the response
            assertNotNull("Search response should not be null", searchResponse);
            assertTrue("Search response should have hits", searchResponse.getHits().getTotal().getTotalHits().getValue() > 0);
            assertEquals("Search response should have one hit", 1, searchResponse.getHits().getHitsCount());
            assertEquals("Hit should have correct ID", "1", searchResponse.getHits().getHits(0).getXId());
        }
    }

    /**
     * Tests min aggregation via gRPC.
     */
    public void testMinAggregation() throws Exception {
        // Create a test index
        String indexName = "test-min-agg";
        createTestIndex(indexName);

        // Index documents with different price values
        indexTestDocument(indexName, "1", "{\"price\": 10.5}");
        indexTestDocument(indexName, "2", "{\"price\": 25.0}");
        indexTestDocument(indexName, "3", "{\"price\": 5.2}");

        // Create a gRPC client
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Build min aggregation request
            MinAggregation minAgg = MinAggregation.newBuilder().setField("price").build();

            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("min_price", AggregationContainer.newBuilder().setMin(minAgg).build())
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder().addIndex(indexName).setSearchRequestBody(requestBody).build();

            // Execute search
            SearchResponse response = searchStub.search(searchRequest);

            // Verify min aggregation result
            assertNotNull("Search response should not be null", response);
            assertTrue("Should have aggregations", response.getAggregationsCount() > 0);
            Aggregate minResult = response.getAggregationsMap().get("min_price");
            assertNotNull("Min aggregation should exist", minResult);
            assertTrue("Should have value", minResult.hasValue());
            assertEquals("Min value should be 5.2", 5.2, minResult.getValue().getDouble(), 0.001);
        }
    }

    /**
     * Tests max aggregation via gRPC.
     */
    public void testMaxAggregation() throws Exception {
        // Create a test index
        String indexName = "test-max-agg";
        createTestIndex(indexName);

        // Index documents with different price values
        indexTestDocument(indexName, "1", "{\"price\": 10.5}");
        indexTestDocument(indexName, "2", "{\"price\": 25.0}");
        indexTestDocument(indexName, "3", "{\"price\": 5.2}");

        // Create a gRPC client
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Build max aggregation request
            MaxAggregation maxAgg = MaxAggregation.newBuilder().setField("price").build();

            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("max_price", AggregationContainer.newBuilder().setMax(maxAgg).build())
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder().addIndex(indexName).setSearchRequestBody(requestBody).build();

            // Execute search
            SearchResponse response = searchStub.search(searchRequest);

            // Verify max aggregation result
            assertNotNull("Search response should not be null", response);
            assertTrue("Should have aggregations", response.getAggregationsCount() > 0);
            Aggregate maxResult = response.getAggregationsMap().get("max_price");
            assertNotNull("Max aggregation should exist", maxResult);
            assertTrue("Should have value", maxResult.hasValue());
            assertEquals("Max value should be 25.0", 25.0, maxResult.getValue().getDouble(), 0.001);
        }
    }
}
