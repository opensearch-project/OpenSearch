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
import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationFields;
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
            assertTrue("Should have min", minResult.hasMin());
            assertTrue("Should have value", minResult.getMin().hasValue());
            assertEquals("Min value should be 5.2", 5.2, minResult.getMin().getValue().getDouble(), 0.001);
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
            assertTrue("Should have max", maxResult.hasMax());
            assertTrue("Should have value", maxResult.getMax().hasValue());
            assertEquals("Max value should be 25.0", 25.0, maxResult.getMax().getValue().getDouble(), 0.001);
        }
    }

    /**
     * Tests terms aggregation on a keyword field via gRPC.
     */
    public void testTermsAggregation() throws Exception {
        String indexName = "test-terms-agg-it";
        createTestIndex(indexName);

        // Index documents with two distinct status values
        indexTestDocument(indexName, "1", "{\"status\": \"active\"}");
        indexTestDocument(indexName, "2", "{\"status\": \"active\"}");
        indexTestDocument(indexName, "3", "{\"status\": \"inactive\"}");

        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Build terms aggregation request on the status keyword field
            TermsAggregation termsAgg = TermsAggregation.newBuilder()
                .setTerms(TermsAggregationFields.newBuilder().setField("status").build())
                .build();

            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("status_terms", AggregationContainer.newBuilder().setTermsAggregation(termsAgg).build())
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder().addIndex(indexName).setSearchRequestBody(requestBody).build();

            SearchResponse response = searchStub.search(searchRequest);

            // Verify response structure
            assertNotNull("Search response should not be null", response);
            assertTrue("Should have aggregations", response.getAggregationsCount() > 0);

            Aggregate termsResult = response.getAggregationsMap().get("status_terms");
            assertNotNull("Terms aggregation should exist", termsResult);
            assertTrue("Should be a string terms (sterms) aggregate", termsResult.hasSterms());

            StringTermsAggregate sterms = termsResult.getSterms();
            assertEquals("Should have two buckets", 2, sterms.getBucketsCount());
            assertEquals("doc_count_error_upper_bound should be 0", 0, sterms.getDocCountErrorUpperBound());
            assertEquals("sum_other_doc_count should be 0", 0, sterms.getSumOtherDocCount());

            // Buckets are returned in descending doc count order
            assertEquals("First bucket key should be 'active'", "active", sterms.getBuckets(0).getKey());
            assertEquals("First bucket doc count should be 2", 2, sterms.getBuckets(0).getDocCount());
            assertEquals("Second bucket key should be 'inactive'", "inactive", sterms.getBuckets(1).getKey());
            assertEquals("Second bucket doc count should be 1", 1, sterms.getBuckets(1).getDocCount());
        }
    }
}
