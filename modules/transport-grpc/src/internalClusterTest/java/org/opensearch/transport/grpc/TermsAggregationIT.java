/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.BucketOrder;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.ManagedChannel;

import java.util.List;

/**
 * Integration tests for Terms Aggregation via gRPC vs REST.
 * Tests behavioral parity between REST and gRPC implementations.
 */
public class TermsAggregationIT extends GrpcTransportBaseIT {

    private static final String TEST_INDEX = "terms-agg-test-idx";

    /**
     * Sets up test data for terms aggregation tests.
     */
    private void setupTestData() throws Exception {
        // Create index with explicit mapping
        String mapping = "{"
            + "\"properties\":{"
            + "\"category\":{\"type\":\"keyword\"},"
            + "\"product\":{\"type\":\"keyword\"},"
            + "\"price\":{\"type\":\"integer\"},"
            + "\"quantity\":{\"type\":\"integer\"}"
            + "}}";

        createIndex(TEST_INDEX);
        client().admin()
            .indices()
            .preparePutMapping(TEST_INDEX)
            .setSource(mapping, XContentType.JSON)
            .get();

        ensureGreen(TEST_INDEX);

        // Index test documents
        String[] docs = {
            "{\"category\":\"electronics\",\"product\":\"laptop\",\"price\":1200,\"quantity\":5}",
            "{\"category\":\"electronics\",\"product\":\"phone\",\"price\":800,\"quantity\":10}",
            "{\"category\":\"electronics\",\"product\":\"tablet\",\"price\":600,\"quantity\":3}",
            "{\"category\":\"books\",\"product\":\"fiction\",\"price\":20,\"quantity\":50}",
            "{\"category\":\"books\",\"product\":\"non-fiction\",\"price\":25,\"quantity\":30}",
            "{\"category\":\"clothing\",\"product\":\"shirt\",\"price\":30,\"quantity\":100}",
            "{\"category\":\"clothing\",\"product\":\"pants\",\"price\":50,\"quantity\":60}",
            "{\"category\":\"clothing\",\"product\":\"shoes\",\"price\":80,\"quantity\":40}"
        };

        for (int i = 0; i < docs.length; i++) {
            indexTestDocument(TEST_INDEX, String.valueOf(i + 1), docs[i]);
        }

        refresh(TEST_INDEX);
    }

    /**
     * Tests basic terms aggregation via REST API.
     */
    public void testBasicTermsAggregationViaREST() throws Exception {
        setupTestData();

        // Execute REST search with terms aggregation
        SearchResponse response = client().prepareSearch(TEST_INDEX)
            .setSize(0)
            .addAggregation(
                org.opensearch.search.aggregations.AggregationBuilders.terms("categories")
                    .field("category")
                    .size(10)
            )
            .get();

        // Verify response
        assertNotNull("Search response should not be null", response);
        assertNotNull("Aggregations should not be null", response.getAggregations());

        Terms categories = response.getAggregations().get("categories");
        assertNotNull("Categories aggregation should not be null", categories);

        List<? extends Terms.Bucket> buckets = categories.getBuckets();
        assertEquals("Should have 3 category buckets", 3, buckets.size());

        // Verify bucket contents
        long electronicsCount = buckets.stream()
            .filter(b -> b.getKeyAsString().equals("electronics"))
            .findFirst()
            .map(Terms.Bucket::getDocCount)
            .orElse(0L);
        assertEquals("Electronics should have 3 documents", 3, electronicsCount);

        long booksCount = buckets.stream()
            .filter(b -> b.getKeyAsString().equals("books"))
            .findFirst()
            .map(Terms.Bucket::getDocCount)
            .orElse(0L);
        assertEquals("Books should have 2 documents", 2, booksCount);

        long clothingCount = buckets.stream()
            .filter(b -> b.getKeyAsString().equals("clothing"))
            .findFirst()
            .map(Terms.Bucket::getDocCount)
            .orElse(0L);
        assertEquals("Clothing should have 3 documents", 3, clothingCount);

        logger.info("REST API terms aggregation test passed");
    }

    /**
     * Tests basic terms aggregation via gRPC API.
     */
    public void testBasicTermsAggregationViaGRPC() throws Exception {
        setupTestData();

        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Build terms aggregation
            TermsAggregation termsAgg = TermsAggregation.newBuilder()
                .setField("category")
                .setSize(10)
                .build();

            AggregationContainer aggContainer = AggregationContainer.newBuilder()
                .setTerms(termsAgg)
                .build();

            // Build search request with aggregation
            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("categories", aggContainer)
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(TEST_INDEX)
                .setSearchRequestBody(requestBody)
                .build();

            // Execute gRPC search
            org.opensearch.protobufs.SearchResponse grpcResponse = searchStub.search(searchRequest);

            // Verify response
            assertNotNull("gRPC search response should not be null", grpcResponse);
            assertTrue("Should have aggregations", grpcResponse.containsAggregations("categories"));

            org.opensearch.protobufs.Aggregate categoriesAgg = grpcResponse.getAggregationsMap().get("categories");
            assertNotNull("Categories aggregation should not be null", categoriesAgg);

            assertTrue("Should be a terms aggregation", categoriesAgg.hasStringTerms() ||
                      categoriesAgg.hasLongTerms() ||
                      categoriesAgg.hasDoubleTerms());

            // Get buckets (assuming string terms for keyword field)
            List<org.opensearch.protobufs.StringTermsBucket> buckets;
            if (categoriesAgg.hasStringTerms()) {
                buckets = categoriesAgg.getStringTerms().getBucketsList();
            } else {
                fail("Expected StringTerms for keyword field, got: " + categoriesAgg.getAggregateCase());
                return;
            }

            assertEquals("Should have 3 category buckets", 3, buckets.size());

            // Verify bucket contents
            long electronicsCount = buckets.stream()
                .filter(b -> b.getKey().equals("electronics"))
                .findFirst()
                .map(org.opensearch.protobufs.StringTermsBucket::getDocCount)
                .orElse(0L);
            assertEquals("Electronics should have 3 documents", 3, electronicsCount);

            long booksCount = buckets.stream()
                .filter(b -> b.getKey().equals("books"))
                .findFirst()
                .map(org.opensearch.protobufs.StringTermsBucket::getDocCount)
                .orElse(0L);
            assertEquals("Books should have 2 documents", 2, booksCount);

            long clothingCount = buckets.stream()
                .filter(b -> b.getKey().equals("clothing"))
                .findFirst()
                .map(org.opensearch.protobufs.StringTermsBucket::getDocCount)
                .orElse(0L);
            assertEquals("Clothing should have 3 documents", 3, clothingCount);

            logger.info("gRPC API terms aggregation test passed");
        }
    }

    /**
     * Tests REST vs gRPC parity with size parameter.
     */
    public void testTermsAggregationParityWithSize() throws Exception {
        setupTestData();

        // REST request
        SearchResponse restResponse = client().prepareSearch(TEST_INDEX)
            .setSize(0)
            .addAggregation(
                org.opensearch.search.aggregations.AggregationBuilders.terms("categories")
                    .field("category")
                    .size(2)
            )
            .get();

        Terms restCategories = restResponse.getAggregations().get("categories");
        List<? extends Terms.Bucket> restBuckets = restCategories.getBuckets();
        assertEquals("REST should return 2 buckets", 2, restBuckets.size());

        // gRPC request
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            TermsAggregation termsAgg = TermsAggregation.newBuilder()
                .setField("category")
                .setSize(2)
                .build();

            AggregationContainer aggContainer = AggregationContainer.newBuilder()
                .setTerms(termsAgg)
                .build();

            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("categories", aggContainer)
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(TEST_INDEX)
                .setSearchRequestBody(requestBody)
                .build();

            org.opensearch.protobufs.SearchResponse grpcResponse = searchStub.search(searchRequest);

            org.opensearch.protobufs.Aggregate categoriesAgg = grpcResponse.getAggregationsMap().get("categories");
            List<org.opensearch.protobufs.StringTermsBucket> grpcBuckets = categoriesAgg.getStringTerms().getBucketsList();

            assertEquals("gRPC should return 2 buckets", 2, grpcBuckets.size());

            // Verify parity - both should return same buckets
            assertEquals("Bucket counts should match", restBuckets.size(), grpcBuckets.size());

            logger.info("REST vs gRPC parity test passed for size parameter");
        }
    }

    /**
     * Tests REST vs gRPC parity with min_doc_count parameter.
     */
    public void testTermsAggregationParityWithMinDocCount() throws Exception {
        setupTestData();

        int minDocCount = 3;

        // REST request
        SearchResponse restResponse = client().prepareSearch(TEST_INDEX)
            .setSize(0)
            .addAggregation(
                org.opensearch.search.aggregations.AggregationBuilders.terms("categories")
                    .field("category")
                    .minDocCount(minDocCount)
            )
            .get();

        Terms restCategories = restResponse.getAggregations().get("categories");
        List<? extends Terms.Bucket> restBuckets = restCategories.getBuckets();

        // Should only return categories with 3+ documents (electronics and clothing)
        assertEquals("REST should return 2 buckets with min_doc_count=3", 2, restBuckets.size());

        // gRPC request
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            TermsAggregation termsAgg = TermsAggregation.newBuilder()
                .setField("category")
                .setMinDocCount(minDocCount)
                .build();

            AggregationContainer aggContainer = AggregationContainer.newBuilder()
                .setTerms(termsAgg)
                .build();

            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("categories", aggContainer)
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(TEST_INDEX)
                .setSearchRequestBody(requestBody)
                .build();

            org.opensearch.protobufs.SearchResponse grpcResponse = searchStub.search(searchRequest);

            org.opensearch.protobufs.Aggregate categoriesAgg = grpcResponse.getAggregationsMap().get("categories");
            List<org.opensearch.protobufs.StringTermsBucket> grpcBuckets = categoriesAgg.getStringTerms().getBucketsList();

            assertEquals("gRPC should return 2 buckets with min_doc_count=3", 2, grpcBuckets.size());

            // Verify all buckets have at least min_doc_count documents
            for (org.opensearch.protobufs.StringTermsBucket bucket : grpcBuckets) {
                assertTrue(
                    "Bucket " + bucket.getKey() + " should have at least " + minDocCount + " documents",
                    bucket.getDocCount() >= minDocCount
                );
            }

            logger.info("REST vs gRPC parity test passed for min_doc_count parameter");
        }
    }

    /**
     * Comprehensive test comparing REST and gRPC results for exact match.
     */
    public void testRESTvsGRPCExactParity() throws Exception {
        setupTestData();

        // REST request
        SearchResponse restResponse = client().prepareSearch(TEST_INDEX)
            .setSize(0)
            .addAggregation(
                org.opensearch.search.aggregations.AggregationBuilders.terms("categories")
                    .field("category")
                    .size(10)
                    .order(org.opensearch.search.aggregations.BucketOrder.count(false))
            )
            .get();

        Terms restCategories = restResponse.getAggregations().get("categories");
        List<? extends Terms.Bucket> restBuckets = restCategories.getBuckets();

        // gRPC request
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            BucketOrder order = BucketOrder.newBuilder()
                .setXCount(org.opensearch.protobufs.SortOrder.DESC)
                .build();

            TermsAggregation termsAgg = TermsAggregation.newBuilder()
                .setField("category")
                .setSize(10)
                .addOrder(order)
                .build();

            AggregationContainer aggContainer = AggregationContainer.newBuilder()
                .setTerms(termsAgg)
                .build();

            SearchRequestBody requestBody = SearchRequestBody.newBuilder()
                .setSize(0)
                .putAggregations("categories", aggContainer)
                .build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(TEST_INDEX)
                .setSearchRequestBody(requestBody)
                .build();

            org.opensearch.protobufs.SearchResponse grpcResponse = searchStub.search(searchRequest);

            org.opensearch.protobufs.Aggregate categoriesAgg = grpcResponse.getAggregationsMap().get("categories");
            List<org.opensearch.protobufs.StringTermsBucket> grpcBuckets = categoriesAgg.getStringTerms().getBucketsList();

            // Compare bucket counts
            assertEquals("Bucket counts should match", restBuckets.size(), grpcBuckets.size());

            // Compare each bucket
            for (int i = 0; i < restBuckets.size(); i++) {
                Terms.Bucket restBucket = restBuckets.get(i);
                org.opensearch.protobufs.StringTermsBucket grpcBucket = grpcBuckets.get(i);

                assertEquals(
                    "Bucket " + i + " keys should match",
                    restBucket.getKeyAsString(),
                    grpcBucket.getKey()
                );

                assertEquals(
                    "Bucket " + i + " doc counts should match",
                    restBucket.getDocCount(),
                    grpcBucket.getDocCount()
                );
            }

            logger.info("REST vs gRPC exact parity test PASSED - results match perfectly!");
        }
    }
}
