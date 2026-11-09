/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.common.util.BitMixer.mix64;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;

/**
 * Integration tests for HLL field mapper demonstrating programmatic field creation
 * and usage patterns for ISM plugin rollup scenarios.
 */
public class HllFieldMapperIntegrationTests extends OpenSearchSingleNodeTestCase {

    public void testProgrammaticFieldCreation() throws IOException {
        // Test that HLL fields can be created

        String indexName = "test-hll-index";
        int precision = 12;

        // Create index with HLL field mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("cardinality_sketch")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Verify the field was created
        MapperService mapperService = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(indexName)).mapperService();

        MappedFieldType fieldType = mapperService.fieldType("cardinality_sketch");
        assertNotNull("HLL field should be created", fieldType);
        assertTrue("Field should be HLL type", fieldType instanceof HllFieldMapper.HllFieldType);

        HllFieldMapper.HllFieldType hllFieldType = (HllFieldMapper.HllFieldType) fieldType;
        assertEquals("Precision should match", precision, hllFieldType.precision());
    }

    public void testDataIngestion() throws IOException {
        // Test ingesting pre-aggregated HLL sketch data (simulating rollup scenario)

        String indexName = "test-rollup-index";
        int precision = 11;

        // Create index with HLL field
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("user_cardinality")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .startObject("timestamp")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Create an HLL sketch with some user IDs
        HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            // Creating the sketch manually here
            // Plugins/consumers can further decide how the sketches are to be created or retrieved
            for (int userId = 1; userId <= 100; userId++) {
                sketch.collect(0, mix64(userId));
            }

            // Serialize the sketch
            BytesStreamOutput out = new BytesStreamOutput();
            sketch.writeTo(0, out);
            org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            // Index a document with the pre-aggregated sketch
            client().prepareIndex(indexName)
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("timestamp", "2024-01-01T00:00:00Z")
                        .field("user_cardinality", sketchBytes)
                        .endObject()
                )
                .get();

            // Refresh to make the document searchable
            client().admin().indices().prepareRefresh(indexName).get();

            // Verify the document was indexed
            long docCount = client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits().value();
            assertEquals("Document should be indexed", 1L, docCount);
        } finally {
            sketch.close();
        }
    }

    public void testMultipleRollupDocuments() throws IOException {
        // Test ingesting multiple rollup documents with HLL
        // Testing indexing/retrieving multiple documents as useful for maybe rollup scenario

        String indexName = "test-multi-rollup";
        int precision = 11;

        // Create index
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("hourly_users")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .startObject("hour")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Index multiple hourly rollup documents
        int numHours = 24;
        for (int hour = 0; hour < numHours; hour++) {
            HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            try {
                // Each hour has some unique users
                for (int userId = hour * 10; userId < hour * 10 + 50; userId++) {
                    sketch.collect(0, mix64(userId));
                }

                BytesStreamOutput out = new BytesStreamOutput();
                sketch.writeTo(0, out);
                org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
                byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

                client().prepareIndex(indexName)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("hour", "2024-01-01T" + String.format(java.util.Locale.ROOT, "%02d", hour) + ":00:00Z")
                            .field("hourly_users", sketchBytes)
                            .endObject()
                    )
                    .get();
            } finally {
                sketch.close();
            }
        }

        // Refresh and verify
        client().admin().indices().prepareRefresh(indexName).get();
        long docCount = client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits().value();
        assertEquals("All hourly documents should be indexed", numHours, docCount);
    }

    public void testCardinalityAggregationMergesMultipleSketches() throws IOException {
        // Test that cardinality aggregation merges multiple HLL sketches correctly

        String indexName = "test-hll-merge-aggregation";
        int precision = 11;

        // Create index
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("hourly_users")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Create an expected merged sketch for comparison
        HyperLogLogPlusPlus expectedMerged = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        long expectedCardinality = 500;

        // Create multiple sketches with non-overlapping values
        int numDocs = 5;
        int valuesPerDoc = 100;

        try {
            for (int docIdx = 0; docIdx < numDocs; docIdx++) {
                HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                try {
                    for (int i = 0; i < valuesPerDoc; i++) {
                        long value = (long) docIdx * valuesPerDoc + i;
                        long hash = mix64(value);
                        sketch.collect(0, hash);
                        expectedMerged.collect(0, hash); // Also collect into expected
                    }

                    BytesStreamOutput out = new BytesStreamOutput();
                    sketch.writeTo(0, out);
                    org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
                    byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

                    client().prepareIndex(indexName)
                        .setSource(XContentFactory.jsonBuilder().startObject().field("hourly_users", sketchBytes).endObject())
                        .get();
                } finally {
                    sketch.close();
                }
            }

            client().admin().indices().prepareRefresh(indexName).get();

            // Run cardinality aggregation across all documents
            SearchResponse response = client().prepareSearch(indexName)
                .addAggregation(cardinality("total_users").field("hourly_users"))
                .get();

            Cardinality cardinalityAgg = response.getAggregations().get("total_users");
            assertNotNull("Cardinality aggregation should return a result", cardinalityAgg);

            long aggregatedCardinality = cardinalityAgg.getValue();

            // The aggregated cardinality should match the expected cardinality
            assertEquals("Aggregation should merge sketches correctly", expectedCardinality, aggregatedCardinality);

            // The aggregated cardinality should match the cardinality from expected expectedMerged
            assertEquals("Aggregation should merge sketches correctly", expectedMerged.cardinality(0), aggregatedCardinality);
        } finally {
            expectedMerged.close();
        }
    }

    public void testCardinalityAggregationWithBuckets() throws IOException {
        // Test that cardinality aggregation works correctly within bucketed aggregations

        String indexName = "test-hll-bucketed-aggregation";
        int precision = 11;

        // Create index with HLL field and category field
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("user_sketch")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Create documents with different categories
        String[] categories = { "electronics", "books", "clothing" };
        int valuesPerCategory = 100;

        for (String category : categories) {
            HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            try {
                // Each category gets unique user IDs
                int baseUserId = category.hashCode() & 0x7FFFFFFF; // Positive hash
                for (int i = 0; i < valuesPerCategory; i++) {
                    sketch.collect(0, mix64(baseUserId + i));
                }

                BytesStreamOutput out = new BytesStreamOutput();
                sketch.writeTo(0, out);
                org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
                byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

                client().prepareIndex(indexName)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("category", category)
                            .field("user_sketch", sketchBytes)
                            .endObject()
                    )
                    .get();
            } finally {
                sketch.close();
            }
        }

        client().admin().indices().prepareRefresh(indexName).get();

        // Run terms aggregation with cardinality sub-aggregation
        SearchResponse response = client().prepareSearch(indexName)
            .addAggregation(terms("by_category").field("category").subAggregation(cardinality("users_per_category").field("user_sketch")))
            .get();

        Terms termsAgg = response.getAggregations().get("by_category");
        assertNotNull("Terms aggregation should return results", termsAgg);
        assertEquals("Should have 3 category buckets", 3, termsAgg.getBuckets().size());

        // Verify each bucket has cardinality
        for (Bucket bucket : termsAgg.getBuckets()) {
            Cardinality cardAgg = bucket.getAggregations().get("users_per_category");
            assertNotNull("Each bucket should have cardinality", cardAgg);

            long bucketCardinality = cardAgg.getValue();
            assertTrue(
                "Bucket " + bucket.getKeyAsString() + " should have cardinality around " + valuesPerCategory + ", got " + bucketCardinality,
                bucketCardinality > 0 && bucketCardinality <= valuesPerCategory * 1.2
            );
        }
    }

    public void testSimpleTwoSketchMerge() throws IOException {
        // Simplified test to debug the merge issue

        String indexName = "test-simple-merge";
        int precision = 11;

        // Create index
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("sketch")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Create two simple sketches
        HyperLogLogPlusPlus sketch1 = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus sketch2 = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus expected = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            // Sketch 1: values 0-99
            for (int i = 0; i < 100; i++) {
                long hash = mix64(i);
                sketch1.collect(0, hash);
                expected.collect(0, hash);
            }

            // Sketch 2: values 100-199
            for (int i = 100; i < 200; i++) {
                long hash = mix64(i);
                sketch2.collect(0, hash);
                expected.collect(0, hash);
            }

            long expectedCard = expected.cardinality(0);

            // Index both sketches
            BytesStreamOutput out1 = new BytesStreamOutput();
            sketch1.writeTo(0, out1);
            org.apache.lucene.util.BytesRef bytesRef1 = out1.bytes().toBytesRef();
            byte[] sketchBytes1 = Arrays.copyOfRange(bytesRef1.bytes, bytesRef1.offset, bytesRef1.offset + bytesRef1.length);
            client().prepareIndex(indexName)
                .setSource(XContentFactory.jsonBuilder().startObject().field("sketch", sketchBytes1).endObject())
                .get();

            BytesStreamOutput out2 = new BytesStreamOutput();
            sketch2.writeTo(0, out2);
            org.apache.lucene.util.BytesRef bytesRef2 = out2.bytes().toBytesRef();
            byte[] sketchBytes2 = Arrays.copyOfRange(bytesRef2.bytes, bytesRef2.offset, bytesRef2.offset + bytesRef2.length);
            client().prepareIndex(indexName)
                .setSource(XContentFactory.jsonBuilder().startObject().field("sketch", sketchBytes2).endObject())
                .get();

            client().admin().indices().prepareRefresh(indexName).get();

            SearchResponse response = client().prepareSearch(indexName).addAggregation(cardinality("test").field("sketch")).get();

            long result = response.getAggregations().<org.opensearch.search.aggregations.metrics.Cardinality>get("test").getValue();

            assertEquals("Should merge both sketches", expectedCard, result);
        } finally {
            sketch1.close();
            sketch2.close();
            expected.close();
        }
    }

    public void testSparseHllFieldWithMixedDocuments() throws IOException {
        // Test that HLL fields work correctly when some documents have the field and others don't
        // This is a scenario where not all documents may have pre-aggregated sketches

        String indexName = "test-sparse-hll";
        int precision = 11;

        // Create index with HLL field and other fields
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("timestamp")
            .field("type", "date")
            .endObject()
            .startObject("user_sketch")
            .field("type", "hll")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(indexName).setMapping(mapping).get();

        // Index documents: some with HLL field, some without
        int docsWithHll = 3;
        int docsWithoutHll = 2;
        int valuesPerSketch = 50;

        HyperLogLogPlusPlus expectedMerged = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            // Index documents WITH HLL field
            for (int i = 0; i < docsWithHll; i++) {
                HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                try {
                    // Each sketch has unique values
                    for (int j = 0; j < valuesPerSketch; j++) {
                        long value = (long) i * valuesPerSketch + j;
                        long hash = mix64(value);
                        sketch.collect(0, hash);
                        expectedMerged.collect(0, hash);
                    }

                    BytesStreamOutput out = new BytesStreamOutput();
                    sketch.writeTo(0, out);
                    org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
                    byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

                    client().prepareIndex(indexName)
                        .setSource(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("category", "with_hll")
                                .field("timestamp", "2024-01-0" + (i + 1) + "T00:00:00Z")
                                .field("user_sketch", sketchBytes)
                                .endObject()
                        )
                        .get();
                } finally {
                    sketch.close();
                }
            }

            // Index documents WITHOUT HLL field
            for (int i = 0; i < docsWithoutHll; i++) {
                client().prepareIndex(indexName)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("category", "without_hll")
                            .field("timestamp", "2024-01-0" + (docsWithHll + i + 1) + "T00:00:00Z")
                            // No user_sketch field
                            .endObject()
                    )
                    .get();
            }

            client().admin().indices().prepareRefresh(indexName).get();

            // Test 1: Verify all documents are indexed
            SearchResponse allDocsResponse = client().prepareSearch(indexName).setSize(0).get();
            assertEquals("All documents should be indexed", docsWithHll + docsWithoutHll, allDocsResponse.getHits().getTotalHits().value());

            // Test 2: Search for documents with HLL field using exists query
            SearchResponse withHllResponse = client().prepareSearch(indexName)
                .setQuery(org.opensearch.index.query.QueryBuilders.existsQuery("user_sketch"))
                .setSize(0)
                .get();
            assertEquals("Should find only documents with HLL field", docsWithHll, withHllResponse.getHits().getTotalHits().value());

            // Test 3: Search for documents without HLL field
            SearchResponse withoutHllResponse = client().prepareSearch(indexName)
                .setQuery(
                    org.opensearch.index.query.QueryBuilders.boolQuery()
                        .mustNot(org.opensearch.index.query.QueryBuilders.existsQuery("user_sketch"))
                )
                .setSize(0)
                .get();
            assertEquals(
                "Should find only documents without HLL field",
                docsWithoutHll,
                withoutHllResponse.getHits().getTotalHits().value()
            );

            // Test 4: Cardinality aggregation across all documents (should only aggregate docs with HLL field)
            SearchResponse cardinalityResponse = client().prepareSearch(indexName)
                .addAggregation(cardinality("total_users").field("user_sketch"))
                .get();

            Cardinality cardinalityAgg = cardinalityResponse.getAggregations().get("total_users");
            assertNotNull("Cardinality aggregation should return a result", cardinalityAgg);

            long aggregatedCardinality = cardinalityAgg.getValue();
            long expectedCardinality = expectedMerged.cardinality(0);

            assertEquals("Cardinality should only aggregate documents with HLL field", expectedCardinality, aggregatedCardinality);

            // Test 5: Terms aggregation by category with cardinality sub-aggregation
            SearchResponse termsResponse = client().prepareSearch(indexName)
                .addAggregation(
                    terms("by_category").field("category").subAggregation(cardinality("users_per_category").field("user_sketch"))
                )
                .get();

            Terms termsAgg = termsResponse.getAggregations().get("by_category");
            assertNotNull("Terms aggregation should return results", termsAgg);
            assertEquals("Should have 2 category buckets", 2, termsAgg.getBuckets().size());

            // Verify the bucket with HLL field has cardinality
            Terms.Bucket withHllBucket = termsAgg.getBucketByKey("with_hll");
            assertNotNull("Should have bucket for 'with_hll' category", withHllBucket);
            assertEquals("Bucket should have correct doc count", docsWithHll, withHllBucket.getDocCount());

            Cardinality withHllCardinality = withHllBucket.getAggregations().get("users_per_category");
            assertNotNull("Bucket should have cardinality aggregation", withHllCardinality);
            assertEquals("Cardinality should match expected", expectedCardinality, withHllCardinality.getValue());

            // Verify the bucket without HLL field has zero cardinality
            Terms.Bucket withoutHllBucket = termsAgg.getBucketByKey("without_hll");
            assertNotNull("Should have bucket for 'without_hll' category", withoutHllBucket);
            assertEquals("Bucket should have correct doc count", docsWithoutHll, withoutHllBucket.getDocCount());

            Cardinality withoutHllCardinality = withoutHllBucket.getAggregations().get("users_per_category");
            assertNotNull("Bucket should have cardinality aggregation", withoutHllCardinality);
            assertEquals("Cardinality should be 0 for documents without HLL field", 0L, withoutHllCardinality.getValue());

            // Test 6: Retrieve a document with HLL field and verify it's in the response
            SearchResponse getDocWithHll = client().prepareSearch(indexName).setQuery(termQuery("category", "with_hll")).setSize(1).get();
            assertEquals("Should find document with HLL field", 1, getDocWithHll.getHits().getHits().length);
            assertTrue(
                "Document should contain user_sketch field",
                getDocWithHll.getHits().getAt(0).getSourceAsMap().containsKey("user_sketch")
            );

            // Test 7: Retrieve a document without HLL field and verify it's not in the response
            SearchResponse getDocWithoutHll = client().prepareSearch(indexName)
                .setQuery(termQuery("category", "without_hll"))
                .setSize(1)
                .get();
            assertEquals("Should find document without HLL field", 1, getDocWithoutHll.getHits().getHits().length);
            assertFalse(
                "Document should not contain user_sketch field",
                getDocWithoutHll.getHits().getAt(0).getSourceAsMap().containsKey("user_sketch")
            );
        } finally {
            expectedMerged.close();
        }
    }

    public void testPrecisionMismatchAcrossIndices() throws IOException {
        // Test aggregating across multiple indices with same field name but different precisions
        // This validates that OpenSearch properly rejects aggregations across indices with
        // mismatched HLL precision configurations

        String index1 = "test-hll-precision-12";
        String index2 = "test-hll-precision-14";
        int precision1 = 12;
        int precision2 = 14;

        // Create first index with precision 12
        XContentBuilder mapping1 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("user_sketch")
            .field("type", "hll")
            .field("precision", precision1)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(index1).setMapping(mapping1).get();

        // Create second index with precision 14 (different precision, same field name)
        XContentBuilder mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("user_sketch")
            .field("type", "hll")
            .field("precision", precision2)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(index2).setMapping(mapping2).get();

        // Create and index sketch with precision 12 in first index
        HyperLogLogPlusPlus sketch1 = new HyperLogLogPlusPlus(precision1, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            for (int i = 0; i < 50; i++) {
                sketch1.collect(0, mix64(i));
            }

            BytesStreamOutput out = new BytesStreamOutput();
            sketch1.writeTo(0, out);
            org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            client().prepareIndex(index1)
                .setId("1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("user_sketch", sketchBytes).endObject())
                .get();
        } finally {
            sketch1.close();
        }

        // Create and index sketch with precision 14 in second index
        HyperLogLogPlusPlus sketch2 = new HyperLogLogPlusPlus(precision2, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            for (int i = 0; i < 50; i++) {
                sketch2.collect(0, mix64(i + 100));
            }

            BytesStreamOutput out = new BytesStreamOutput();
            sketch2.writeTo(0, out);
            org.apache.lucene.util.BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] sketchBytes = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            client().prepareIndex(index2)
                .setId("1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("user_sketch", sketchBytes).endObject())
                .get();
        } finally {
            sketch2.close();
        }

        client().admin().indices().prepareRefresh(index1, index2).get();

        // Attempt to aggregate across both indices using wildcard pattern
        // This should fail because the indices have different precisions for the same field
        SearchPhaseExecutionException exception = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch("test-hll-precision-*").addAggregation(cardinality("total_users").field("user_sketch")).get();
        });

        // Verify the error mentions precision mismatch
        String errorMessage = exception.getMessage();
        Throwable cause = exception.getCause();
        String fullMessage = errorMessage + (cause != null ? " " + cause.getMessage() : "");

        assertTrue(
            "Error should mention precision mismatch. Got: " + fullMessage,
            fullMessage.contains("Cannot merge HLL++ sketches with different precision")
        );
    }
}
