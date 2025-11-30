/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

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

import static org.opensearch.common.util.BitMixer.mix64;
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
            byte[] sketchBytes = out.bytes().toBytesRef().bytes;

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
                byte[] sketchBytes = out.bytes().toBytesRef().bytes;

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
                    byte[] sketchBytes = out.bytes().toBytesRef().bytes;

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
                byte[] sketchBytes = out.bytes().toBytesRef().bytes;

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
            client().prepareIndex(indexName)
                .setSource(XContentFactory.jsonBuilder().startObject().field("sketch", out1.bytes().toBytesRef().bytes).endObject())
                .get();

            BytesStreamOutput out2 = new BytesStreamOutput();
            sketch2.writeTo(0, out2);
            client().prepareIndex(indexName)
                .setSource(XContentFactory.jsonBuilder().startObject().field("sketch", out2.bytes().toBytesRef().bytes).endObject())
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
}
