/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for FieldLevelSegmentStatsCalculator
 */
public class FieldLevelSegmentStatsCalculatorTests extends OpenSearchTestCase {

    /**
     * Test core calculation functionality including basic stats, field filtering,
     * proportional attribution, and edge cases
     */
    public void testCoreCalculationAndFiltering() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            // Add documents with various field types
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new TextField("text_field", "content " + i + " with text", Field.Store.YES));
                doc.add(new StringField("keyword_field", "keyword" + i, Field.Store.NO));
                doc.add(new NumericDocValuesField("numeric_docvalues", i));
                doc.add(new IntPoint("int_point", i));
                doc.add(new SortedDocValuesField("sorted_docvalues", new BytesRef("sorted" + i)));
                // Sparse field - only in some docs
                if (i % 10 == 0) {
                    doc.add(new TextField("sparse_field", "sparse " + i, Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
                FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();

                // Test basic calculation
                Map<String, Map<String, Long>> stats = calculator.calculateFieldLevelStats(segmentReader);
                assertNotNull("Stats should not be null", stats);
                assertFalse("Stats should not be empty", stats.isEmpty());

                // Verify all field types have appropriate stats
                assertTrue("Should have text field", stats.containsKey("text_field"));
                assertTrue("Should have keyword field", stats.containsKey("keyword_field"));
                assertTrue("Should have numeric docvalues", stats.containsKey("numeric_docvalues"));
                assertTrue("Should have sorted docvalues", stats.containsKey("sorted_docvalues"));
                assertTrue("Should have point values", stats.containsKey("int_point"));
                assertTrue("Should have sparse field", stats.containsKey("sparse_field"));

                // Verify proportional attribution (sparse field should be smaller)
                Map<String, Long> sparseStats = stats.get("sparse_field");
                Map<String, Long> textStats = stats.get("text_field");
                assertTrue(
                    "Sparse field should have smaller stats",
                    sparseStats.getOrDefault("tim", 0L) < textStats.getOrDefault("tim", 0L)
                );

                // Test field filtering
                Set<String> filter = Set.of("text_field", "numeric_docvalues");
                Map<String, Map<String, Long>> filteredStats = calculator.calculateFieldLevelStats(segmentReader, filter, false);
                assertEquals("Should only have filtered fields", 2, filteredStats.size());
                assertTrue("Should have text_field", filteredStats.containsKey("text_field"));
                assertTrue("Should have numeric_docvalues", filteredStats.containsKey("numeric_docvalues"));
                assertFalse("Should not have keyword_field", filteredStats.containsKey("keyword_field"));

                // Test empty segment edge case
                writer.deleteAll();
                writer.commit();
            }
        }
    }

    /**
     * Test sampling for large segments with reduced complexity
     */
    public void testSamplingAndLargeSegments() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            // Create a large segment
            for (int i = 0; i < 10000; i++) {
                Document doc = new Document();
                doc.add(new TextField("text", "Document " + i, Field.Store.NO));
                doc.add(new NumericDocValuesField("numeric", i));
                for (int j = 0; j < 10; j++) {
                    doc.add(new TextField("field_" + j, "content_" + i, Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
                // Use low threshold to trigger sampling
                FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator(1024 * 1024);

                Map<String, Map<String, Long>> sampledStats = calculator.calculateFieldLevelStats(segmentReader, null, true);
                assertNotNull("Sampled stats should not be null", sampledStats);
                assertFalse("Sampled stats should not be empty", sampledStats.isEmpty());

                // Verify sampling metadata
                for (Map<String, Long> fieldStats : sampledStats.values()) {
                    assertEquals("Should be marked as sampled", 1L, (long) fieldStats.get("_sampled"));
                    assertEquals("Should show 10% sampling rate", 10L, (long) fieldStats.get("_sampling_rate"));
                }

                // Verify reasonable performance
                long start = System.currentTimeMillis();
                calculator.calculateFieldLevelStats(segmentReader, null, true);
                long elapsed = System.currentTimeMillis() - start;
                assertTrue("Sampling should be fast", elapsed < 5000);
            }
        }
    }

    /**
     * Test comprehensive error handling including memory constraints and edge cases
     */
    public void testComprehensiveErrorHandling() throws IOException {
        // Test with actual segments that trigger error handling paths
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            // Create a document
            Document doc = new Document();
            doc.add(new TextField("test_field", "test content", Field.Store.NO));
            doc.add(new NumericDocValuesField("numeric", 42));
            writer.addDocument(doc);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();

                // Test with very low memory threshold to trigger sampling
                FieldLevelSegmentStatsCalculator lowMemCalculator = new FieldLevelSegmentStatsCalculator(1);
                Map<String, Map<String, Long>> sampledStats = lowMemCalculator.calculateFieldLevelStats(segmentReader, null, true);

                // Should handle memory constraints with sampling
                assertNotNull("Should handle low memory with sampling", sampledStats);
                for (Map<String, Long> fieldStats : sampledStats.values()) {
                    assertEquals("Should be marked as sampled", 1L, (long) fieldStats.get("_sampled"));
                }

                // Test with standard calculator
                FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();
                Map<String, Map<String, Long>> stats = calculator.calculateFieldLevelStats(segmentReader);
                assertNotNull("Should calculate stats normally", stats);

                // Test with field filter on non-existent field
                Set<String> nonExistentFilter = Set.of("non_existent_field");
                Map<String, Map<String, Long>> filteredStats = calculator.calculateFieldLevelStats(segmentReader, nonExistentFilter, false);
                assertTrue("Should return empty for non-existent fields", filteredStats.isEmpty());

                // Test with empty field filter
                Set<String> emptyFilter = Set.of();
                Map<String, Map<String, Long>> emptyFilterStats = calculator.calculateFieldLevelStats(segmentReader, emptyFilter, false);
                assertTrue("Should return empty for empty filter", emptyFilterStats.isEmpty());
            }
        }
    }

    /**
     * Test caching behavior
     */
    public void testCachingBehavior() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            for (int i = 0; i < 50; i++) {
                Document doc = new Document();
                doc.add(new TextField("field", "text " + i, Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
                FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();

                Map<String, Map<String, Long>> stats1 = calculator.calculateFieldLevelStats(segmentReader);
                Map<String, Map<String, Long>> stats2 = calculator.calculateFieldLevelStats(segmentReader);
                assertEquals("Cached results should match", stats1, stats2);

                // Field filtering should bypass cache
                Set<String> filter = Set.of("field");
                Map<String, Map<String, Long>> filtered = calculator.calculateFieldLevelStats(segmentReader, filter, false);
                assertEquals("Filtered stats should have one field", 1, filtered.size());
            }
        }
    }

    /**
     * Test integration with stats classes including serialization and XContent
     */
    public void testIntegrationWithStatsClasses() throws IOException {
        // Test SegmentsStats integration and serialization
        SegmentsStats stats = new SegmentsStats();
        Map<String, Map<String, Long>> fieldStats = Map.of(
            "text_field",
            Map.of("tim", 1000L, "tip", 100L, "_sampled", 1L),
            "numeric_field",
            Map.of("dvd", 800L, "dvm", 80L)
        );
        stats.addFieldLevelFileSizes(fieldStats);

        // Test binary serialization
        BytesStreamOutput output = new BytesStreamOutput();
        stats.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        SegmentsStats deserialized = new SegmentsStats(input);
        assertEquals("Deserialized stats should match", fieldStats, deserialized.getFieldLevelFileSizes());

        // Test XContent serialization
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue("JSON should contain field_level_file_sizes", json.contains("field_level_file_sizes"));
        assertTrue("JSON should contain sampling metadata", json.contains("_sampled"));

        // Test CommonStatsFlags
        CommonStatsFlags flags = new CommonStatsFlags();
        assertFalse("Should be false by default", flags.includeFieldLevelSegmentFileSizes());
        flags.includeFieldLevelSegmentFileSizes(true);
        assertTrue("Should be true after setting", flags.includeFieldLevelSegmentFileSizes());

        // Test IndicesStatsRequest
        IndicesStatsRequest request = new IndicesStatsRequest();
        assertFalse("Should be false by default", request.includeFieldLevelSegmentFileSizes());
        request.includeFieldLevelSegmentFileSizes(true);
        assertTrue("Should be true after setting", request.includeFieldLevelSegmentFileSizes());
    }
}
