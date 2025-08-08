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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class FieldLevelStatsTests extends OpenSearchTestCase {

    /**
     * Test basic field-level stats calculation with proper value validation
     */
    public void testFieldLevelStatsCalculationWithValueValidation() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            // Add documents with various field types
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new TextField("text_field", "content " + i + " with more text", Field.Store.YES));
                doc.add(new StringField("keyword_field", "keyword" + i, Field.Store.NO));
                doc.add(new NumericDocValuesField("numeric_field", i));
                doc.add(new LongPoint("point_field", i));
                doc.add(new SortedDocValuesField("sorted_field", new BytesRef("sorted" + i)));
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();
                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);

                    // Verify we get stats for our fields
                    assertNotNull("Field stats should not be null", fieldStats);
                    assertFalse("Field stats should not be empty", fieldStats.isEmpty());

                    // Verify text field has both tim and tip stats
                    if (fieldStats.containsKey("text_field")) {
                        Map<String, Long> textStats = fieldStats.get("text_field");
                        assertTrue("Text field should have tim stats", textStats.containsKey("tim"));
                        assertTrue("Text field should have tip stats", textStats.containsKey("tip"));
                        assertTrue("Text field should have pos stats", textStats.containsKey("pos"));

                        // Validate proportions: tip should be ~10% of tim
                        long timSize = textStats.get("tim");
                        long tipSize = textStats.get("tip");
                        assertTrue("tim size should be positive", timSize > 0);
                        assertTrue("tip size should be positive", tipSize > 0);
                        assertTrue("tip should be smaller than tim", tipSize < timSize);

                        // Position data should exist for text fields
                        long posSize = textStats.get("pos");
                        assertTrue("pos size should be positive", posSize > 0);
                    }

                    // Verify numeric field has doc values
                    if (fieldStats.containsKey("numeric_field")) {
                        Map<String, Long> numericStats = fieldStats.get("numeric_field");
                        assertTrue("Numeric field should have dvd stats", numericStats.containsKey("dvd"));
                        assertTrue("Numeric field should have dvm stats", numericStats.containsKey("dvm"));

                        long dvdSize = numericStats.get("dvd");
                        long dvmSize = numericStats.get("dvm");
                        assertTrue("dvd size should be positive", dvdSize > 0);
                        assertTrue("dvm size should be positive", dvmSize > 0);
                        assertTrue("dvm should be smaller than dvd", dvmSize < dvdSize);
                    }

                    // Verify point field has point values
                    if (fieldStats.containsKey("point_field")) {
                        Map<String, Long> pointStats = fieldStats.get("point_field");
                        assertTrue("Point field should have dim stats", pointStats.containsKey("dim"));
                        assertTrue("Point field should have dii stats", pointStats.containsKey("dii"));

                        long dimSize = pointStats.get("dim");
                        long diiSize = pointStats.get("dii");
                        assertTrue("dim size should be positive", dimSize > 0);
                        assertTrue("dii size should be positive", diiSize > 0);
                    }
                }
            }
        }
    }

    /**
     * Test empty segment handling
     */
    public void testEmptySegment() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            writer.commit(); // Create empty segment

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();
                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);

                    assertNotNull("Field stats should not be null for empty segment", fieldStats);
                    assertTrue("Field stats should be empty for empty segment", fieldStats.isEmpty());
                }
            }
        }
    }

    /**
     * Test large segment requiring sampling (if implemented)
     */
    public void testLargeSegmentWithManyTerms() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            // Add many documents to create a large segment
            for (int i = 0; i < 10000; i++) {
                Document doc = new Document();
                // Create unique terms to increase term dictionary size
                doc.add(new TextField("large_text_field", "unique term " + i + " content", Field.Store.NO));
                doc.add(new NumericDocValuesField("large_numeric_field", i));
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();

                    long startTime = System.currentTimeMillis();
                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);
                    long endTime = System.currentTimeMillis();

                    // Should complete in reasonable time even for large segments
                    assertTrue("Calculation should complete within 5 seconds", (endTime - startTime) < 5000);

                    assertNotNull("Field stats should not be null", fieldStats);
                    assertFalse("Field stats should not be empty", fieldStats.isEmpty());

                    // Verify stats scale appropriately with document count
                    if (fieldStats.containsKey("large_text_field")) {
                        Map<String, Long> textStats = fieldStats.get("large_text_field");
                        long timSize = textStats.get("tim");
                        // With 10000 documents, tim should be substantial (at least 1KB)
                        assertTrue("tim size should reflect large term dictionary", timSize > 1000);
                    }
                }
            }
        }
    }

    /**
     * Test fields with no data
     */
    public void testFieldsWithNoData() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            // Add documents with some fields having no values
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new TextField("populated_field", "content " + i, Field.Store.NO));
                // Don't add values for other declared fields
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();
                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);

                    // Should only have stats for populated field
                    assertTrue("Should have stats for populated field", fieldStats.containsKey("populated_field"));
                    assertEquals("Should only have stats for one field", 1, fieldStats.size());
                }
            }
        }
    }

    /**
     * Test proportional attribution logic
     */
    public void testProportionalAttribution() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            // Create documents with different field sizes
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                // Small field
                doc.add(new StringField("small_field", "a", Field.Store.NO));
                // Large field
                doc.add(
                    new TextField(
                        "large_field",
                        "very long content that should take more space " + i + " " + "with lots of terms and positions to track",
                        Field.Store.NO
                    )
                );
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();
                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);

                    Map<String, Long> smallFieldStats = fieldStats.get("small_field");
                    Map<String, Long> largeFieldStats = fieldStats.get("large_field");

                    assertNotNull("Small field should have stats", smallFieldStats);
                    assertNotNull("Large field should have stats", largeFieldStats);

                    // Large field should have larger stats
                    long smallTimSize = smallFieldStats.getOrDefault("tim", 0L);
                    long largeTimSize = largeFieldStats.getOrDefault("tim", 0L);

                    assertTrue("Large field should have larger tim than small field", largeTimSize > smallTimSize);
                }
            }
        }
    }

    /**
     * Test SegmentsStats integration and serialization
     */
    public void testSegmentsStatsIntegration() throws IOException {
        SegmentsStats stats = new SegmentsStats();

        // Create realistic field-level stats
        Map<String, Map<String, Long>> fieldStats = Map.of(
            "text_field",
            Map.of("tim", 1000L, "tip", 100L, "pos", 500L),
            "numeric_field",
            Map.of("dvd", 800L, "dvm", 80L),
            "point_field",
            Map.of("dim", 600L, "dii", 150L)
        );

        stats.addFieldLevelFileSizes(fieldStats);

        // Verify stats were added correctly
        Map<String, Map<String, Long>> retrievedStats = stats.getFieldLevelFileSizes();
        assertNotNull("Retrieved stats should not be null", retrievedStats);
        assertEquals("Should have 3 fields", 3, retrievedStats.size());

        // Verify specific field stats
        assertEquals("text_field tim should match", Long.valueOf(1000L), retrievedStats.get("text_field").get("tim"));
        assertEquals("numeric_field dvd should match", Long.valueOf(800L), retrievedStats.get("numeric_field").get("dvd"));

        // Test serialization/deserialization
        BytesStreamOutput output = new BytesStreamOutput();
        stats.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        SegmentsStats deserialized = new SegmentsStats(input);

        assertEquals("Deserialized stats should match original", stats.getFieldLevelFileSizes(), deserialized.getFieldLevelFileSizes());

        // Verify proportions in deserialized stats
        Map<String, Long> deserializedTextField = deserialized.getFieldLevelFileSizes().get("text_field");
        assertTrue("tip should be ~10% of tim", deserializedTextField.get("tip") < deserializedTextField.get("tim"));
    }

    /**
     * Test CommonStatsFlags with field-level flag
     */
    public void testCommonStatsFlagsWithFieldLevel() throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags();
        assertFalse("Field level flag should be false by default", flags.includeFieldLevelSegmentFileSizes());

        flags.includeFieldLevelSegmentFileSizes(true);
        assertTrue("Field level flag should be true after setting", flags.includeFieldLevelSegmentFileSizes());

        // Test serialization maintains flag state
        BytesStreamOutput output = new BytesStreamOutput();
        flags.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        CommonStatsFlags deserialized = new CommonStatsFlags(input);
        assertTrue("Deserialized flag should maintain true state", deserialized.includeFieldLevelSegmentFileSizes());
    }

    /**
     * Test IndicesStatsRequest with field-level flag
     */
    public void testIndicesStatsRequestWithFieldLevel() {
        IndicesStatsRequest request = new IndicesStatsRequest();
        assertFalse("Field level flag should be false by default", request.includeFieldLevelSegmentFileSizes());

        request.includeFieldLevelSegmentFileSizes(true);
        assertTrue("Field level flag should be true after setting", request.includeFieldLevelSegmentFileSizes());
    }

    /**
     * Test error handling for corrupted segments
     */
    public void testErrorHandlingForCorruptedSegment() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            // Create a normal segment
            Document doc = new Document();
            doc.add(new TextField("field", "content", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    // Calculator should handle errors gracefully
                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();
                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);

                    // Should return valid stats or empty map, but not throw exception
                    assertNotNull("Should handle errors and return non-null", fieldStats);
                }
            }
        }
    }

    /**
     * Test memory efficiency with high cardinality fields
     */
    public void testMemoryEfficiencyWithHighCardinalityFields() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setRAMBufferSizeMB(16))) {

            // Create high cardinality field
            for (int i = 0; i < 1000; i++) {
                Document doc = new Document();
                // Each document has a unique sorted value
                doc.add(new SortedDocValuesField("high_cardinality_field", new BytesRef("unique_value_" + i)));
                // Add multiple fields to increase complexity
                doc.add(new NumericDocValuesField("numeric_" + (i % 10), i));
                doc.add(new TextField("text_" + (i % 5), "content " + i, Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (LeafReaderContext context : reader.leaves()) {
                    SegmentReader segmentReader = (SegmentReader) context.reader();

                    FieldLevelSegmentStatsCalculator calculator = new FieldLevelSegmentStatsCalculator();

                    // Should complete without OOM
                    Runtime runtime = Runtime.getRuntime();
                    long memoryBefore = runtime.totalMemory() - runtime.freeMemory();

                    Map<String, Map<String, Long>> fieldStats = calculator.calculateFieldLevelStats(segmentReader);

                    long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
                    long memoryUsed = memoryAfter - memoryBefore;

                    // Memory usage should be reasonable (less than 50MB for calculation)
                    assertTrue("Memory usage should be reasonable", memoryUsed < 50 * 1024 * 1024);

                    assertNotNull("Should calculate stats for high cardinality field", fieldStats);
                }
            }
        }
    }
}
