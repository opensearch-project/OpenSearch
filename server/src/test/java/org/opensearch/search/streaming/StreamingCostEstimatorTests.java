/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamingCostEstimator} and {@link FlushModeResolver#decideFlushMode}.
 */
public class StreamingCostEstimatorTests extends OpenSearchTestCase {

    // ========================
    // Tests for decideFlushMode
    // ========================

    /**
     * Test decideFlushMode with valid high cardinality metrics - should stream.
     */
    public void testDecideFlushModeWithHighCardinality() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 50000, 100000);

        FlushMode result = FlushModeResolver.decideFlushMode(
            metrics,
            FlushMode.PER_SHARD,
            100000, // max bucket count
            0.01,   // min cardinality ratio
            1000    // min bucket count
        );

        assertEquals("should stream for valid metrics", FlushMode.PER_SEGMENT, result);
    }

    /**
     * Test decideFlushMode returns PER_SHARD when bucket count exceeds max.
     */
    public void testDecideFlushModeExceedsMaxBuckets() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 200000, 100000);

        FlushMode result = FlushModeResolver.decideFlushMode(
            metrics,
            FlushMode.PER_SHARD,
            100000, // max bucket count - exceeded!
            0.01,
            1000
        );

        assertEquals("should not stream when exceeding max buckets", FlushMode.PER_SHARD, result);
    }

    /**
     * Test decideFlushMode returns PER_SHARD when bucket count is below minimum.
     */
    public void testDecideFlushModeBelowMinBuckets() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 500, 100000);

        FlushMode result = FlushModeResolver.decideFlushMode(
            metrics,
            FlushMode.PER_SHARD,
            100000,
            0.01,
            1000 // min bucket count - not met!
        );

        assertEquals("should not stream when below min buckets", FlushMode.PER_SHARD, result);
    }

    /**
     * Test decideFlushMode returns PER_SHARD when cardinality ratio is too low.
     */
    public void testDecideFlushModeLowCardinalityRatio() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 5000, 1000000);
        // ratio = 5000 / 1000000 = 0.005 < 0.01

        FlushMode result = FlushModeResolver.decideFlushMode(
            metrics,
            FlushMode.PER_SHARD,
            100000,
            0.01, // min cardinality ratio - not met!
            1000
        );

        assertEquals("should not stream with low cardinality ratio", FlushMode.PER_SHARD, result);
    }

    /**
     * Test decideFlushMode returns default when metrics are non-streamable.
     */
    public void testDecideFlushModeNonStreamable() {
        StreamingCostMetrics metrics = StreamingCostMetrics.nonStreamable();

        FlushMode result = FlushModeResolver.decideFlushMode(metrics, FlushMode.PER_SHARD, 100000, 0.01, 1000);

        assertEquals("should return default for non-streamable", FlushMode.PER_SHARD, result);
    }

    /**
     * Test edge case: exactly at bucket count threshold.
     */
    public void testDecideFlushModeExactlyAtMaxBuckets() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 100000, 100000);

        FlushMode result = FlushModeResolver.decideFlushMode(
            metrics,
            FlushMode.PER_SHARD,
            100000, // exactly at max
            0.01,
            1000
        );

        // At exactly max, should still stream (not exceeding)
        assertEquals("should stream when exactly at max buckets", FlushMode.PER_SEGMENT, result);
    }

    /**
     * Test edge case: exactly at min bucket threshold.
     */
    public void testDecideFlushModeExactlyAtMinBuckets() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 1000, 10000);

        FlushMode result = FlushModeResolver.decideFlushMode(
            metrics,
            FlushMode.PER_SHARD,
            100000,
            0.01,
            1000 // exactly at min
        );

        // At exactly min, should stream (not below)
        assertEquals("should stream when exactly at min buckets", FlushMode.PER_SEGMENT, result);
    }

    // ======================================
    // Tests for StreamingCostEstimator methods
    // ======================================

    /**
     * Test estimateOrdinals with normal data.
     */
    public void testEstimateOrdinalsWithNormalData() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add 100 documents with 10 unique terms
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("term_" + (i % 10))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    ValuesSource.Bytes.WithOrdinals valuesSource = createMockOrdinalsValuesSource(reader);

                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateOrdinals(reader, valuesSource, 50);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("TopN size should match shardSize", 50, metrics.topNSize());
                    assertEquals("Should have 10 unique terms", 10, metrics.estimatedBucketCount());
                    assertEquals("Should have 100 docs with field", 100, metrics.estimatedDocCount());
                }
            }
        }
    }

    /**
     * Test estimateOrdinals with empty index.
     */
    public void testEstimateOrdinalsWithEmptyIndex() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create at least one doc to have a valid index, then delete it
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("temp")));
                writer.addDocument(doc);
                writer.deleteAll();
                writer.commit();

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    ValuesSource.Bytes.WithOrdinals valuesSource = createMockOrdinalsValuesSourceEmpty();

                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateOrdinals(reader, valuesSource, 50);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("Should have 0 unique terms", 0, metrics.estimatedBucketCount());
                    assertEquals("Should have 0 docs with field", 0, metrics.estimatedDocCount());
                }
            }
        }
    }

    /**
     * Test estimateOrdinals with multiple segments.
     */
    public void testEstimateOrdinalsWithMultipleSegments() throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMaxBufferedDocs(10); // Force multiple segments
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                // Add documents in batches to create multiple segments
                for (int batch = 0; batch < 3; batch++) {
                    for (int i = 0; i < 20; i++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("field", new BytesRef("term_" + (i % 5))));
                        writer.addDocument(doc);
                    }
                    writer.commit(); // Force a new segment
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    ValuesSource.Bytes.WithOrdinals valuesSource = createMockOrdinalsValuesSource(reader);

                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateOrdinals(reader, valuesSource, 10);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("TopN size should match shardSize", 10, metrics.topNSize());
                    // Max cardinality across segments should be 5 (each segment has 5 unique terms)
                    assertEquals("Should have max 5 unique terms per segment", 5, metrics.estimatedBucketCount());
                    assertEquals("Should have 60 docs with field", 60, metrics.estimatedDocCount());
                }
            }
        }
    }

    /**
     * Test estimateOrdinals handles IOException by returning non-streamable.
     */
    public void testEstimateOrdinalsWithIOException() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("term")));
                writer.addDocument(doc);

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    // Create a mock that throws IOException
                    ValuesSource.Bytes.WithOrdinals valuesSource = mock(ValuesSource.Bytes.WithOrdinals.class);
                    when(valuesSource.ordinalsValues(any(LeafReaderContext.class))).thenThrow(new IOException("Test exception"));

                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateOrdinals(reader, valuesSource, 50);

                    assertFalse("Should be non-streamable on IOException", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test estimateNumericTerms uses doc count as cardinality estimate.
     */
    public void testEstimateNumericTermsUsesDocCount() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add 100 documents
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("term_" + i)));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateNumericTerms(reader, 25);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("TopN size should match", 25, metrics.topNSize());
                    // Uses doc count as cardinality estimate
                    assertEquals("Cardinality estimate should be doc count", 100, metrics.estimatedBucketCount());
                    assertEquals("Should have 100 docs", 100, metrics.estimatedDocCount());
                }
            }
        }
    }

    /**
     * Test estimateNumericTerms with empty index.
     */
    public void testEstimateNumericTermsWithEmptyIndex() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create empty index
                writer.commit();

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateNumericTerms(reader, 10);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("TopN size should match", 10, metrics.topNSize());
                    assertEquals("Cardinality estimate should be 0", 0, metrics.estimatedBucketCount());
                    assertEquals("Should have 0 docs", 0, metrics.estimatedDocCount());
                }
            }
        }
    }

    /**
     * Test estimateOrdinals for cardinality use case (topN = 1).
     */
    public void testEstimateOrdinalsForCardinality() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add 50 documents with 15 unique terms
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("value_" + (i % 15))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    ValuesSource.Bytes.WithOrdinals valuesSource = createMockOrdinalsValuesSource(reader);

                    // Cardinality uses topN=1 (single result)
                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateOrdinals(reader, valuesSource, 1);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("TopN size should be 1 for cardinality", 1, metrics.topNSize());
                    assertEquals("Should have 15 unique values", 15, metrics.estimatedBucketCount());
                    assertEquals("Should have 50 docs with field", 50, metrics.estimatedDocCount());
                }
            }
        }
    }

    /**
     * Test estimateOrdinals for cardinality with empty index.
     */
    public void testEstimateOrdinalsForCardinalityWithEmptyIndex() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("temp")));
                writer.addDocument(doc);
                writer.deleteAll();
                writer.commit();

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    ValuesSource.Bytes.WithOrdinals valuesSource = createMockOrdinalsValuesSourceEmpty();

                    StreamingCostMetrics metrics = StreamingCostEstimator.estimateOrdinals(reader, valuesSource, 1);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertEquals("TopN size should be 1 for cardinality", 1, metrics.topNSize());
                    assertEquals("Should have 0 unique values", 0, metrics.estimatedBucketCount());
                    assertEquals("Should have 0 docs with field", 0, metrics.estimatedDocCount());
                }
            }
        }
    }

    // ======================================
    // Helper methods
    // ======================================

    /**
     * Creates a mock ValuesSource that returns actual SortedSetDocValues from the reader.
     */
    private ValuesSource.Bytes.WithOrdinals createMockOrdinalsValuesSource(IndexReader reader) throws IOException {
        ValuesSource.Bytes.WithOrdinals valuesSource = mock(ValuesSource.Bytes.WithOrdinals.class);
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedSetDocValues docValues = leaf.reader().getSortedSetDocValues("field");
            when(valuesSource.ordinalsValues(leaf)).thenReturn(docValues);
        }
        return valuesSource;
    }

    /**
     * Creates a mock ValuesSource that returns null SortedSetDocValues (empty field).
     */
    private ValuesSource.Bytes.WithOrdinals createMockOrdinalsValuesSourceEmpty() throws IOException {
        ValuesSource.Bytes.WithOrdinals valuesSource = mock(ValuesSource.Bytes.WithOrdinals.class);
        when(valuesSource.ordinalsValues(any(LeafReaderContext.class))).thenReturn(null);
        return valuesSource;
    }

}
