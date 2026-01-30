/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.StreamingCostEstimable;
import org.opensearch.search.streaming.StreamingCostMetrics;

import java.io.IOException;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.mockito.Mockito.when;

/**
 * Tests for factory-level streaming cost estimation.
 *
 * <p>These tests verify that aggregator factories correctly implement
 * {@link StreamingCostEstimable#estimateStreamingCost} and that the
 * {@link AggregatorFactories} tree traversal works correctly.
 */
public class FactoryStreamingCostEstimationTests extends AggregatorTestCase {

    // ========================================
    // TermsAggregatorFactory Tests
    // ========================================

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with string terms (ordinals).
     */
    public void testTermsFactoryEstimateStringTerms() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add 100 documents with 10 unique terms
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + (i % 10))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category").size(5);

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    assertTrue(
                        "TermsAggregatorFactory should implement StreamingCostEstimable",
                        result.factory instanceof StreamingCostEstimable
                    );

                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertTrue("TopN size should be positive", metrics.topNSize() > 0);
                }
            }
        }
    }

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with numeric terms.
     */
    public void testTermsFactoryEstimateNumericTerms() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add 50 documents with numeric values
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("count", i % 10));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("count", NumberFieldMapper.NumberType.INTEGER);

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("count").size(5);

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    assertTrue(
                        "TermsAggregatorFactory should implement StreamingCostEstimable",
                        result.factory instanceof StreamingCostEstimable
                    );

                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Should be streamable", metrics.streamable());
                    assertTrue("TopN size should be positive", metrics.topNSize() > 0);
                }
            }
        }
    }

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with empty index.
     */
    public void testTermsFactoryEstimateEmptyIndex() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create empty index with just schema
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("temp")));
                writer.addDocument(doc);
                writer.deleteAll();
                writer.commit();

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category");

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Should be streamable even with empty index", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with numeric terms and key-based ordering.
     * Numeric terms with key ordering are NOT streamable because numeric streaming aggregators
     * don't support key-based ordering.
     */
    public void testTermsFactoryEstimateNumericTermsWithKeyOrder() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("count", i % 10));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("count", NumberFieldMapper.NumberType.INTEGER);

                    // Numeric terms with key ascending order - should NOT be streamable
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("count")
                        .size(5)
                        .order(BucketOrder.key(true));

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    assertTrue(
                        "TermsAggregatorFactory should implement StreamingCostEstimable",
                        result.factory instanceof StreamingCostEstimable
                    );

                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertFalse("Numeric terms with key order should NOT be streamable", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with numeric terms and key descending order.
     * Should also be non-streamable.
     */
    public void testTermsFactoryEstimateNumericTermsWithKeyDescOrder() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("value", i));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

                    // Numeric terms with key descending order - should NOT be streamable
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("value")
                        .size(5)
                        .order(BucketOrder.key(false));

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertFalse("Numeric terms with key descending order should NOT be streamable", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with string terms and key-based ordering.
     * String/ordinals terms with key ordering ARE streamable (only numeric is rejected).
     */
    public void testTermsFactoryEstimateStringTermsWithKeyOrder() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + (i % 10))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    // String terms with key ascending order - should be streamable
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category")
                        .size(5)
                        .order(BucketOrder.key(true));

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("String terms with key order should be streamable", metrics.streamable());
                    assertTrue("TopN size should be positive", metrics.topNSize() > 0);
                }
            }
        }
    }

    /**
     * Test TermsAggregatorFactory.estimateStreamingCost with numeric terms and count ordering.
     * Numeric terms with count ordering (default) ARE streamable.
     */
    public void testTermsFactoryEstimateNumericTermsWithCountOrder() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("count", i % 10));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("count", NumberFieldMapper.NumberType.INTEGER);

                    // Numeric terms with count descending order (default) - should be streamable
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("count")
                        .size(5)
                        .order(BucketOrder.count(false));

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Numeric terms with count order should be streamable", metrics.streamable());
                    assertTrue("TopN size should be positive", metrics.topNSize() > 0);
                }
            }
        }
    }

    // ========================================
    // CardinalityAggregatorFactory Tests
    // ========================================

    /**
     * Test CardinalityAggregatorFactory.estimateStreamingCost with ordinals.
     */
    public void testCardinalityFactoryEstimateWithOrdinals() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add 80 documents with 20 unique values
                for (int i = 0; i < 80; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("value_" + (i % 20))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder cardinalityBuilder = new CardinalityAggregationBuilder("cardinality").field("field");

                    FactoryAndContext result = createAggregatorFactory(cardinalityBuilder, searcher, fieldType);
                    assertTrue(
                        "CardinalityAggregatorFactory should implement StreamingCostEstimable",
                        result.factory instanceof StreamingCostEstimable
                    );

                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Should be streamable", metrics.streamable());
                    // Cardinality topN is based on HLL precision (1 << precision), not a fixed value
                    assertTrue("TopN size should be positive", metrics.topNSize() > 0);
                }
            }
        }
    }

    /**
     * Test CardinalityAggregatorFactory.estimateStreamingCost with non-ordinals (numeric) returns non-streamable.
     */
    public void testCardinalityFactoryEstimateNonOrdinals() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("number", i));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

                    CardinalityAggregationBuilder cardinalityBuilder = new CardinalityAggregationBuilder("cardinality").field("number");

                    FactoryAndContext result = createAggregatorFactory(cardinalityBuilder, searcher, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertFalse("Should be non-streamable for numeric cardinality", metrics.streamable());
                }
            }
        }
    }

    // ========================================
    // Metric Aggregator Factories Tests
    // ========================================

    /**
     * Test that metric factories (Max, Min, Avg, Sum, ValueCount) return neutral metrics.
     */
    public void testMetricFactoriesReturnNeutralMetrics() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("value", i));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

                    MaxAggregationBuilder maxBuilder = new MaxAggregationBuilder("max").field("value");

                    FactoryAndContext result = createAggregatorFactory(maxBuilder, searcher, fieldType);
                    assertTrue(
                        "MaxAggregatorFactory should implement StreamingCostEstimable",
                        result.factory instanceof StreamingCostEstimable
                    );

                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    // Metric factories return neutral metrics - they don't add buckets
                    assertTrue("Metric aggs should be streamable", metrics.streamable());
                    assertEquals("Should have neutral topN size", 1, metrics.topNSize());
                }
            }
        }
    }

    // ========================================
    // AggregatorFactories Tree Traversal Tests
    // ========================================

    /**
     * Test nested aggregation cost combination: terms -> max.
     */
    public void testNestedAggregationCostCombination() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + (i % 10))));
                    doc.add(new SortedNumericDocValuesField("value", i));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

                    // terms(category) -> max(value)
                    MaxAggregationBuilder maxBuilder = new MaxAggregationBuilder("max").field("value");
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category")
                        .size(5)
                        .subAggregation(maxBuilder);

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, categoryFieldType, valueFieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Nested streamable aggs should be streamable", metrics.streamable());
                    // TopN is multiplied: parent topN * sub-agg topN (which is 1 for neutral/metric aggs)
                    assertTrue("TopN size should be positive", metrics.topNSize() > 0);
                }
            }
        }
    }

    /**
     * Test nested aggregation with non-streamable sub-aggregation: terms -> top_hits.
     */
    public void testNestedAggregationWithNonStreamableSubAgg() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + (i % 5))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    // terms(category) -> top_hits (non-streamable)
                    TopHitsAggregationBuilder topHitsBuilder = new TopHitsAggregationBuilder("top_docs").size(3);
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category")
                        .subAggregation(topHitsBuilder);

                    FactoryAndContext result = createAggregatorFactory(termsBuilder, searcher, fieldType);

                    // We need to test the factory tree traversal, not just the terms factory
                    // The AggregatorFactories.estimateStreamingCostFromFactories should detect the non-streamable sub-agg
                    AggregatorFactories subFactories = result.factory.getSubFactories();
                    AggregatorFactory[] subFactoryArray = subFactories.getFactories();

                    // Verify sub-factory is not StreamingCostEstimable (TopHitsAggregatorFactory)
                    boolean hasNonStreamable = false;
                    for (AggregatorFactory subFactory : subFactoryArray) {
                        if (!(subFactory instanceof StreamingCostEstimable)) {
                            hasNonStreamable = true;
                            break;
                        }
                    }
                    assertTrue("Should have non-streamable sub-factory (TopHitsAggregatorFactory)", hasNonStreamable);
                }
            }
        }
    }

    /**
     * Test sibling aggregation cost combination: terms + cardinality.
     */
    public void testSiblingAggregationCostCombination() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + (i % 10))));
                    doc.add(new SortedSetDocValuesField("brand", new BytesRef("brand_" + (i % 5))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType brandFieldType = new KeywordFieldMapper.KeywordFieldType("brand");

                    // Test that both factories are StreamingCostEstimable
                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category").size(5);
                    CardinalityAggregationBuilder cardinalityBuilder = new CardinalityAggregationBuilder("cardinality").field("brand");

                    FactoryAndContext termsResult = createAggregatorFactory(termsBuilder, searcher, categoryFieldType, brandFieldType);
                    FactoryAndContext cardinalityResult = createAggregatorFactory(
                        cardinalityBuilder,
                        searcher,
                        categoryFieldType,
                        brandFieldType
                    );

                    assertTrue("Terms factory should be StreamingCostEstimable", termsResult.factory instanceof StreamingCostEstimable);
                    assertTrue(
                        "Cardinality factory should be StreamingCostEstimable",
                        cardinalityResult.factory instanceof StreamingCostEstimable
                    );

                    StreamingCostMetrics termsMetrics = ((StreamingCostEstimable) termsResult.factory).estimateStreamingCost(
                        termsResult.searchContext
                    );
                    StreamingCostMetrics cardinalityMetrics = ((StreamingCostEstimable) cardinalityResult.factory).estimateStreamingCost(
                        cardinalityResult.searchContext
                    );

                    // Verify individual metrics
                    assertTrue("Terms should be streamable", termsMetrics.streamable());
                    assertTrue("Cardinality should be streamable", cardinalityMetrics.streamable());

                    // Verify sibling combination logic
                    StreamingCostMetrics combined = termsMetrics.combineWithSibling(cardinalityMetrics);
                    assertTrue("Combined should be streamable", combined.streamable());
                    // Sibling combination adds topN sizes
                    assertTrue("Combined topN should be positive", combined.topNSize() > 0);
                }
            }
        }
    }

    // ========================================
    // Streaming Fallback Tests
    // ========================================

    /**
     * Test that string terms aggregation falls back to non-streamable when cardinality is low.
     * Low cardinality means maxCardinality is less than minEstimatedBucketCount setting.
     */
    public void testTermsFactoryFallbackLowCardinality() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create index with low cardinality (only 5 unique terms)
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + (i % 5))));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category").size(5);

                    // Create context with high minBucketCount so cardinality (5) < minBucketCount (1000)
                    FactoryAndContext result = createAggregatorFactoryWithMinBucketCount(termsBuilder, searcher, 1000L, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertFalse("Low cardinality should NOT be streamable", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test that string terms aggregation falls back to non-streamable for match-all query
     * when majority of segments have no deleted docs (traditional aggregator can use term frequency optimization).
     */
    public void testTermsFactoryFallbackMatchAllWithCleanSegments() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create index with high cardinality (more than default minBucketCount)
                for (int i = 0; i < 5000; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category").size(10);

                    // Create context with MatchAllDocsQuery - should fall back due to term frequency optimization
                    FactoryAndContext result = createAggregatorFactoryWithQuery(termsBuilder, searcher, new MatchAllDocsQuery(), fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertFalse("Match-all with clean segments should NOT be streamable", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test that string terms with match-all but deleted docs IS streamable
     * (term frequency optimization won't work when segments have deletions).
     */
    public void testTermsFactoryStreamableMatchAllWithDeletedDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            // Use IndexWriterConfig that doesn't merge away deletions
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                // Create index with high cardinality in multiple batches to create multiple segments
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 1000; i++) {
                        Document doc = new Document();
                        String catValue = "cat_" + (batch * 1000 + i);
                        doc.add(new SortedSetDocValuesField("category", new BytesRef(catValue)));
                        // Add indexed field for deletion
                        doc.add(new org.apache.lucene.document.StringField("id", catValue, org.apache.lucene.document.Field.Store.NO));
                        writer.addDocument(doc);
                    }
                    writer.commit(); // Create a segment per batch
                }

                // Delete documents from all segments to make them "dirty"
                for (int batch = 0; batch < 5; batch++) {
                    writer.deleteDocuments(new org.apache.lucene.index.Term("id", "cat_" + (batch * 1000)));
                }
                writer.commit();

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category").size(10);

                    // Match-all but with deleted docs - should be streamable
                    FactoryAndContext result = createAggregatorFactoryWithQuery(termsBuilder, searcher, new MatchAllDocsQuery(), fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    // With deletions in all segments, cleanRatio will be 0, so streaming is preferred
                    assertTrue("Match-all with deleted docs should be streamable", metrics.streamable());
                }
            }
        }
    }

    /**
     * Test that non-match-all query with high cardinality IS streamable.
     */
    public void testTermsFactoryStreamableNonMatchAllQuery() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create index with high cardinality
                for (int i = 0; i < 5000; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                    writer.addDocument(doc);
                }

                try (IndexReader reader = DirectoryReader.open(writer)) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("category").size(10);

                    // Non-match-all query (using a term query) - should be streamable even with clean segments
                    Query termQuery = new TermQuery(new Term("category", "cat_100"));
                    FactoryAndContext result = createAggregatorFactoryWithQuery(termsBuilder, searcher, termQuery, fieldType);
                    StreamingCostMetrics metrics = ((StreamingCostEstimable) result.factory).estimateStreamingCost(result.searchContext);

                    assertTrue("Non-match-all query should be streamable", metrics.streamable());
                }
            }
        }
    }

    // ========================================
    // Helper methods
    // ========================================

    private MultiBucketConsumerService.MultiBucketConsumer createBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
    }

    /**
     * Creates an AggregatorFactory from an AggregationBuilder using the test infrastructure.
     * Returns the factory and the SearchContext used to create it.
     * Uses a non-match-all query to avoid triggering the match-all optimization fallback.
     */
    private FactoryAndContext createAggregatorFactory(
        AggregationBuilder aggregationBuilder,
        IndexSearcher searcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        // Use a BooleanQuery wrapper to avoid being detected as match-all
        // This simulates a filtered query scenario
        Query nonMatchAllQuery = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST).build();
        SearchContext searchContext = createSearchContext(
            searcher,
            createIndexSettings(),
            nonMatchAllQuery,
            createBucketConsumer(),
            fieldTypes
        );
        QueryShardContext queryShardContext = searchContext.getQueryShardContext();
        AggregatorFactory factory = aggregationBuilder.rewrite(queryShardContext).build(queryShardContext, null);
        return new FactoryAndContext(factory, searchContext);
    }

    /**
     * Creates an AggregatorFactory with a specific query.
     */
    private FactoryAndContext createAggregatorFactoryWithQuery(
        AggregationBuilder aggregationBuilder,
        IndexSearcher searcher,
        Query query,
        MappedFieldType... fieldTypes
    ) throws IOException {
        SearchContext searchContext = createSearchContext(searcher, createIndexSettings(), query, createBucketConsumer(), fieldTypes);
        QueryShardContext queryShardContext = searchContext.getQueryShardContext();
        AggregatorFactory factory = aggregationBuilder.rewrite(queryShardContext).build(queryShardContext, null);
        return new FactoryAndContext(factory, searchContext);
    }

    /**
     * Creates an AggregatorFactory with a mocked minEstimatedBucketCount.
     */
    private FactoryAndContext createAggregatorFactoryWithMinBucketCount(
        AggregationBuilder aggregationBuilder,
        IndexSearcher searcher,
        long minBucketCount,
        MappedFieldType... fieldTypes
    ) throws IOException {
        SearchContext searchContext = createSearchContext(searcher, createIndexSettings(), null, createBucketConsumer(), fieldTypes);
        // Mock the minEstimatedBucketCount setting
        when(searchContext.getStreamingMinEstimatedBucketCount()).thenReturn(minBucketCount);
        QueryShardContext queryShardContext = searchContext.getQueryShardContext();
        AggregatorFactory factory = aggregationBuilder.rewrite(queryShardContext).build(queryShardContext, null);
        return new FactoryAndContext(factory, searchContext);
    }

    /**
     * Helper class to return both the factory and its SearchContext.
     */
    private static class FactoryAndContext {
        final AggregatorFactory factory;
        final SearchContext searchContext;

        FactoryAndContext(AggregatorFactory factory, SearchContext searchContext) {
            this.factory = factory;
            this.searchContext = searchContext;
        }
    }
}
