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
import org.apache.lucene.search.IndexSearcher;
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
                    assertEquals("Should have 10 unique terms", 10, metrics.estimatedBucketCount());
                    assertEquals("Should have 100 docs", 100, metrics.estimatedDocCount());
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
                    // Numeric terms use doc count as upper bound for cardinality
                    assertEquals("Cardinality estimate should be doc count", 50, metrics.estimatedBucketCount());
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
                    assertEquals("Should have 0 buckets", 0, metrics.estimatedBucketCount());
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
                    assertEquals("TopN size should be 1 for cardinality", 1, metrics.topNSize());
                    assertEquals("Should have 20 unique values", 20, metrics.estimatedBucketCount());
                    assertEquals("Should have 80 docs", 80, metrics.estimatedDocCount());
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
                    assertEquals("Should have neutral bucket count", 1, metrics.estimatedBucketCount());
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
                    // The parent terms agg has 10 buckets, sub-agg is neutral (1 bucket per parent)
                    // Combined: 10 * 1 = 10 buckets
                    assertEquals("Combined bucket count should reflect nesting", 10, metrics.estimatedBucketCount());
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
                    // Sibling combination uses addition of bucket counts (10 terms + 5 cardinality = 15)
                    assertEquals("Combined buckets should be sum of siblings", 15, combined.estimatedBucketCount());
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
     */
    private FactoryAndContext createAggregatorFactory(
        AggregationBuilder aggregationBuilder,
        IndexSearcher searcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        SearchContext searchContext = createSearchContext(searcher, createIndexSettings(), null, createBucketConsumer(), fieldTypes);
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
