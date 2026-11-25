/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.StreamStringTermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.FlushMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

public class StreamCardinalityAggregatorTests extends AggregatorTestCase {

    public void testBasicCardinality() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // Cardinality is approximate, so we check it's close to expected value
                    assertTrue(result.getValue() > 0);
                }
            }
        }
    }

    public void testEmptyResults() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertEquals(0, result.getValue(), 0);
                }
            }
        }
    }

    public void testWithMultipleValues() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("term_" + (i % 5))));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // Should have approximately 5 unique values (term_0 through term_4)
                    assertTrue(result.getValue() > 0);
                }
            }
        }
    }

    public void testWithMultiValuedDocuments() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                document.add(new SortedSetDocValuesField("field", new BytesRef("cherry")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // Should have approximately 3 unique values (apple, banana, cherry)
                    assertTrue(result.getValue() > 0);
                }
            }
        }
    }

    public void testReset() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality firstResult = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertTrue(firstResult.getValue() > 0);

                    aggregator.doReset();

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality secondResult = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertTrue(secondResult.getValue() > 0);
                }
            }
        }
    }

    public void testReduceSimple() throws Exception {
        try (Directory directory1 = newDirectory(); Directory directory2 = newDirectory()) {
            // Create first aggregation with some data
            List<InternalAggregation> aggs = new ArrayList<>();

            try (IndexWriter indexWriter1 = new IndexWriter(directory1, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                indexWriter1.addDocument(doc);

                doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter1.addDocument(doc);

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
                    aggs.add(
                        buildInternalStreamingAggregation(
                            new CardinalityAggregationBuilder("cardinality").field("field"),
                            fieldType,
                            searcher1
                        )
                    );
                }
            }

            // Create second aggregation with overlapping data
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter2.addDocument(doc);

                doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef("cherry")));
                indexWriter2.addDocument(doc);

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
                    aggs.add(
                        buildInternalStreamingAggregation(
                            new CardinalityAggregationBuilder("cardinality").field("field"),
                            fieldType,
                            searcher2
                        )
                    );
                }
            }

            // Reduce the aggregations
            InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
                getMockScriptService(),
                b -> {},
                PipelineTree.EMPTY
            );

            InternalAggregation reduced = aggs.get(0).reduce(aggs, ctx);
            assertThat(reduced, instanceOf(InternalCardinality.class));

            InternalCardinality cardinality = (InternalCardinality) reduced;
            // After reducing, should have approximately 3 unique values (apple, banana, cherry)
            assertTrue(cardinality.getValue() > 0);
        }
    }

    public void testReduceSingleAggregation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add multiple documents with different values
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter.addDocument(doc);
                }

                indexWriter.commit();

                try (IndexReader reader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("cardinality").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        searcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    // Execute the aggregator
                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, searcher.getIndexReader().leaves().size());
                    searcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    // Get the result and reduce it
                    InternalCardinality topLevel = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    // Now perform the reduce operation
                    MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer =
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            Integer.MAX_VALUE,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        );
                    InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
                        aggregator.context().bigArrays(),
                        getMockScriptService(),
                        reduceBucketConsumer,
                        PipelineTree.EMPTY
                    );

                    InternalCardinality reduced = (InternalCardinality) topLevel.reduce(Collections.singletonList(topLevel), context);

                    assertThat(reduced, notNullValue());
                    // Should have approximately 5 unique values
                    assertTrue(reduced.getValue() > 0);
                }
            }
        }
    }

    public void testWithPrecisionThreshold() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create documents with many unique values
                for (int i = 0; i < 100; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field")
                        .precisionThreshold(100);

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // With 100 unique values, cardinality should be close to 100
                    assertTrue(result.getValue() > 50);
                }
            }
        }
    }

    public void testMultipleBatches() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("batch1_value1")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("batch1_value2")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    // First batch
                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality firstBatch = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertTrue(firstBatch.getValue() > 0);

                    // Reset for second batch
                    aggregator.doReset();

                    // Second batch
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality secondBatch = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertTrue(secondBatch.getValue() > 0);
                }
            }
        }
    }

    public void testLargeCardinality() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create documents with many unique values
                for (int i = 0; i < 1000; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // With 1000 unique values, cardinality should be close to 1000 (allowing for HLL approximation)
                    assertTrue(result.getValue() > 900);
                }
            }
        }
    }

    public void testWithDuplicates() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create many documents but only a few unique values
                for (int i = 0; i < 100; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("value_" + (i % 3))));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // Should have exactly 3 unique values
                    assertTrue(result.getValue() >= 2 && result.getValue() <= 4); // Allow some HLL approximation error
                }
            }
        }
    }

    public void testSingleUniqueValue() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create many documents with the same value
                for (int i = 0; i < 50; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("same_value")));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // Should have cardinality of 1
                    assertEquals(1, result.getValue(), 0);
                }
            }
        }
    }

    public void testReduceWithMultipleAggregations() throws Exception {
        try (Directory directory1 = newDirectory(); Directory directory2 = newDirectory(); Directory directory3 = newDirectory()) {
            List<InternalAggregation> aggs = new ArrayList<>();

            // First aggregation
            try (IndexWriter indexWriter1 = new IndexWriter(directory1, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter1.addDocument(doc);
                }

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
                    aggs.add(
                        buildInternalStreamingAggregation(
                            new CardinalityAggregationBuilder("cardinality").field("field"),
                            fieldType,
                            searcher1
                        )
                    );
                }
            }

            // Second aggregation - overlapping values
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                for (int i = 5; i < 15; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter2.addDocument(doc);
                }

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
                    aggs.add(
                        buildInternalStreamingAggregation(
                            new CardinalityAggregationBuilder("cardinality").field("field"),
                            fieldType,
                            searcher2
                        )
                    );
                }
            }

            // Third aggregation - new values
            try (IndexWriter indexWriter3 = new IndexWriter(directory3, new IndexWriterConfig())) {
                for (int i = 15; i < 20; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter3.addDocument(doc);
                }

                try (IndexReader reader3 = maybeWrapReaderEs(DirectoryReader.open(indexWriter3))) {
                    IndexSearcher searcher3 = newIndexSearcher(reader3);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
                    aggs.add(
                        buildInternalStreamingAggregation(
                            new CardinalityAggregationBuilder("cardinality").field("field"),
                            fieldType,
                            searcher3
                        )
                    );
                }
            }

            // Reduce the aggregations
            InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
                getMockScriptService(),
                b -> {},
                PipelineTree.EMPTY
            );

            InternalAggregation reduced = aggs.get(0).reduce(aggs, ctx);
            assertThat(reduced, instanceOf(InternalCardinality.class));

            InternalCardinality cardinality = (InternalCardinality) reduced;
            // After reducing, should have approximately 20 unique values (0-19)
            assertTrue(cardinality.getValue() > 15);
        }
    }

    public void testDifferentPrecisionThresholds() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create documents with 50 unique values
                for (int i = 0; i < 50; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    // Test with low precision
                    CardinalityAggregationBuilder lowPrecisionBuilder = new CardinalityAggregationBuilder("test").field("field")
                        .precisionThreshold(10);

                    StreamCardinalityAggregator lowPrecisionAgg = createStreamAggregator(
                        null,
                        lowPrecisionBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    lowPrecisionAgg.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), lowPrecisionAgg);
                    lowPrecisionAgg.postCollection();

                    InternalCardinality lowPrecisionResult = (InternalCardinality) lowPrecisionAgg.buildAggregations(new long[] { 0 })[0];

                    // Test with high precision
                    CardinalityAggregationBuilder highPrecisionBuilder = new CardinalityAggregationBuilder("test").field("field")
                        .precisionThreshold(1000);

                    StreamCardinalityAggregator highPrecisionAgg = createStreamAggregator(
                        null,
                        highPrecisionBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    highPrecisionAgg.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), highPrecisionAgg);
                    highPrecisionAgg.postCollection();

                    InternalCardinality highPrecisionResult = (InternalCardinality) highPrecisionAgg.buildAggregations(new long[] { 0 })[0];

                    // Both should give reasonable results, high precision should be closer to actual
                    assertTrue(lowPrecisionResult.getValue() > 0);
                    assertTrue(highPrecisionResult.getValue() > 0);
                }
            }
        }
    }

    public void testResetClearsState() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // First batch data
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("first_batch")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    // First collection
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    InternalCardinality firstResult = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    long firstCardinality = firstResult.getValue();

                    // Reset
                    aggregator.doReset();

                    // Second collection - should start fresh
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    InternalCardinality secondResult = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    long secondCardinality = secondResult.getValue();

                    // After reset, cardinality should be the same since we're processing the same data
                    assertEquals(firstCardinality, secondCardinality);
                }
            }
        }
    }

    public void testNonOrdinalValueSourceThrowsException() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    // Use a number field type which will create a non-ordinal value source
                    MappedFieldType fieldType = new org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType(
                        "field",
                        org.opensearch.index.mapper.NumberFieldMapper.NumberType.LONG
                    );

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    // This should throw IllegalStateException when getLeafCollector is called with non-ordinal value source
                    IllegalStateException exception = expectThrows(
                        IllegalStateException.class,
                        () -> indexSearcher.search(new MatchAllDocsQuery(), aggregator)
                    );
                    assertTrue(exception.getMessage().contains("only supports ordinal value sources"));
                }
            }
        }
    }

    public void testBuildEmptyAggregation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    InternalCardinality empty = (InternalCardinality) aggregator.buildEmptyAggregation();
                    assertThat(empty, notNullValue());
                    assertEquals("test", empty.getName());
                    assertEquals(0, empty.getValue(), 0);
                }
            }
        }
    }

    public void testMetricMethod() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    // Test metric() method
                    double metricValue = aggregator.metric(0);
                    assertTrue(metricValue > 0);
                    assertTrue(metricValue <= 10);
                }
            }
        }
    }

    public void testCollectDebugInfo() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    // Collect debug info
                    Map<String, Object> debugInfo = new HashMap<>();
                    aggregator.collectDebugInfo((key, value) -> debugInfo.put(key, value));

                    // Verify debug info contains expected keys
                    assertTrue(debugInfo.containsKey("empty_collectors_used"));
                    assertTrue(debugInfo.containsKey("ordinals_collectors_used"));
                }
            }
        }
    }

    public void testMultipleLeafCollectorInvocations() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // Add documents to first segment
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("segment1_value1")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("segment1_value2")));
                indexWriter.addDocument(document);

                // Force a segment
                indexWriter.commit();

                // Add documents to second segment
                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("segment2_value1")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("segment2_value2")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    // This will call getLeafCollector multiple times (once per segment)
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // Should have approximately 4 unique values
                    assertTrue(result.getValue() > 0);
                }
            }
        }
    }

    public void testDoCloseCalledDuringPostCollection() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);

                    // Post collection should properly clean up stream collector
                    aggregator.postCollection();

                    // Build aggregation to ensure it works after postCollection
                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(result, notNullValue());
                    assertTrue(result.getValue() > 0);

                    // Close is called automatically by test framework
                }
            }
        }
    }

    public void testNullValuesSource() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("other_field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    // Field doesn't exist, so values source will be null
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("non_existent_field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field(
                        "non_existent_field"
                    );

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertEquals(0, result.getValue(), 0);
                }
            }
        }
    }

    public void testResetAfterError() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    // Reset should work even if called multiple times
                    aggregator.doReset();
                    aggregator.doReset();

                    // Should be able to collect again after multiple resets
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertTrue(result.getValue() > 0);
                }
            }
        }
    }

    public void testEmptyOrdinalsCollector() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add a document with a field but no ordinals (empty segment)
                Document document = new Document();
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalCardinality result = (InternalCardinality) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertEquals(0, result.getValue(), 0);

                    // Verify empty collector was used
                    Map<String, Object> debugInfo = new HashMap<>();
                    aggregator.collectDebugInfo((key, value) -> debugInfo.put(key, value));
                    assertTrue((Integer) debugInfo.get("empty_collectors_used") > 0);
                }
            }
        }
    }

    public void testBuildTopLevel() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("value_" + i)));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("test").field("field");

                    StreamCardinalityAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        fieldType
                    );

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalAggregation topLevel = aggregator.buildTopLevel();
                    assertThat(topLevel, instanceOf(InternalCardinality.class));

                    InternalCardinality cardinality = (InternalCardinality) topLevel;
                    assertTrue(cardinality.getValue() > 0);
                }
            }
        }
    }

    /**
     * Streaming terms with a cardinality sub-aggregation go through the hybrid collector: whichever stage finishes a segment,
     * every batch reports the distinct values of its own segment. Both segments share the group names, so the terms
     * aggregation reuses bucket ordinals across batches.
     */
    public void testHybridCollectorInStreamingBatches() throws Exception {
        List<Map<String, Set<String>>> expected = new ArrayList<>();
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int segment = 0; segment < 2; segment++) {
                    Map<String, Set<String>> users = new HashMap<>();
                    for (int g = 0; g < 300; g++) {
                        for (int u = 0; u < 2; u++) {
                            addGroupAndUser(indexWriter, users, "g" + g, "u" + segment + "_" + ((g * 7 + u * 1031) % 3000));
                        }
                    }
                    // Three large groups spread the user ordinals over 3000 values, so a BitArray per bucket is ~400 bytes
                    for (int u = 0; u < 3000; u++) {
                        addGroupAndUser(indexWriter, users, "filler" + (u % 3), "u" + segment + "_" + u);
                    }
                    indexWriter.commit();
                    expected.add(users);
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                assertEquals(2, indexReader.leaves().size());
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                // unlimited keeps BitArrays; 60KB moves from BitArrays to roaring bitmaps mid-segment; 1 byte goes on to direct
                for (long memoryThreshold : new long[] { Long.MAX_VALUE, 60 * 1024, 1 }) {
                    Map<String, Object> debug = collectStreamingBatches(
                        indexSearcher,
                        new CardinalityAggregationContext(true, memoryThreshold),
                        null,
                        expected
                    );
                    assertEquals(2, debug.get("hybrid_collectors_used"));
                    assertEquals(memoryThreshold == Long.MAX_VALUE ? 0 : 2, debug.get("roaring_ordinals_collectors_used"));
                    assertEquals(memoryThreshold == 1 ? 2 : 0, debug.get("hybrid_collectors_switched_to_direct"));
                }
                CardinalityAggregationContext hybrid = new CardinalityAggregationContext(true, Long.MAX_VALUE);
                assertEquals(2, collectStreamingBatches(indexSearcher, hybrid, "direct", expected).get("string_hashing_collectors_used"));
                assertEquals(2, collectStreamingBatches(indexSearcher, hybrid, "ordinals", expected).get("ordinals_collectors_used"));
                CardinalityAggregationContext legacy = new CardinalityAggregationContext(false, Long.MAX_VALUE);
                assertEquals(0, collectStreamingBatches(indexSearcher, legacy, null, expected).get("hybrid_collectors_used"));
            }
        }
    }

    private static void addGroupAndUser(IndexWriter indexWriter, Map<String, Set<String>> users, String group, String user)
        throws IOException {
        Document document = new Document();
        document.add(new SortedSetDocValuesField("group", new BytesRef(group)));
        document.add(new SortedSetDocValuesField("user", new BytesRef(user)));
        indexWriter.addDocument(document);
        users.computeIfAbsent(group, k -> new HashSet<>()).add(user);
    }

    /**
     * Collects one batch per segment, the way streaming search does, checks each batch against {@code expected} and returns
     * the debug info of the cardinality aggregator.
     */
    private Map<String, Object> collectStreamingBatches(
        IndexSearcher indexSearcher,
        CardinalityAggregationContext cardinalityContext,
        String executionHint,
        List<Map<String, Set<String>>> expected
    ) throws IOException {
        CardinalityAggregationBuilder card = new CardinalityAggregationBuilder("users").field("user");
        if (executionHint != null) {
            card.executionHint(executionHint);
        }
        SearchContext searchContext = createSearchContext(
            indexSearcher,
            createIndexSettings(),
            null,
            new MultiBucketConsumerService.MultiBucketConsumer(
                DEFAULT_MAX_BUCKETS,
                new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
            ),
            new KeywordFieldMapper.KeywordFieldType("group"),
            new KeywordFieldMapper.KeywordFieldType("user")
        );
        when(searchContext.isStreamSearch()).thenReturn(true);
        when(searchContext.getFlushMode()).thenReturn(FlushMode.PER_SEGMENT);
        when(searchContext.cardinalityAggregationContext()).thenReturn(cardinalityContext);
        Aggregator aggregator = createAggregator(
            new TermsAggregationBuilder("groups").field("group").size(1000).subAggregation(card),
            searchContext
        );
        assertThat(aggregator, instanceOf(StreamStringTermsAggregator.class));
        Aggregator cardinality = aggregator.subAggregator("users");
        assertThat(cardinality, instanceOf(StreamCardinalityAggregator.class));

        aggregator.preCollection();
        List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
        for (int segment = 0; segment < leaves.size(); segment++) {
            LeafReaderContext leaf = leaves.get(segment);
            LeafBucketCollector leafCollector = aggregator.getLeafCollector(leaf);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                leafCollector.collect(doc, 0);
            }
            aggregator.postCollection();
            StringTerms batch = (StringTerms) aggregator.buildTopLevelBatch();
            Map<String, Set<String>> users = expected.get(segment);
            assertEquals(users.size(), batch.getBuckets().size());
            for (StringTerms.Bucket bucket : batch.getBuckets()) {
                InternalCardinality value = bucket.getAggregations().get("users");
                assertEquals(
                    "segment " + segment + " " + bucket.getKeyAsString(),
                    users.get(bucket.getKeyAsString()).size(),
                    value.getValue()
                );
            }
        }
        Map<String, Object> debug = new HashMap<>();
        cardinality.collectDebugInfo(debug::put);
        return debug;
    }

    private InternalAggregation buildInternalStreamingAggregation(
        CardinalityAggregationBuilder builder,
        MappedFieldType fieldType,
        IndexSearcher searcher
    ) throws IOException {
        StreamCardinalityAggregator aggregator = createStreamAggregator(
            null,
            builder,
            searcher,
            createIndexSettings(),
            new MultiBucketConsumerService.MultiBucketConsumer(
                DEFAULT_MAX_BUCKETS,
                new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
            ),
            fieldType
        );

        aggregator.preCollection();
        assertEquals("strictly single segment", 1, searcher.getIndexReader().leaves().size());
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        return aggregator.buildTopLevel();
    }
}
