/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.stream.StreamNumericTermsAggregator;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class StreamNumericTermsAggregatorTests extends AggregatorTestCase {
    public void testBuildAggregationsBatchDirectBucketCreation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", 1));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", 1));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", 2));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", 3));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.key(true));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<LongTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsNumber().longValue(), equalTo(1L));
                    assertThat(buckets.get(0).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(1).getKeyAsNumber().longValue(), equalTo(2L));
                    assertThat(buckets.get(1).getDocCount(), equalTo(1L));
                    assertThat(buckets.get(2).getKeyAsNumber().longValue(), equalTo(3L));
                    assertThat(buckets.get(2).getDocCount(), equalTo(1L));

                    for (LongTerms.Bucket bucket : buckets) {
                        assertThat(bucket, instanceOf(LongTerms.Bucket.class));
                        assertThat(bucket.getKey(), instanceOf(Long.class));
                        assertThat(bucket.getKeyAsString(), notNullValue());
                    }
                }
            }
        }
    }

    public void testBuildAggregationsBatchEmptyResults() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(0));
                }
            }
        }
    }

    public void testBuildAggregationsBatchWithSingleValuedOrds() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", i % 3));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.count(false));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<LongTerms.Bucket> buckets = result.getBuckets();

                    // value 0 appears in docs 0,3,6,9 = 4 times
                    // value 1 appears in docs 1,4,7 = 3 times
                    // value 2 appears in docs 2,5,8 = 3 times
                    LongTerms.Bucket term0Bucket = buckets.stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 0L)
                        .findFirst()
                        .orElse(null);
                    assertThat(term0Bucket, notNullValue());
                    assertThat(term0Bucket.getDocCount(), equalTo(4L));

                    LongTerms.Bucket term1Bucket = buckets.stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 1L)
                        .findFirst()
                        .orElse(null);
                    assertThat(term1Bucket, notNullValue());
                    assertThat(term1Bucket.getDocCount(), equalTo(3L));

                    LongTerms.Bucket term2Bucket = buckets.stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 2L)
                        .findFirst()
                        .orElse(null);
                    assertThat(term2Bucket, notNullValue());
                    assertThat(term2Bucket.getDocCount(), equalTo(3L));
                }
            }
        }
    }

    public void testBuildAggregationsBatchWithSize() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create fewer unique terms to test size parameter more meaningfully
                for (int i = 0; i < 20; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", i % 10));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field").size(5);

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // For streaming aggregator, size limitation may not be applied at buildAggregations level
                    // but rather handled during the reduce phase. Test that we get all terms for this batch.
                    assertThat(result.getBuckets().size(), equalTo(10));

                    // Verify each term appears exactly twice (20 docs / 10 unique terms)
                    for (LongTerms.Bucket bucket : result.getBuckets()) {
                        assertThat(bucket.getDocCount(), equalTo(2L));
                    }
                }
            }
        }
    }

    public void testBuildAggregationsBatchWithCountOrder() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 3; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", 100));
                    indexWriter.addDocument(document);
                }

                for (int i = 0; i < 2; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", 200));
                    indexWriter.addDocument(document);
                }

                Document document = new Document();
                document.add(new NumericDocValuesField("field", 300));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.count(false));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<LongTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsNumber().longValue(), equalTo(100L));
                    assertThat(buckets.get(0).getDocCount(), equalTo(3L));
                    assertThat(buckets.get(1).getKeyAsNumber().longValue(), equalTo(200L));
                    assertThat(buckets.get(1).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(2).getKeyAsNumber().longValue(), equalTo(300L));
                    assertThat(buckets.get(2).getDocCount(), equalTo(1L));
                }
            }
        }
    }

    public void testBuildAggregationsBatchReset() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", 42));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms firstResult = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(firstResult.getBuckets().size(), equalTo(1));

                    aggregator.doReset();

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    LongTerms secondResult = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(secondResult.getBuckets().size(), equalTo(1));
                    assertThat(secondResult.getBuckets().get(0).getDocCount(), equalTo(1L));
                }
            }
        }
    }

    public void testMultipleBatches() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", 123));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms firstBatch = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(firstBatch.getBuckets().size(), equalTo(1));
                    assertThat(firstBatch.getBuckets().get(0).getKeyAsNumber().longValue(), equalTo(123L));
                }
            }
        }
    }

    public void testSubAggregationWithMax() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new NumericDocValuesField("category", 1));
                document.add(new NumericDocValuesField("price", 100));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("category", 1));
                document.add(new NumericDocValuesField("price", 200));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("category", 2));
                document.add(new NumericDocValuesField("price", 50));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new NumberFieldMapper.NumberFieldType(
                        "category",
                        NumberFieldMapper.NumberType.LONG
                    );
                    MappedFieldType priceFieldType = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .subAggregation(new MaxAggregationBuilder("max_price").field("price"));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        priceFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    LongTerms.Bucket category1Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 1L)
                        .findFirst()
                        .orElse(null);
                    assertThat(category1Bucket, notNullValue());
                    assertThat(category1Bucket.getDocCount(), equalTo(2L));
                    Max maxPrice = category1Bucket.getAggregations().get("max_price");
                    assertThat(maxPrice.getValue(), equalTo(200.0));

                    LongTerms.Bucket category2Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 2L)
                        .findFirst()
                        .orElse(null);
                    assertThat(category2Bucket, notNullValue());
                    assertThat(category2Bucket.getDocCount(), equalTo(1L));
                    maxPrice = category2Bucket.getAggregations().get("max_price");
                    assertThat(maxPrice.getValue(), equalTo(50.0));
                }
            }
        }
    }

    public void testSubAggregationWithSum() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("category", 1));
                document.add(new NumericDocValuesField("sales", 1000));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("category", 1));
                document.add(new NumericDocValuesField("sales", 2000));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("category", 2));
                document.add(new NumericDocValuesField("sales", 500));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new NumberFieldMapper.NumberFieldType(
                        "category",
                        NumberFieldMapper.NumberType.LONG
                    );
                    MappedFieldType salesFieldType = new NumberFieldMapper.NumberFieldType("sales", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .subAggregation(new SumAggregationBuilder("total_sales").field("sales"));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        salesFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    LongTerms.Bucket category1Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 1L)
                        .findFirst()
                        .orElse(null);
                    assertThat(category1Bucket, notNullValue());
                    InternalSum totalSales = category1Bucket.getAggregations().get("total_sales");
                    assertThat(totalSales.getValue(), equalTo(3000.0));

                    LongTerms.Bucket category2Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 2L)
                        .findFirst()
                        .orElse(null);
                    assertThat(category2Bucket, notNullValue());
                    totalSales = category2Bucket.getAggregations().get("total_sales");
                    assertThat(totalSales.getValue(), equalTo(500.0));
                }
            }
        }
    }

    public void testSubAggregationWithAvg() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("product", 100));
                document.add(new NumericDocValuesField("rating", 4));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("product", 100));
                document.add(new NumericDocValuesField("rating", 5));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("product", 200));
                document.add(new NumericDocValuesField("rating", 3));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType productFieldType = new NumberFieldMapper.NumberFieldType("product", NumberFieldMapper.NumberType.LONG);
                    MappedFieldType ratingFieldType = new NumberFieldMapper.NumberFieldType("rating", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("products").field("product")
                        .subAggregation(new AvgAggregationBuilder("avg_rating").field("rating"));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        productFieldType,
                        ratingFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    LongTerms.Bucket product100Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 100L)
                        .findFirst()
                        .orElse(null);
                    assertThat(product100Bucket, notNullValue());
                    Avg avgRating = product100Bucket.getAggregations().get("avg_rating");
                    assertThat(avgRating.getValue(), equalTo(4.5));

                    LongTerms.Bucket product200Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 200L)
                        .findFirst()
                        .orElse(null);
                    assertThat(product200Bucket, notNullValue());
                    avgRating = product200Bucket.getAggregations().get("avg_rating");
                    assertThat(avgRating.getValue(), equalTo(3.0));
                }
            }
        }
    }

    public void testSubAggregationWithMinAndCount() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("store", 1));
                document.add(new NumericDocValuesField("inventory", 100));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("store", 1));
                document.add(new NumericDocValuesField("inventory", 50));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("store", 2));
                document.add(new NumericDocValuesField("inventory", 200));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType storeFieldType = new NumberFieldMapper.NumberFieldType("store", NumberFieldMapper.NumberType.LONG);
                    MappedFieldType inventoryFieldType = new NumberFieldMapper.NumberFieldType(
                        "inventory",
                        NumberFieldMapper.NumberType.LONG
                    );

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("stores").field("store")
                        .subAggregation(new MinAggregationBuilder("min_inventory").field("inventory"))
                        .subAggregation(new ValueCountAggregationBuilder("inventory_count").field("inventory"));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        storeFieldType,
                        inventoryFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    LongTerms.Bucket store1Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 1L)
                        .findFirst()
                        .orElse(null);
                    assertThat(store1Bucket, notNullValue());
                    assertThat(store1Bucket.getDocCount(), equalTo(2L));

                    Min minInventory = store1Bucket.getAggregations().get("min_inventory");
                    assertThat(minInventory.getValue(), equalTo(50.0));

                    ValueCount inventoryCount = store1Bucket.getAggregations().get("inventory_count");
                    assertThat(inventoryCount.getValue(), equalTo(2L));

                    LongTerms.Bucket store2Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 2L)
                        .findFirst()
                        .orElse(null);
                    assertThat(store2Bucket, notNullValue());
                    assertThat(store2Bucket.getDocCount(), equalTo(1L));

                    minInventory = store2Bucket.getAggregations().get("min_inventory");
                    assertThat(minInventory.getValue(), equalTo(200.0));

                    inventoryCount = store2Bucket.getAggregations().get("inventory_count");
                    assertThat(inventoryCount.getValue(), equalTo(1L));
                }
            }
        }
    }

    public void testMultipleSubAggregations() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("region", 1));
                document.add(new NumericDocValuesField("temperature", 25));
                document.add(new NumericDocValuesField("humidity", 60));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("region", 1));
                document.add(new NumericDocValuesField("temperature", 30));
                document.add(new NumericDocValuesField("humidity", 65));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("region", 2));
                document.add(new NumericDocValuesField("temperature", 35));
                document.add(new NumericDocValuesField("humidity", 80));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType regionFieldType = new NumberFieldMapper.NumberFieldType("region", NumberFieldMapper.NumberType.LONG);
                    MappedFieldType tempFieldType = new NumberFieldMapper.NumberFieldType("temperature", NumberFieldMapper.NumberType.LONG);
                    MappedFieldType humidityFieldType = new NumberFieldMapper.NumberFieldType(
                        "humidity",
                        NumberFieldMapper.NumberType.LONG
                    );

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("regions").field("region")
                        .subAggregation(new AvgAggregationBuilder("avg_temp").field("temperature"))
                        .subAggregation(new MaxAggregationBuilder("max_temp").field("temperature"))
                        .subAggregation(new MinAggregationBuilder("min_humidity").field("humidity"))
                        .subAggregation(new SumAggregationBuilder("total_humidity").field("humidity"));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        regionFieldType,
                        tempFieldType,
                        humidityFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    LongTerms.Bucket region1Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 1L)
                        .findFirst()
                        .orElse(null);
                    assertThat(region1Bucket, notNullValue());
                    assertThat(region1Bucket.getDocCount(), equalTo(2L));

                    Avg avgTemp = region1Bucket.getAggregations().get("avg_temp");
                    assertThat(avgTemp.getValue(), equalTo(27.5));

                    Max maxTemp = region1Bucket.getAggregations().get("max_temp");
                    assertThat(maxTemp.getValue(), equalTo(30.0));

                    Min minHumidity = region1Bucket.getAggregations().get("min_humidity");
                    assertThat(minHumidity.getValue(), equalTo(60.0));

                    InternalSum totalHumidity = region1Bucket.getAggregations().get("total_humidity");
                    assertThat(totalHumidity.getValue(), equalTo(125.0));

                    LongTerms.Bucket region2Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().longValue() == 2L)
                        .findFirst()
                        .orElse(null);
                    assertThat(region2Bucket, notNullValue());
                    assertThat(region2Bucket.getDocCount(), equalTo(1L));

                    avgTemp = region2Bucket.getAggregations().get("avg_temp");
                    assertThat(avgTemp.getValue(), equalTo(35.0));

                    maxTemp = region2Bucket.getAggregations().get("max_temp");
                    assertThat(maxTemp.getValue(), equalTo(35.0));

                    minHumidity = region2Bucket.getAggregations().get("min_humidity");
                    assertThat(minHumidity.getValue(), equalTo(80.0));

                    totalHumidity = region2Bucket.getAggregations().get("total_humidity");
                    assertThat(totalHumidity.getValue(), equalTo(80.0));
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
                doc.add(new NumericDocValuesField("category", 1));
                indexWriter1.addDocument(doc);

                doc = new Document();
                doc.add(new NumericDocValuesField("category", 2));
                indexWriter1.addDocument(doc);

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("category", NumberFieldMapper.NumberType.LONG);
                    aggs.add(
                        buildInternalStreamingAggregation(new TermsAggregationBuilder("categories").field("category"), fieldType, searcher1)
                    );
                }
            }

            // Create second aggregation with overlapping data
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("category", 1));
                indexWriter2.addDocument(doc);

                doc = new Document();
                doc.add(new NumericDocValuesField("category", 3));
                indexWriter2.addDocument(doc);

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("category", NumberFieldMapper.NumberType.LONG);
                    aggs.add(
                        buildInternalStreamingAggregation(new TermsAggregationBuilder("categories").field("category"), fieldType, searcher2)
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
            assertThat(reduced, instanceOf(LongTerms.class));

            LongTerms terms = (LongTerms) reduced;
            assertThat(terms.getBuckets().size(), equalTo(3));

            // Check that category 1 bucket has count 2 (from both aggregations)
            LongTerms.Bucket category1Bucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsNumber().longValue() == 1L)
                .findFirst()
                .orElse(null);
            assertThat(category1Bucket, notNullValue());
            assertThat(category1Bucket.getDocCount(), equalTo(2L));

            // Check that categories 2 and 3 buckets each have count 1
            LongTerms.Bucket category2Bucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsNumber().longValue() == 2L)
                .findFirst()
                .orElse(null);
            assertThat(category2Bucket, notNullValue());
            assertThat(category2Bucket.getDocCount(), equalTo(1L));

            LongTerms.Bucket category3Bucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsNumber().longValue() == 3L)
                .findFirst()
                .orElse(null);
            assertThat(category3Bucket, notNullValue());
            assertThat(category3Bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testReduceWithSubAggregations() throws Exception {
        try (Directory directory1 = newDirectory(); Directory directory2 = newDirectory()) {
            List<InternalAggregation> aggs = new ArrayList<>();

            // First aggregation
            try (IndexWriter indexWriter1 = new IndexWriter(directory1, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("category", 1));
                doc.add(new NumericDocValuesField("price", 100));
                indexWriter1.addDocument(doc);

                doc = new Document();
                doc.add(new NumericDocValuesField("category", 1));
                doc.add(new NumericDocValuesField("price", 200));
                indexWriter1.addDocument(doc);

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType categoryFieldType = new NumberFieldMapper.NumberFieldType(
                        "category",
                        NumberFieldMapper.NumberType.LONG
                    );
                    MappedFieldType priceFieldType = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .subAggregation(new SumAggregationBuilder("total_price").field("price"));

                    aggs.add(buildInternalStreamingAggregation(aggregationBuilder, categoryFieldType, priceFieldType, searcher1));
                }
            }

            // Second aggregation
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("category", 1));
                doc.add(new NumericDocValuesField("price", 150));
                indexWriter2.addDocument(doc);

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType categoryFieldType = new NumberFieldMapper.NumberFieldType(
                        "category",
                        NumberFieldMapper.NumberType.LONG
                    );
                    MappedFieldType priceFieldType = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .order(BucketOrder.key(false))
                        .subAggregation(new SumAggregationBuilder("total_price").field("price"));

                    aggs.add(buildInternalStreamingAggregation(aggregationBuilder, categoryFieldType, priceFieldType, searcher2));
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
            assertThat(reduced, instanceOf(LongTerms.class));

            LongTerms terms = (LongTerms) reduced;
            assertThat(terms.getBuckets().size(), equalTo(1));

            LongTerms.Bucket category1Bucket = terms.getBuckets().get(0);
            assertThat(category1Bucket.getKeyAsNumber().longValue(), equalTo(1L));
            assertThat(category1Bucket.getDocCount(), equalTo(3L)); // 2 from first + 1 from second

            // Check that sub-aggregation values are properly reduced
            InternalSum totalPrice = category1Bucket.getAggregations().get("total_price");
            assertThat(totalPrice.getValue(), equalTo(450.0)); // 100 + 200 + 150
        }
    }

    public void testReduceWithSizeLimit() throws Exception {
        try (Directory directory1 = newDirectory(); Directory directory2 = newDirectory()) {
            List<InternalAggregation> aggs = new ArrayList<>();

            // First aggregation with multiple terms
            try (IndexWriter indexWriter1 = new IndexWriter(directory1, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField("category", i));
                    indexWriter1.addDocument(doc);
                }

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("category", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category").size(3);

                    aggs.add(buildInternalStreamingAggregation(aggregationBuilder, fieldType, searcher1));
                }
            }

            // Second aggregation with different terms
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                for (int i = 3; i < 8; i++) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField("category", i));
                    indexWriter2.addDocument(doc);
                }

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("category", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category").size(3);

                    aggs.add(buildInternalStreamingAggregation(aggregationBuilder, fieldType, searcher2));
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
            assertThat(reduced, instanceOf(LongTerms.class));

            LongTerms terms = (LongTerms) reduced;

            // Size limit should be applied during reduce phase
            assertThat(terms.getBuckets().size(), equalTo(3));

            // Check that overlapping terms (3, 4) have doc count 2
            for (LongTerms.Bucket bucket : terms.getBuckets()) {
                long key = bucket.getKeyAsNumber().longValue();
                if (key == 3L || key == 4L) {
                    assertThat(bucket.getDocCount(), equalTo(2L));
                } else {
                    assertThat(bucket.getDocCount(), equalTo(1L));
                }
            }
        }
    }

    public void testReduceSingleAggregation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Add multiple documents with different categories to test reduce logic properly
                Document doc1 = new Document();
                doc1.add(new NumericDocValuesField("category", 1));
                indexWriter.addDocument(doc1);

                Document doc2 = new Document();
                doc2.add(new NumericDocValuesField("category", 1));
                indexWriter.addDocument(doc2);

                Document doc3 = new Document();
                doc3.add(new NumericDocValuesField("category", 2));
                indexWriter.addDocument(doc3);

                Document doc4 = new Document();
                doc4.add(new NumericDocValuesField("category", 3));
                indexWriter.addDocument(doc4);

                Document doc5 = new Document();
                doc5.add(new NumericDocValuesField("category", 2));
                indexWriter.addDocument(doc5);

                indexWriter.commit(); // Ensure data is committed before reading

                try (IndexReader reader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("category", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .order(BucketOrder.count(false)); // Order by count descending

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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
                    LongTerms topLevel = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

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

                    LongTerms reduced = (LongTerms) topLevel.reduce(Collections.singletonList(topLevel), context);

                    assertThat(reduced, notNullValue());
                    assertThat(reduced.getBuckets().size(), equalTo(3));

                    List<LongTerms.Bucket> buckets = reduced.getBuckets();

                    // Verify the buckets are sorted by count (descending)
                    // category 1: 2 docs, category 2: 2 docs, category 3: 1 doc
                    LongTerms.Bucket firstBucket = buckets.get(0);
                    LongTerms.Bucket secondBucket = buckets.get(1);
                    LongTerms.Bucket thirdBucket = buckets.get(2);

                    // First two buckets should have count 2 (categories 1 and 2)
                    assertThat(firstBucket.getDocCount(), equalTo(2L));
                    assertThat(secondBucket.getDocCount(), equalTo(2L));
                    assertThat(thirdBucket.getDocCount(), equalTo(1L));

                    // Third bucket should be category 3 with count 1
                    assertThat(thirdBucket.getKeyAsNumber().longValue(), equalTo(3L));

                    // Verify that categories 1 and 2 are the first two (order may vary for equal counts)
                    assertTrue(
                        "First two buckets should be categories 1 and 2",
                        (firstBucket.getKeyAsNumber().longValue() == 1L || firstBucket.getKeyAsNumber().longValue() == 2L)
                            && (secondBucket.getKeyAsNumber().longValue() == 1L || secondBucket.getKeyAsNumber().longValue() == 2L)
                            && !firstBucket.getKeyAsNumber().equals(secondBucket.getKeyAsNumber())
                    );

                    // Verify total document count across all buckets
                    long totalDocs = buckets.stream().mapToLong(LongTerms.Bucket::getDocCount).sum();
                    assertThat(totalDocs, equalTo(5L));
                }
            }
        }
    }

    private InternalAggregation buildInternalStreamingAggregation(
        TermsAggregationBuilder builder,
        MappedFieldType fieldType1,
        IndexSearcher searcher
    ) throws IOException {
        return buildInternalStreamingAggregation(builder, fieldType1, null, searcher);
    }

    private InternalAggregation buildInternalStreamingAggregation(
        TermsAggregationBuilder builder,
        MappedFieldType fieldType1,
        MappedFieldType fieldType2,
        IndexSearcher searcher
    ) throws IOException {
        StreamNumericTermsAggregator aggregator;
        if (fieldType2 != null) {
            aggregator = createStreamAggregator(
                null,
                builder,
                searcher,
                createIndexSettings(),
                new MultiBucketConsumerService.MultiBucketConsumer(
                    DEFAULT_MAX_BUCKETS,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                ),
                fieldType1,
                fieldType2
            );
        } else {
            aggregator = createStreamAggregator(
                null,
                builder,
                searcher,
                createIndexSettings(),
                new MultiBucketConsumerService.MultiBucketConsumer(
                    DEFAULT_MAX_BUCKETS,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                ),
                fieldType1
            );
        }

        aggregator.preCollection();
        assertEquals("strictly single segment", 1, searcher.getIndexReader().leaves().size());
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        return aggregator.buildTopLevel();
    }

    public void testDoubleTermsResults() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", NumericUtils.doubleToSortableLong(1.5)));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", NumericUtils.doubleToSortableLong(2.5)));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", NumericUtils.doubleToSortableLong(1.5)));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.key(true));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    DoubleTerms result = (DoubleTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    List<DoubleTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsNumber().doubleValue(), equalTo(1.5));
                    assertThat(buckets.get(0).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(1).getKeyAsNumber().doubleValue(), equalTo(2.5));
                    assertThat(buckets.get(1).getDocCount(), equalTo(1L));
                }
            }
        }
    }

    public void testDoubleTermsWithSubAggregation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("price", NumericUtils.doubleToSortableLong(9.99)));
                document.add(new NumericDocValuesField("quantity", 10));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("price", NumericUtils.doubleToSortableLong(9.99)));
                document.add(new NumericDocValuesField("quantity", 20));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("price", NumericUtils.doubleToSortableLong(19.99)));
                document.add(new NumericDocValuesField("quantity", 5));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType priceFieldType = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.DOUBLE);
                    MappedFieldType quantityFieldType = new NumberFieldMapper.NumberFieldType(
                        "quantity",
                        NumberFieldMapper.NumberType.LONG
                    );

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("prices").field("price")
                        .subAggregation(new SumAggregationBuilder("total_quantity").field("quantity"));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        priceFieldType,
                        quantityFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    DoubleTerms result = (DoubleTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    DoubleTerms.Bucket price999Bucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsNumber().doubleValue() == 9.99)
                        .findFirst()
                        .orElse(null);
                    assertThat(price999Bucket, notNullValue());
                    assertThat(price999Bucket.getDocCount(), equalTo(2L));

                    InternalSum totalQuantity = price999Bucket.getAggregations().get("total_quantity");
                    assertThat(totalQuantity.getValue(), equalTo(30.0));
                }
            }
        }
    }

    public void testUnsignedLongTermsResults() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", Long.MAX_VALUE));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", Long.MAX_VALUE - 1));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", Long.MAX_VALUE));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.UNSIGNED_LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    UnsignedLongTerms result = (UnsignedLongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    // Verify we have the expected buckets with correct doc counts
                    UnsignedLongTerms.Bucket maxValueBucket = result.getBuckets()
                        .stream()
                        .filter(b -> b.getKeyAsNumber().longValue() == Long.MAX_VALUE)
                        .findFirst()
                        .orElse(null);
                    assertThat(maxValueBucket, notNullValue());
                    assertThat(maxValueBucket.getDocCount(), equalTo(2L));
                }
            }
        }
    }

    public void testMultiValuedField() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("tags", 1));
                document.add(new SortedNumericDocValuesField("tags", 2));
                document.add(new SortedNumericDocValuesField("tags", 3));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedNumericDocValuesField("tags", 2));
                document.add(new SortedNumericDocValuesField("tags", 4));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("tags", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("tags")
                        .order(BucketOrder.key(true));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(4));

                    List<LongTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsNumber().longValue(), equalTo(1L));
                    assertThat(buckets.get(0).getDocCount(), equalTo(1L));
                    assertThat(buckets.get(1).getKeyAsNumber().longValue(), equalTo(2L));
                    assertThat(buckets.get(1).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(2).getKeyAsNumber().longValue(), equalTo(3L));
                    assertThat(buckets.get(2).getDocCount(), equalTo(1L));
                    assertThat(buckets.get(3).getKeyAsNumber().longValue(), equalTo(4L));
                    assertThat(buckets.get(3).getDocCount(), equalTo(1L));
                }
            }
        }
    }

    public void testKeyOrderDescending() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 1; i <= 5; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", i));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.key(false));

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(5));

                    // The order is specified but buckets may not be sorted until reduce phase
                    // Just verify all expected keys are present
                    List<LongTerms.Bucket> buckets = result.getBuckets();
                    for (int i = 1; i <= 5; i++) {
                        long expectedKey = i;
                        boolean found = buckets.stream().anyMatch(b -> b.getKeyAsNumber().longValue() == expectedKey);
                        assertTrue("Expected key " + expectedKey + " to be present", found);
                    }
                }
            }
        }
    }

    public void testDifferentNumberTypes() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", 100));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", 200));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                    NumberFieldMapper.NumberType[] types = {
                        NumberFieldMapper.NumberType.INTEGER,
                        NumberFieldMapper.NumberType.SHORT,
                        NumberFieldMapper.NumberType.BYTE };

                    for (NumberFieldMapper.NumberType type : types) {
                        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", type);

                        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                        StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                        LongTerms result = (LongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                        assertThat(result, notNullValue());
                        assertThat(result.getBuckets().size(), equalTo(2));
                    }
                }
            }
        }
    }

    public void testFloatNumberType() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", NumericUtils.floatToSortableInt(3.14f)));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", NumericUtils.floatToSortableInt(2.71f)));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", NumericUtils.floatToSortableInt(3.14f)));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.FLOAT);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    DoubleTerms result = (DoubleTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));
                }
            }
        }
    }

    public void testEmptyDoubleTermsResult() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    DoubleTerms result = (DoubleTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(0));
                }
            }
        }
    }

    public void testEmptyUnsignedLongTermsResult() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.UNSIGNED_LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    UnsignedLongTerms result = (UnsignedLongTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(0));
                }
            }
        }
    }

    public void testMultipleOwningBucketOrds() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new NumericDocValuesField("field", 1));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new NumericDocValuesField("field", 2));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamNumericTermsAggregator aggregator = createStreamAggregator(
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

                    InternalAggregation[] results = aggregator.buildAggregations(new long[] { 0 });

                    assertThat(results.length, equalTo(1));
                    assertThat(results[0], instanceOf(LongTerms.class));

                    LongTerms result1 = (LongTerms) results[0];
                    assertThat(result1.getBuckets().size(), equalTo(2));
                }
            }
        }
    }
}
