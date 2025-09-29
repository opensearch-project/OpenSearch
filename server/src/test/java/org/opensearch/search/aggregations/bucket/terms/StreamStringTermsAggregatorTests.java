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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
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
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class StreamStringTermsAggregatorTests extends AggregatorTestCase {
    public void testBuildAggregationsBatchDirectBucketCreation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("apple")));
                document.add(new SortedSetDocValuesField("field", new BytesRef("cherry")));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("banana")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.key(true));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsString(), equalTo("apple"));
                    assertThat(buckets.get(0).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("banana"));
                    assertThat(buckets.get(1).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cherry"));
                    assertThat(buckets.get(2).getDocCount(), equalTo(1L));

                    for (StringTerms.Bucket bucket : buckets) {
                        assertThat(bucket, instanceOf(StringTerms.Bucket.class));
                        assertThat(bucket.getKey(), instanceOf(String.class));
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
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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
                    assertThat("strictly single segment", indexSearcher.getIndexReader().leaves().size(), lessThanOrEqualTo(1));
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

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
                    document.add(new SortedSetDocValuesField("field", new BytesRef("term_" + (i % 3))));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.count(false));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<StringTerms.Bucket> buckets = result.getBuckets();

                    // term_0 appears in docs 0,3,6,9 = 4 times
                    // term_1 appears in docs 1,4,7 = 3 times
                    // term_2 appears in docs 2,5,8 = 3 times
                    StringTerms.Bucket term0Bucket = buckets.stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("term_0"))
                        .findFirst()
                        .orElse(null);
                    assertThat(term0Bucket, notNullValue());
                    assertThat(term0Bucket.getDocCount(), equalTo(4L));

                    StringTerms.Bucket term1Bucket = buckets.stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("term_1"))
                        .findFirst()
                        .orElse(null);
                    assertThat(term1Bucket, notNullValue());
                    assertThat(term1Bucket.getDocCount(), equalTo(3L));

                    StringTerms.Bucket term2Bucket = buckets.stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("term_2"))
                        .findFirst()
                        .orElse(null);
                    assertThat(term2Bucket, notNullValue());
                    assertThat(term2Bucket.getDocCount(), equalTo(3L));

                    for (StringTerms.Bucket bucket : buckets) {
                        assertThat(bucket.getKeyAsString().startsWith("term_"), equalTo(true));
                    }
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
                    document.add(new SortedSetDocValuesField("field", new BytesRef("term_" + (i % 10))));
                    indexWriter.addDocument(document);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field").size(5);

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    // For streaming aggregator, size limitation may not be applied at buildAggregations level
                    // but rather handled during the reduce phase. Test that we get all terms for this batch.
                    assertThat(result.getBuckets().size(), equalTo(10));

                    // Verify each term appears exactly twice (20 docs / 10 unique terms)
                    for (StringTerms.Bucket bucket : result.getBuckets()) {
                        assertThat(bucket.getDocCount(), equalTo(2L));
                        assertThat(bucket.getKeyAsString().startsWith("term_"), equalTo(true));
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
                    document.add(new SortedSetDocValuesField("field", new BytesRef("common")));
                    indexWriter.addDocument(document);
                }

                for (int i = 0; i < 2; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("medium")));
                    indexWriter.addDocument(document);
                }

                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("rare")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.count(false));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(3));

                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.get(0).getKeyAsString(), equalTo("common"));
                    assertThat(buckets.get(0).getDocCount(), equalTo(3L));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("medium"));
                    assertThat(buckets.get(1).getDocCount(), equalTo(2L));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("rare"));
                    assertThat(buckets.get(2).getDocCount(), equalTo(1L));
                }
            }
        }
    }

    public void testBuildAggregationsBatchReset() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms firstResult = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(firstResult.getBuckets().size(), equalTo(1));

                    aggregator.doReset();

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms secondResult = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];
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
                document.add(new SortedSetDocValuesField("field", new BytesRef("batch1")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field");

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms firstBatch = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(firstBatch.getBuckets().size(), equalTo(1));
                    assertThat(firstBatch.getBuckets().get(0).getKeyAsString(), equalTo("batch1"));
                }
            }
        }
    }

    public void testSubAggregationWithMax() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                document.add(new NumericDocValuesField("price", 100));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                document.add(new NumericDocValuesField("price", 200));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("category", new BytesRef("books")));
                document.add(new NumericDocValuesField("price", 50));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType priceFieldType = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .subAggregation(new MaxAggregationBuilder("max_price").field("price"));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    StringTerms.Bucket electronicsBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("electronics"))
                        .findFirst()
                        .orElse(null);
                    assertThat(electronicsBucket, notNullValue());
                    assertThat(electronicsBucket.getDocCount(), equalTo(2L));
                    Max maxPrice = electronicsBucket.getAggregations().get("max_price");
                    assertThat(maxPrice.getValue(), equalTo(200.0));

                    StringTerms.Bucket booksBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("books"))
                        .findFirst()
                        .orElse(null);
                    assertThat(booksBucket, notNullValue());
                    assertThat(booksBucket.getDocCount(), equalTo(1L));
                    maxPrice = booksBucket.getAggregations().get("max_price");
                    assertThat(maxPrice.getValue(), equalTo(50.0));
                }
            }
        }
    }

    public void testSubAggregationWithSum() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                document.add(new NumericDocValuesField("sales", 1000));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                document.add(new NumericDocValuesField("sales", 2000));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("category", new BytesRef("books")));
                document.add(new NumericDocValuesField("sales", 500));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType salesFieldType = new NumberFieldMapper.NumberFieldType("sales", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .subAggregation(new SumAggregationBuilder("total_sales").field("sales"));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    StringTerms.Bucket electronicsBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("electronics"))
                        .findFirst()
                        .orElse(null);
                    assertThat(electronicsBucket, notNullValue());
                    InternalSum totalSales = electronicsBucket.getAggregations().get("total_sales");
                    assertThat(totalSales.getValue(), equalTo(3000.0));

                    StringTerms.Bucket booksBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("books"))
                        .findFirst()
                        .orElse(null);
                    assertThat(booksBucket, notNullValue());
                    totalSales = booksBucket.getAggregations().get("total_sales");
                    assertThat(totalSales.getValue(), equalTo(500.0));
                }
            }
        }
    }

    public void testSubAggregationWithAvg() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("product", new BytesRef("laptop")));
                document.add(new NumericDocValuesField("rating", 4));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("product", new BytesRef("laptop")));
                document.add(new NumericDocValuesField("rating", 5));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("product", new BytesRef("phone")));
                document.add(new NumericDocValuesField("rating", 3));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType productFieldType = new KeywordFieldMapper.KeywordFieldType("product");
                    MappedFieldType ratingFieldType = new NumberFieldMapper.NumberFieldType("rating", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("products").field("product")
                        .subAggregation(new AvgAggregationBuilder("avg_rating").field("rating"));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    StringTerms.Bucket laptopBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("laptop"))
                        .findFirst()
                        .orElse(null);
                    assertThat(laptopBucket, notNullValue());
                    Avg avgRating = laptopBucket.getAggregations().get("avg_rating");
                    assertThat(avgRating.getValue(), equalTo(4.5));

                    StringTerms.Bucket phoneBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("phone"))
                        .findFirst()
                        .orElse(null);
                    assertThat(phoneBucket, notNullValue());
                    avgRating = phoneBucket.getAggregations().get("avg_rating");
                    assertThat(avgRating.getValue(), equalTo(3.0));
                }
            }
        }
    }

    public void testSubAggregationWithMinAndCount() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("store", new BytesRef("store_a")));
                document.add(new NumericDocValuesField("inventory", 100));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("store", new BytesRef("store_a")));
                document.add(new NumericDocValuesField("inventory", 50));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("store", new BytesRef("store_b")));
                document.add(new NumericDocValuesField("inventory", 200));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType storeFieldType = new KeywordFieldMapper.KeywordFieldType("store");
                    MappedFieldType inventoryFieldType = new NumberFieldMapper.NumberFieldType(
                        "inventory",
                        NumberFieldMapper.NumberType.LONG
                    );

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("stores").field("store")
                        .subAggregation(new MinAggregationBuilder("min_inventory").field("inventory"))
                        .subAggregation(new ValueCountAggregationBuilder("inventory_count").field("inventory"));

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    StringTerms.Bucket storeABucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("store_a"))
                        .findFirst()
                        .orElse(null);
                    assertThat(storeABucket, notNullValue());
                    assertThat(storeABucket.getDocCount(), equalTo(2L));

                    Min minInventory = storeABucket.getAggregations().get("min_inventory");
                    assertThat(minInventory.getValue(), equalTo(50.0));

                    ValueCount inventoryCount = storeABucket.getAggregations().get("inventory_count");
                    assertThat(inventoryCount.getValue(), equalTo(2L));

                    StringTerms.Bucket storeBBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("store_b"))
                        .findFirst()
                        .orElse(null);
                    assertThat(storeBBucket, notNullValue());
                    assertThat(storeBBucket.getDocCount(), equalTo(1L));

                    minInventory = storeBBucket.getAggregations().get("min_inventory");
                    assertThat(minInventory.getValue(), equalTo(200.0));

                    inventoryCount = storeBBucket.getAggregations().get("inventory_count");
                    assertThat(inventoryCount.getValue(), equalTo(1L));
                }
            }
        }
    }

    public void testMultipleSubAggregations() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("region", new BytesRef("north")));
                document.add(new NumericDocValuesField("temperature", 25));
                document.add(new NumericDocValuesField("humidity", 60));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("region", new BytesRef("north")));
                document.add(new NumericDocValuesField("temperature", 30));
                document.add(new NumericDocValuesField("humidity", 65));
                indexWriter.addDocument(document);

                document = new Document();
                document.add(new SortedSetDocValuesField("region", new BytesRef("south")));
                document.add(new NumericDocValuesField("temperature", 35));
                document.add(new NumericDocValuesField("humidity", 80));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType regionFieldType = new KeywordFieldMapper.KeywordFieldType("region");
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

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    assertThat(result.getBuckets().size(), equalTo(2));

                    StringTerms.Bucket northBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("north"))
                        .findFirst()
                        .orElse(null);
                    assertThat(northBucket, notNullValue());
                    assertThat(northBucket.getDocCount(), equalTo(2L));

                    Avg avgTemp = northBucket.getAggregations().get("avg_temp");
                    assertThat(avgTemp.getValue(), equalTo(27.5));

                    Max maxTemp = northBucket.getAggregations().get("max_temp");
                    assertThat(maxTemp.getValue(), equalTo(30.0));

                    Min minHumidity = northBucket.getAggregations().get("min_humidity");
                    assertThat(minHumidity.getValue(), equalTo(60.0));

                    InternalSum totalHumidity = northBucket.getAggregations().get("total_humidity");
                    assertThat(totalHumidity.getValue(), equalTo(125.0));

                    StringTerms.Bucket southBucket = result.getBuckets()
                        .stream()
                        .filter(bucket -> bucket.getKeyAsString().equals("south"))
                        .findFirst()
                        .orElse(null);
                    assertThat(southBucket, notNullValue());
                    assertThat(southBucket.getDocCount(), equalTo(1L));

                    avgTemp = southBucket.getAggregations().get("avg_temp");
                    assertThat(avgTemp.getValue(), equalTo(35.0));

                    maxTemp = southBucket.getAggregations().get("max_temp");
                    assertThat(maxTemp.getValue(), equalTo(35.0));

                    minHumidity = southBucket.getAggregations().get("min_humidity");
                    assertThat(minHumidity.getValue(), equalTo(80.0));

                    totalHumidity = southBucket.getAggregations().get("total_humidity");
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
                doc.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                indexWriter1.addDocument(doc);

                doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("books")));
                indexWriter1.addDocument(doc);

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    aggs.add(
                        buildInternalStreamingAggregation(new TermsAggregationBuilder("categories").field("category"), fieldType, searcher1)
                    );
                }
            }

            // Create second aggregation with overlapping data
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                indexWriter2.addDocument(doc);

                doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("clothing")));
                indexWriter2.addDocument(doc);

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");
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
            assertThat(reduced, instanceOf(StringTerms.class));

            StringTerms terms = (StringTerms) reduced;
            assertThat(terms.getBuckets().size(), equalTo(3));

            // Check that electronics bucket has count 2 (from both aggregations)
            StringTerms.Bucket electronicsBucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsString().equals("electronics"))
                .findFirst()
                .orElse(null);
            assertThat(electronicsBucket, notNullValue());
            assertThat(electronicsBucket.getDocCount(), equalTo(2L));

            // Check that books and clothing buckets each have count 1
            StringTerms.Bucket booksBucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsString().equals("books"))
                .findFirst()
                .orElse(null);
            assertThat(booksBucket, notNullValue());
            assertThat(booksBucket.getDocCount(), equalTo(1L));

            StringTerms.Bucket clothingBucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsString().equals("clothing"))
                .findFirst()
                .orElse(null);
            assertThat(clothingBucket, notNullValue());
            assertThat(clothingBucket.getDocCount(), equalTo(1L));
        }
    }

    public void testReduceWithSubAggregations() throws Exception {
        try (Directory directory1 = newDirectory(); Directory directory2 = newDirectory()) {
            List<InternalAggregation> aggs = new ArrayList<>();

            // First aggregation
            try (IndexWriter indexWriter1 = new IndexWriter(directory1, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                doc.add(new NumericDocValuesField("price", 100));
                indexWriter1.addDocument(doc);

                doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                doc.add(new NumericDocValuesField("price", 200));
                indexWriter1.addDocument(doc);

                doc = new Document();
                String anotherCategory = "clashing value to break on segments";
                assertThat(anotherCategory, lessThan("electronics"));
                doc.add(new SortedSetDocValuesField("category", new BytesRef(anotherCategory)));
                doc.add(new NumericDocValuesField("price", Long.MAX_VALUE));
                indexWriter1.addDocument(doc);

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType priceFieldType = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .subAggregation(new SumAggregationBuilder("total_price").field("price"));

                    aggs.add(buildInternalStreamingAggregation(aggregationBuilder, categoryFieldType, priceFieldType, searcher1));
                }
            }

            // Second aggregation
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                doc.add(new NumericDocValuesField("price", 150));
                indexWriter2.addDocument(doc);

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
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
            assertThat(reduced, instanceOf(StringTerms.class));

            StringTerms terms = (StringTerms) reduced;
            assertThat(terms.getBuckets().size(), equalTo(1 + 1));

            StringTerms.Bucket electronicsBucket = terms.getBuckets().get(0);
            assertThat(electronicsBucket.getKeyAsString(), equalTo("electronics"));
            assertThat(electronicsBucket.getDocCount(), equalTo(3L)); // 2 from first + 1 from second

            // Check that sub-aggregation values are properly reduced
            InternalSum totalPrice = electronicsBucket.getAggregations().get("total_price");
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
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                    indexWriter1.addDocument(doc);
                }

                try (IndexReader reader1 = maybeWrapReaderEs(DirectoryReader.open(indexWriter1))) {
                    IndexSearcher searcher1 = newIndexSearcher(reader1);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category").size(3);

                    aggs.add(buildInternalStreamingAggregation(aggregationBuilder, fieldType, searcher1));
                }
            }

            // Second aggregation with different terms
            try (IndexWriter indexWriter2 = new IndexWriter(directory2, new IndexWriterConfig())) {
                for (int i = 3; i < 8; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                    indexWriter2.addDocument(doc);
                }

                try (IndexReader reader2 = maybeWrapReaderEs(DirectoryReader.open(indexWriter2))) {
                    IndexSearcher searcher2 = newIndexSearcher(reader2);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

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
            assertThat(reduced, instanceOf(StringTerms.class));

            StringTerms terms = (StringTerms) reduced;

            // Size limit should be applied during reduce phase
            assertThat(terms.getBuckets().size(), equalTo(3));

            // Check that overlapping terms (cat_3, cat_4) have doc count 2
            for (StringTerms.Bucket bucket : terms.getBuckets()) {
                if (bucket.getKeyAsString().equals("cat_3") || bucket.getKeyAsString().equals("cat_4")) {
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
                doc1.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                indexWriter.addDocument(doc1);

                Document doc2 = new Document();
                doc2.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                indexWriter.addDocument(doc2);

                Document doc3 = new Document();
                doc3.add(new SortedSetDocValuesField("category", new BytesRef("books")));
                indexWriter.addDocument(doc3);

                Document doc4 = new Document();
                doc4.add(new SortedSetDocValuesField("category", new BytesRef("clothing")));
                indexWriter.addDocument(doc4);

                Document doc5 = new Document();
                doc5.add(new SortedSetDocValuesField("category", new BytesRef("books")));
                indexWriter.addDocument(doc5);

                indexWriter.commit(); // Ensure data is committed before reading

                try (IndexReader reader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .order(BucketOrder.count(false)); // Order by count descending

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
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
                    StringTerms topLevel = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

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

                    StringTerms reduced = (StringTerms) topLevel.reduce(Collections.singletonList(topLevel), context);

                    assertThat(reduced, notNullValue());
                    assertThat(reduced.getBuckets().size(), equalTo(3));

                    List<StringTerms.Bucket> buckets = reduced.getBuckets();

                    // Verify the buckets are sorted by count (descending)
                    // electronics: 2 docs, books: 2 docs, clothing: 1 doc
                    StringTerms.Bucket firstBucket = buckets.get(0);
                    StringTerms.Bucket secondBucket = buckets.get(1);
                    StringTerms.Bucket thirdBucket = buckets.get(2);

                    // First two buckets should have count 2 (electronics and books)
                    assertThat(firstBucket.getDocCount(), equalTo(2L));
                    assertThat(secondBucket.getDocCount(), equalTo(2L));
                    assertThat(thirdBucket.getDocCount(), equalTo(1L));

                    // Third bucket should be clothing with count 1
                    assertThat(thirdBucket.getKeyAsString(), equalTo("clothing"));

                    // Verify that electronics and books are the first two (order may vary for equal counts)
                    assertTrue(
                        "First two buckets should be electronics and books",
                        (firstBucket.getKeyAsString().equals("electronics") || firstBucket.getKeyAsString().equals("books"))
                            && (secondBucket.getKeyAsString().equals("electronics") || secondBucket.getKeyAsString().equals("books"))
                            && !firstBucket.getKeyAsString().equals(secondBucket.getKeyAsString())
                    );

                    // Verify total document count across all buckets
                    long totalDocs = buckets.stream().mapToLong(StringTerms.Bucket::getDocCount).sum();
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
        StreamStringTermsAggregator aggregator;
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
}
