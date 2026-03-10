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
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Timer;
import org.opensearch.search.profile.aggregation.AggregationProfileBreakdown;
import org.opensearch.search.profile.aggregation.AggregationProfiler;
import org.opensearch.search.profile.aggregation.ProfilingAggregator;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.streaming.FlushMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                    assertThat(result.getBuckets().size(), equalTo(10));

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

    public void testBuildAggregationsWithContextSearcherNoProfile() throws Exception {
        doAggOverManySegments(false);
    }

    public void testBuildAggregationsWithContextSearcherProfile() throws Exception {
        doAggOverManySegments(true);
    }

    private void doAggOverManySegments(boolean profile) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                boolean isSegmented = false;
                for (int i = 0; i < 3; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("common")));
                    indexWriter.addDocument(document);
                    if (rarely()) {
                        indexWriter.flush();
                        isSegmented = true;
                    }
                }
                indexWriter.flush();
                for (int i = 0; i < 2; i++) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("field", new BytesRef("medium")));
                    indexWriter.addDocument(document);
                    if (rarely()) {
                        indexWriter.flush();
                        isSegmented = true;
                    }
                }

                if (!isSegmented) {
                    indexWriter.flush();
                }

                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("rare")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    SearchContext searchContext = createSearchContext(
                        indexSearcher,
                        createIndexSettings(),
                        null,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            MultiBucketConsumerService.DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        new NumberFieldMapper.NumberFieldType("test", NumberFieldMapper.NumberType.INTEGER)
                    );
                    when(searchContext.isStreamSearch()).thenReturn(true);
                    when(searchContext.getFlushMode()).thenReturn(FlushMode.PER_SEGMENT);
                    SearchShardTarget searchShardTarget = new SearchShardTarget(
                        "node_1",
                        new ShardId("foo", "_na_", 1),
                        null,
                        OriginalIndices.NONE
                    );
                    when(searchContext.shardTarget()).thenReturn(searchShardTarget);
                    SearchShardTask task = new SearchShardTask(0, "n/a", "n/a", "test-kind", null, null);
                    searchContext.setTask(task);
                    when(searchContext.queryResult()).thenReturn(new QuerySearchResult());
                    when(searchContext.fetchResult()).thenReturn(new FetchSearchResult());
                    StreamSearchChannelListener listenerMock = mock(StreamSearchChannelListener.class);
                    final List<InternalAggregations> perSegAggs = new ArrayList<>();
                    when(searchContext.getStreamChannelListener()).thenReturn(listenerMock);
                    doAnswer((invok) -> {
                        QuerySearchResult querySearchResult = ((QueryFetchSearchResult) invok.getArgument(0, TransportResponse.class))
                            .queryResult();
                        InternalAggregations internalAggregations = querySearchResult.aggregations().expand();
                        perSegAggs.add(internalAggregations);
                        return null;
                    }).when(listenerMock).onStreamResponse(any(), anyBoolean());
                    ContextIndexSearcher contextIndexSearcher = searchContext.searcher();

                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .order(BucketOrder.count(false));

                    Aggregator aggregator = createStreamAggregator(
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

                    if (profile) {
                        aggregator = wrapByProfilingAgg(aggregator);
                    }

                    aggregator.preCollection();

                    contextIndexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                        new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
                        getMockScriptService(),
                        b -> {},
                        PipelineTree.EMPTY
                    );

                    assertThat(perSegAggs, not(empty()));
                    InternalAggregations summary = InternalAggregations.reduce(perSegAggs, ctx);

                    StringTerms result = summary.get("test");

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

    private static Aggregator wrapByProfilingAgg(Aggregator aggregator) throws IOException {
        AggregationProfiler aggregationProfiler = mock(AggregationProfiler.class);
        AggregationProfileBreakdown aggregationProfileBreakdown = mock(AggregationProfileBreakdown.class);
        when(aggregationProfileBreakdown.getTimer(any())).thenReturn(mock(Timer.class));
        when(aggregationProfiler.getQueryBreakdown(any())).thenReturn(aggregationProfileBreakdown);
        aggregator = new ProfilingAggregator(aggregator, aggregationProfiler);
        return aggregator;
    }

    public void testBuildAggregationsBatchReset() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("test")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("field", new BytesRef("best")));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
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
                    assertThat(firstResult.getBuckets().size(), equalTo(2));

                    aggregator.doReset();

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms secondResult = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];
                    assertThat(secondResult.getBuckets().size(), equalTo(2));
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
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
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

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
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

            StringTerms.Bucket electronicsBucket = terms.getBuckets()
                .stream()
                .filter(bucket -> bucket.getKeyAsString().equals("electronics"))
                .findFirst()
                .orElse(null);
            assertThat(electronicsBucket, notNullValue());
            assertThat(electronicsBucket.getDocCount(), equalTo(2L));

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
            assertThat(electronicsBucket.getDocCount(), equalTo(3L));

            InternalSum totalPrice = electronicsBucket.getAggregations().get("total_price");
            assertThat(totalPrice.getValue(), equalTo(450.0));
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

            assertThat(terms.getBuckets().size(), equalTo(3));

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

                    StringTerms.Bucket firstBucket = buckets.get(0);
                    StringTerms.Bucket secondBucket = buckets.get(1);
                    StringTerms.Bucket thirdBucket = buckets.get(2);

                    assertThat(firstBucket.getDocCount(), equalTo(2L));
                    assertThat(secondBucket.getDocCount(), equalTo(2L));
                    assertThat(thirdBucket.getDocCount(), equalTo(1L));

                    assertThat(thirdBucket.getKeyAsString(), equalTo("clothing"));

                    assertTrue(
                        "First two buckets should be electronics and books",
                        (firstBucket.getKeyAsString().equals("electronics") || firstBucket.getKeyAsString().equals("books"))
                            && (secondBucket.getKeyAsString().equals("electronics") || secondBucket.getKeyAsString().equals("books"))
                            && !firstBucket.getKeyAsString().equals(secondBucket.getKeyAsString())
                    );

                    long totalDocs = buckets.stream().mapToLong(StringTerms.Bucket::getDocCount).sum();
                    assertThat(totalDocs, equalTo(5L));
                }
            }
        }
    }

    public void testThrowOnManySegments() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < atLeast(2); i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("electronics")));
                    indexWriter.addDocument(doc);
                    indexWriter.commit();
                }
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

                    aggregator.preCollection();

                    assertThrows(IllegalStateException.class, () -> { searcher.search(new MatchAllDocsQuery(), aggregator); });
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

    public void testCollectDebugInfo() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig())) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("string", new BytesRef("a")));
                iw.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("string", new BytesRef("b")));
                iw.addDocument(document);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("string");

                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field("string");
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

                // Collect debug info
                Map<String, Object> debugInfo = new HashMap<>();
                BiConsumer<String, Object> debugCollector = debugInfo::put;
                aggregator.collectDebugInfo(debugCollector);

                assertTrue("Should contain result_strategy", debugInfo.containsKey("result_strategy"));
                assertEquals("streaming_terms", debugInfo.get("result_strategy"));

                assertTrue("Should contain segments_with_single_valued_ords", debugInfo.containsKey("segments_with_single_valued_ords"));
                assertTrue("Should contain segments_with_multi_valued_ords", debugInfo.containsKey("segments_with_multi_valued_ords"));
            }
        }
    }

    public void testOrderByMaxSubAggregationDescending() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create 10 categories with varying max values (size=3, total=10)
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                    doc.add(new NumericDocValuesField("value", (i + 1) * 100));
                    indexWriter.addDocument(doc);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .size(3)
                        .shardSize(3)
                        .order(BucketOrder.aggregation("max_value", false))
                        .subAggregation(new MaxAggregationBuilder("max_value").field("value"));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        valueFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(3));

                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_7"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_8"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_9"));

                    Max max0 = buckets.get(0).getAggregations().get("max_value");
                    Max max1 = buckets.get(1).getAggregations().get("max_value");
                    Max max2 = buckets.get(2).getAggregations().get("max_value");
                    assertThat(max0.getValue(), equalTo(800.0));
                    assertThat(max1.getValue(), equalTo(900.0));
                    assertThat(max2.getValue(), equalTo(1000.0));

                    // Verify otherDocCount: 10 categories * 1 doc = 10 total, selected 3*1=3, other=7
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(7L));
                }
            }
        }
    }

    public void testOrderByCardinalitySubAggregationDescending() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create 10 categories with varying cardinality (size=5, total=10)
                for (int i = 0; i < 10; i++) {
                    int uniqueUsers = (i + 1) * 2; // cat_0=2, cat_1=4, ..., cat_9=20
                    for (int j = 0; j < uniqueUsers; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                        doc.add(new SortedSetDocValuesField("user_id", new BytesRef("user_" + (i * 100 + j))));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType userIdFieldType = new KeywordFieldMapper.KeywordFieldType("user_id");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .size(5)
                        .shardSize(5)
                        .order(BucketOrder.aggregation("unique_users", false))
                        .subAggregation(new CardinalityAggregationBuilder("unique_users").field("user_id"));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        userIdFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(5));

                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_5"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_6"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_7"));
                    assertThat(buckets.get(3).getKeyAsString(), equalTo("cat_8"));
                    assertThat(buckets.get(4).getKeyAsString(), equalTo("cat_9"));

                    Cardinality card0 = buckets.get(0).getAggregations().get("unique_users");
                    Cardinality card1 = buckets.get(1).getAggregations().get("unique_users");
                    Cardinality card2 = buckets.get(2).getAggregations().get("unique_users");
                    Cardinality card3 = buckets.get(3).getAggregations().get("unique_users");
                    Cardinality card4 = buckets.get(4).getAggregations().get("unique_users");
                    assertThat(card0.getValue(), equalTo(12L));
                    assertThat(card1.getValue(), equalTo(14L));
                    assertThat(card2.getValue(), equalTo(16L));
                    assertThat(card3.getValue(), equalTo(18L));
                    assertThat(card4.getValue(), equalTo(20L));

                    // Verify otherDocCount: total docs = 2+4+6+8+10+12+14+16+18+20=110, selected=12+14+16+18+20=80, other=30
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(30L));
                }
            }
        }
    }

    public void testOrderByCardinalitySubAggregationAscending() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create 10 categories where TOP 3 by cardinality ASC won't be alphabetically first
                // cat_z=2, cat_y=4, cat_x=6, cat_a=8, cat_b=10, cat_c=12, cat_d=14, cat_e=16, cat_f=18, cat_g=20
                String[] names = { "cat_z", "cat_y", "cat_x", "cat_a", "cat_b", "cat_c", "cat_d", "cat_e", "cat_f", "cat_g" };
                for (int i = 0; i < 10; i++) {
                    int uniqueUsers = (i + 1) * 2;
                    for (int j = 0; j < uniqueUsers; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("category", new BytesRef(names[i])));
                        doc.add(new SortedSetDocValuesField("user_id", new BytesRef("user_" + (i * 100 + j))));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType userIdFieldType = new KeywordFieldMapper.KeywordFieldType("user_id");

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .size(3)
                        .shardSize(3)
                        .order(BucketOrder.aggregation("unique_users", true))
                        .subAggregation(new CardinalityAggregationBuilder("unique_users").field("user_id"));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        userIdFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(3));

                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_x"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_y"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_z"));

                    Cardinality card0 = buckets.get(0).getAggregations().get("unique_users");
                    Cardinality card1 = buckets.get(1).getAggregations().get("unique_users");
                    Cardinality card2 = buckets.get(2).getAggregations().get("unique_users");
                    assertThat(card0.getValue(), equalTo(6L));
                    assertThat(card1.getValue(), equalTo(4L));
                    assertThat(card2.getValue(), equalTo(2L));

                    // Verify otherDocCount: total docs = 2+4+6+8+10+12+14+16+18+20=110, selected=6+4+2=12, other=98
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(98L));
                }
            }
        }
    }

    public void testNoSortOrder() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create categories where alphabetical order != count order
                // cat_z=10 docs (highest), cat_y=9, cat_x=8, cat_a=7, cat_b=6, cat_c=5, cat_d=4, cat_e=3, cat_f=2, cat_g=1 (lowest)
                String[] names = { "cat_z", "cat_y", "cat_x", "cat_a", "cat_b", "cat_c", "cat_d", "cat_e", "cat_f", "cat_g" };
                for (int i = 0; i < 10; i++) {
                    int numDocs = 10 - i;
                    for (int j = 0; j < numDocs; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("category", new BytesRef(names[i])));
                        doc.add(new NumericDocValuesField("value", (i + 1) * 100 + j));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .size(5)
                        .shardSize(5)
                        .subAggregation(new MaxAggregationBuilder("max_value").field("value"));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        valueFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(5));

                    // Default order is count DESC, so top 5 should be cat_z, cat_y, cat_x, cat_a, cat_b (highest counts)
                    // Returned in alphabetical order at shard level
                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_a"));
                    assertThat(buckets.get(0).getDocCount(), equalTo(7L));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_b"));
                    assertThat(buckets.get(1).getDocCount(), equalTo(6L));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_x"));
                    assertThat(buckets.get(2).getDocCount(), equalTo(8L));
                    assertThat(buckets.get(3).getKeyAsString(), equalTo("cat_y"));
                    assertThat(buckets.get(3).getDocCount(), equalTo(9L));
                    assertThat(buckets.get(4).getKeyAsString(), equalTo("cat_z"));
                    assertThat(buckets.get(4).getDocCount(), equalTo(10L));

                    // Verify otherDocCount: total=55 docs (10+9+8+7+6+5+4+3+2+1), selected=40 (10+9+8+7+6), other=15
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(15L));
                }
            }
        }
    }

    public void testOrderByMaxAscending() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create 10 categories where TOP 3 by max ASC won't be alphabetically first
                // cat_z=100, cat_y=200, cat_x=300, cat_a=400, ...
                String[] names = { "cat_z", "cat_y", "cat_x", "cat_a", "cat_b", "cat_c", "cat_d", "cat_e", "cat_f", "cat_g" };
                for (int i = 0; i < 10; i++) {
                    int numDocs = 5;
                    for (int j = 0; j < numDocs; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("category", new BytesRef(names[i])));
                        doc.add(new NumericDocValuesField("value", (i + 1) * 100 + j));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .size(3)
                        .shardSize(3)
                        .order(BucketOrder.aggregation("max_value", true))
                        .subAggregation(new MaxAggregationBuilder("max_value").field("value"));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        valueFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(3));

                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_x"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_y"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_z"));

                    Max max0 = buckets.get(0).getAggregations().get("max_value");
                    Max max1 = buckets.get(1).getAggregations().get("max_value");
                    Max max2 = buckets.get(2).getAggregations().get("max_value");
                    assertThat(max0.getValue(), equalTo(304.0));
                    assertThat(max1.getValue(), equalTo(204.0));
                    assertThat(max2.getValue(), equalTo(104.0));

                    // Verify otherDocCount: 10 categories * 5 docs = 50 total, selected 3*5=15, other=35
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(35L));
                }
            }
        }
    }

    public void testOrderByMaxDescending() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create 10 categories with varying max values
                for (int i = 0; i < 10; i++) {
                    int numDocs = 5;
                    for (int j = 0; j < numDocs; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                        doc.add(new NumericDocValuesField("value", (i + 1) * 100 + j));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");
                    MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.LONG);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category")
                        .size(3)
                        .shardSize(3)
                        .order(BucketOrder.aggregation("max_value", false))
                        .subAggregation(new MaxAggregationBuilder("max_value").field("value"));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType,
                        valueFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(3));

                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_7"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_8"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_9"));

                    Max max0 = buckets.get(0).getAggregations().get("max_value");
                    Max max1 = buckets.get(1).getAggregations().get("max_value");
                    Max max2 = buckets.get(2).getAggregations().get("max_value");
                    assertThat(max0.getValue(), equalTo(804.0));
                    assertThat(max1.getValue(), equalTo(904.0));
                    assertThat(max2.getValue(), equalTo(1004.0));

                    // Verify otherDocCount: 10 categories * 5 docs = 50 total, selected 3*5=15, other=35
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(35L));
                }
            }
        }
    }

    public void testMinDocCount() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create categories with varying doc counts: cat_0=1, cat_1=2, cat_2=3, cat_3=4, cat_4=5
                for (int i = 0; i < 5; i++) {
                    for (int j = 0; j <= i; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("category", new BytesRef("cat_" + i)));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType("category");

                    // Test with minDocCount=3, should only return cat_2, cat_3, cat_4
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("categories").field("category").minDocCount(3);

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        createIndexSettings(),
                        new MultiBucketConsumerService.MultiBucketConsumer(
                            DEFAULT_MAX_BUCKETS,
                            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                        ),
                        categoryFieldType
                    );

                    aggregator.preCollection();
                    assertEquals("strictly single segment", 1, indexSearcher.getIndexReader().leaves().size());
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();

                    StringTerms result = (StringTerms) aggregator.buildAggregations(new long[] { 0 })[0];

                    assertThat(result, notNullValue());
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(3));

                    assertThat(buckets.get(0).getKeyAsString(), equalTo("cat_2"));
                    assertThat(buckets.get(0).getDocCount(), equalTo(3L));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("cat_3"));
                    assertThat(buckets.get(1).getDocCount(), equalTo(4L));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("cat_4"));
                    assertThat(buckets.get(2).getDocCount(), equalTo(5L));

                    // Verify otherDocCount: cat_0=1 + cat_1=2 = 3 docs excluded
                    assertThat(result.getSumOfOtherDocCounts(), equalTo(3L));
                }
            }
        }
    }

    public void testKeyOrderWithSizeLimitDropsCorrectBuckets() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create terms where top 5 by count != first 5 alphabetically
                // Alphabetically: aaa, bbb, ccc, ddd, eee, fff, ggg, hhh, iii, jjj
                // By count: zzz(100), yyy(90), xxx(80), www(70), vvv(60), aaa(50), bbb(40), ccc(30), ddd(20), eee(10)
                String[] terms = { "zzz", "yyy", "xxx", "www", "vvv", "aaa", "bbb", "ccc", "ddd", "eee" };
                int[] counts = { 100, 90, 80, 70, 60, 50, 40, 30, 20, 10 };
                for (int i = 0; i < terms.length; i++) {
                    for (int j = 0; j < counts[i]; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("field", new BytesRef(terms[i])));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    // Request size=5 with key order ascending - should return first 5 alphabetically
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .size(5)
                        .shardSize(5)
                        .order(BucketOrder.key(true));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
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
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(5));

                    // With key order ASC, should return first 5 alphabetically: aaa, bbb, ccc, ddd, eee
                    // NOT the top 5 by doc count (zzz, yyy, xxx, www, vvv)
                    assertThat(buckets.get(0).getKeyAsString(), equalTo("aaa"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("bbb"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("ccc"));
                    assertThat(buckets.get(3).getKeyAsString(), equalTo("ddd"));
                    assertThat(buckets.get(4).getKeyAsString(), equalTo("eee"));
                }
            }
        }
    }

    public void testKeyOrderDescendingWithSizeLimitDropsCorrectBuckets() throws Exception {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                // Create terms where top 5 by count != last 5 alphabetically
                // Alphabetically: aaa, bbb, ccc, ddd, eee, fff, ggg, hhh, iii, jjj
                // By count: jjj(100), iii(90), hhh(80), ggg(70), fff(60), aaa(50), bbb(40), ccc(30), ddd(20), eee(10)
                String[] terms = { "jjj", "iii", "hhh", "ggg", "fff", "aaa", "bbb", "ccc", "ddd", "eee" };
                int[] counts = { 100, 90, 80, 70, 60, 50, 40, 30, 20, 10 };
                for (int i = 0; i < terms.length; i++) {
                    for (int j = 0; j < counts[i]; j++) {
                        Document doc = new Document();
                        doc.add(new SortedSetDocValuesField("field", new BytesRef(terms[i])));
                        indexWriter.addDocument(doc);
                    }
                }

                try (IndexReader indexReader = maybeWrapReaderEs(DirectoryReader.open(indexWriter))) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    // Request size=5 with key order descending - should return last 5 alphabetically
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("test").field("field")
                        .size(5)
                        .shardSize(5)
                        .order(BucketOrder.key(false));

                    IndexSettings indexSettings = new IndexSettings(
                        IndexMetadata.builder("_index")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put("index.aggregation.streaming.min_segment_size", 1)
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .build(),
                        Settings.EMPTY
                    );

                    StreamStringTermsAggregator aggregator = createStreamAggregator(
                        null,
                        aggregationBuilder,
                        indexSearcher,
                        indexSettings,
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
                    List<StringTerms.Bucket> buckets = result.getBuckets();
                    assertThat(buckets.size(), equalTo(5));

                    // With key order DESC, should return last 5 alphabetically: jjj, iii, hhh, ggg, fff
                    // NOT the top 5 by doc count
                    assertThat(buckets.get(0).getKeyAsString(), equalTo("fff"));
                    assertThat(buckets.get(1).getKeyAsString(), equalTo("ggg"));
                    assertThat(buckets.get(2).getKeyAsString(), equalTo("hhh"));
                    assertThat(buckets.get(3).getKeyAsString(), equalTo("iii"));
                    assertThat(buckets.get(4).getKeyAsString(), equalTo("jjj"));
                }
            }
        }
    }
}
