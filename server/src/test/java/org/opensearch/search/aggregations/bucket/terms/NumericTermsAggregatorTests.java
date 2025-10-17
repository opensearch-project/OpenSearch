/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class NumericTermsAggregatorTests extends AggregatorTestCase {
    private static final String LONG_FIELD = "long";

    private static final List<Long> dataset;
    static {
        List<Long> d = new ArrayList<>(45);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < i; j++) {
                d.add((long) i);
            }
        }
        dataset = d;
    }

    public void testMatchNoDocs() throws IOException {

        testSearchCase(
            new MatchNoDocsQuery(),
            dataset,
            aggregation -> aggregation.field(LONG_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            null // without type hint
        );

        testSearchCase(
            new MatchNoDocsQuery(),
            dataset,
            aggregation -> aggregation.field(LONG_FIELD),
            agg -> assertEquals(0, agg.getBuckets().size()),
            ValueType.NUMERIC // with type hint
        );
    }

    public void testMatchAllDocs() throws IOException {
        Query query = new MatchAllDocsQuery();

        testSearchCase(query, dataset, aggregation -> aggregation.field(LONG_FIELD), agg -> {
            assertEquals(9, agg.getBuckets().size());
            for (int i = 0; i < 9; i++) {
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(i);
                assertThat(bucket.getKey(), equalTo(9L - i));
                assertThat(bucket.getDocCount(), equalTo(9L - i));
            }
        },
            null // without type hint
        );

        testSearchCase(query, dataset, aggregation -> aggregation.field(LONG_FIELD), agg -> {
            assertEquals(9, agg.getBuckets().size());
            for (int i = 0; i < 9; i++) {
                LongTerms.Bucket bucket = (LongTerms.Bucket) agg.getBuckets().get(i);
                assertThat(bucket.getKey(), equalTo(9L - i));
                assertThat(bucket.getDocCount(), equalTo(9L - i));
            }
        },
            ValueType.NUMERIC // with type hint
        );
    }

    public void testBadIncludeExclude() throws IOException {
        IncludeExclude includeExclude = new IncludeExclude("foo", null);

        // Numerics don't support any regex include/exclude, so should fail no matter what we do

        AggregationExecutionException e = expectThrows(
            AggregationExecutionException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(LONG_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"),
                null
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Aggregation [_name] cannot support regular expression style "
                    + "include/exclude settings as they can only be applied to string fields. Use an array of numeric "
                    + "values for include/exclude clauses used to filter numeric fields"
            )
        );

        e = expectThrows(
            AggregationExecutionException.class,
            () -> testSearchCase(
                new MatchNoDocsQuery(),
                dataset,
                aggregation -> aggregation.field(LONG_FIELD).includeExclude(includeExclude).format("yyyy-MM-dd"),
                agg -> fail("test should have failed with exception"),
                ValueType.NUMERIC // with type hint
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Aggregation [_name] cannot support regular expression style "
                    + "include/exclude settings as they can only be applied to string fields. Use an array of numeric "
                    + "values for include/exclude clauses used to filter numeric fields"
            )
        );

    }

    public void testNumericTermAggregatorForResultSelectionStrategy() throws IOException {
        List<Long> dataSet = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            dataSet.add(i);
        }

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (Long value : dataSet) {
                    document.add(new SortedNumericDocValuesField(LONG_FIELD, value));
                    document.add(new LongPoint(LONG_FIELD, value));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                MappedFieldType longFieldType = new NumberFieldMapper.NumberFieldType(LONG_FIELD, NumberFieldMapper.NumberType.LONG);

                // Case 1: PriorityQueue selection, when buckets > size && buckets <= 5*size (size=2, buckets=100)
                TermsAggregationBuilder aggregationBuilder1 = new TermsAggregationBuilder("_name").field(LONG_FIELD).size(2);
                aggregationBuilder1.userValueTypeHint(ValueType.NUMERIC);
                aggregationBuilder1.order(org.opensearch.search.aggregations.BucketOrder.count(false)); // count desc
                NumericTermsAggregator aggregator1 = createAggregatorWithCustomizableSearchContext(
                    new MatchAllDocsQuery(),
                    aggregationBuilder1,
                    indexSearcher,
                    createIndexSettings(),
                    new MultiBucketConsumerService.MultiBucketConsumer(
                        DEFAULT_MAX_BUCKETS,
                        new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                    ),
                    searchContext -> when(searchContext.bucketSelectionStrategyFactor()).thenReturn(5),
                    longFieldType
                );
                collectDocuments(indexSearcher, aggregator1);
                aggregator1.buildAggregations(new long[] { 0 });
                assertEquals("priority_queue", aggregator1.getResultSelectionStrategy());

                // Case 2: QuickSelect selection, when buckets > size && buckets > 5*size (size=20, buckets=100)
                TermsAggregationBuilder aggregationBuilder2 = new TermsAggregationBuilder("_name").field(LONG_FIELD).size(20);
                aggregationBuilder2.userValueTypeHint(ValueType.NUMERIC);
                aggregationBuilder2.order(org.opensearch.search.aggregations.BucketOrder.count(false)); // count desc
                NumericTermsAggregator aggregator2 = createAggregatorWithCustomizableSearchContext(
                    new MatchAllDocsQuery(),
                    aggregationBuilder2,
                    indexSearcher,
                    createIndexSettings(),
                    new MultiBucketConsumerService.MultiBucketConsumer(
                        DEFAULT_MAX_BUCKETS,
                        new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                    ),
                    searchContext -> when(searchContext.bucketSelectionStrategyFactor()).thenReturn(5),
                    longFieldType
                );
                collectDocuments(indexSearcher, aggregator2);
                aggregator2.buildAggregations(new long[] { 0 });
                assertEquals("quick_select", aggregator2.getResultSelectionStrategy());

                // Case 3: Get All buckets when buckets <= size (size=110, buckets=100)
                TermsAggregationBuilder aggregationBuilder3 = new TermsAggregationBuilder("_name").field(LONG_FIELD).size(110);
                aggregationBuilder3.userValueTypeHint(ValueType.NUMERIC);
                aggregationBuilder3.order(org.opensearch.search.aggregations.BucketOrder.count(false)); // count desc
                NumericTermsAggregator aggregator3 = createAggregatorWithCustomizableSearchContext(
                    new MatchAllDocsQuery(),
                    aggregationBuilder3,
                    indexSearcher,
                    createIndexSettings(),
                    new MultiBucketConsumerService.MultiBucketConsumer(
                        DEFAULT_MAX_BUCKETS,
                        new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                    ),
                    searchContext -> when(searchContext.bucketSelectionStrategyFactor()).thenReturn(5),
                    longFieldType
                );
                collectDocuments(indexSearcher, aggregator3);
                aggregator3.buildAggregations(new long[] { 0 });
                assertEquals("select_all", aggregator3.getResultSelectionStrategy());
            }
        }
    }

    public void testBucketSelectionStrategyFactorSetting() {
        java.util.Comparator<InternalTerms.Bucket<?>> mockComparator = (b1, b2) -> Long.compare(b2.getDocCount(), b1.getDocCount());

        // Test with factor = 0 (should always use priority_queue)
        BucketSelectionStrategy strategy0 = BucketSelectionStrategy.determine(20, 100L, BucketOrder.count(false), mockComparator, 0);
        assertEquals(BucketSelectionStrategy.PRIORITY_QUEUE, strategy0);

        // default behavior
        BucketSelectionStrategy strategy1 = BucketSelectionStrategy.determine(20, 100L, BucketOrder.count(false), mockComparator, 5);
        assertEquals(BucketSelectionStrategy.QUICK_SELECT_OR_SELECT_ALL, strategy1);

        BucketSelectionStrategy strategy2 = BucketSelectionStrategy.determine(2, 100L, BucketOrder.count(false), mockComparator, 1);
        assertEquals(BucketSelectionStrategy.PRIORITY_QUEUE, strategy2);
    }

    private void testSearchCase(
        Query query,
        List<Long> dataset,
        Consumer<TermsAggregationBuilder> configure,
        Consumer<InternalMappedTerms> verify,
        ValueType valueType
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (Long value : dataset) {
                    document.add(new SortedNumericDocValuesField(LONG_FIELD, value));
                    document.add(new LongPoint(LONG_FIELD, value));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name");
                if (valueType != null) {
                    aggregationBuilder.userValueTypeHint(valueType);
                }
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                MappedFieldType longFieldType = new NumberFieldMapper.NumberFieldType(LONG_FIELD, NumberFieldMapper.NumberType.LONG);

                InternalMappedTerms rareTerms = searchAndReduce(indexSearcher, query, aggregationBuilder, longFieldType);
                verify.accept(rareTerms);
            }
        }
    }

    /**
     * Helper method to collect all documents using the aggregator's leaf collector.
     * This simulates the document collection phase that happens during normal aggregation.
     */
    private void collectDocuments(IndexSearcher searcher, NumericTermsAggregator aggregator) throws IOException {
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            LeafBucketCollector leafCollector = aggregator.getLeafCollector(ctx, LeafBucketCollector.NO_OP_COLLECTOR);
            for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                leafCollector.collect(docId, 0); // collect with bucket ordinal 0 (root bucket)
            }
        }
    }
}
