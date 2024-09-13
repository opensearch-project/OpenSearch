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

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.*;
import org.opensearch.search.aggregations.*;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.mockito.Mockito.when;

public class CardinalityAggregatorTests extends AggregatorTestCase {

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testRangeFieldValues() throws IOException {
        RangeType rangeType = RangeType.DOUBLE;
        final RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(rangeType, 6.0D, 10.0D, true, true);
        final String fieldName = "rangeField";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);
        Set<RangeFieldMapper.Range> multiRecord = new HashSet<>(2);
        multiRecord.add(range1);
        multiRecord.add(range2);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range2)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(multiRecord))));
        }, card -> {
            assertEquals(3.0, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, fieldType);
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", 0, 5), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testUnmappedMissingString() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number").missing("🍌🍌🍌");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, null);
    }

    public void testUnmappedMissingNumber() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number").missing(1234);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, null);
    }

    public void testUnmappedMissingGeoPoint() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number")
            .missing(new GeoPoint(42.39561, -71.13051));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, null);
    }

    private void testAggregation(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalCardinality> verify
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalCardinality> verify,
        MappedFieldType fieldType
    ) throws IOException {
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    public void testDynamicPruningDisabledWhenExceedingThreshold() throws IOException {
        final String fieldName = "testField";
        final String filterFieldName = "filterField";

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);

        int randomCardinality = randomIntBetween(20, 100);
        AtomicInteger counter = new AtomicInteger();

        testDynamicPruning(aggregationBuilder, new TermQuery(new Term(filterFieldName, "foo")), iw -> {
            for (int i = 0; i < randomCardinality; i++) {
                String filterValue = "foo";
                if (randomBoolean()) {
                    filterValue = "bar";
                    counter.getAndIncrement();
                }
                iw.addDocument(
                    asList(
                        new KeywordField(filterFieldName, filterValue, Field.Store.NO),
                        new KeywordField(fieldName, String.valueOf(i), Field.Store.NO),
                        new SortedSetDocValuesField(fieldName, new BytesRef(String.valueOf(i)))
                    )
                );
            }
        },
            card -> { assertEquals(randomCardinality - counter.get(), card.getValue(), 0); },
            fieldType,
            10,
            (collectCount) -> assertEquals(randomCardinality - counter.get(), (int) collectCount)
        );
    }

    public void testDynamicPruningFixedValues() throws IOException {
        final String fieldName = "testField";
        final String filterFieldName = "filterField";

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);
        testDynamicPruning(aggregationBuilder, new TermQuery(new Term(filterFieldName, "foo")), iw -> {
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "1", Field.Store.NO),
                    new KeywordField(fieldName, "2", Field.Store.NO),
                    new KeywordField(filterFieldName, "foo", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("1")),
                    new SortedSetDocValuesField(fieldName, new BytesRef("2"))
                )
            );
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "2", Field.Store.NO),
                    new KeywordField(filterFieldName, "foo", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("2"))
                )
            );
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "1", Field.Store.NO),
                    new KeywordField(filterFieldName, "foo", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("1"))
                )
            );
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "2", Field.Store.NO),
                    new KeywordField(filterFieldName, "foo", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("2"))
                )
            );
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "3", Field.Store.NO),
                    new KeywordField(filterFieldName, "foo", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("3"))
                )
            );
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "4", Field.Store.NO),
                    new KeywordField(filterFieldName, "bar", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("4"))
                )
            );
            iw.addDocument(
                asList(
                    new KeywordField(fieldName, "5", Field.Store.NO),
                    new KeywordField(filterFieldName, "bar", Field.Store.NO),
                    new SortedSetDocValuesField(fieldName, new BytesRef("5"))
                )
            );
        }, card -> {
            assertEquals(3.0, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, fieldType, 100, (collectCount) -> assertEquals(0, (int) collectCount));
    }

    public void testDynamicPruningRandomValues() throws IOException {
        final String fieldName = "testField";
        final String filterFieldName = "filterField";

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);

        int randomCardinality = randomIntBetween(1, 100);
        AtomicInteger counter = new AtomicInteger();

        testDynamicPruning(aggregationBuilder, new TermQuery(new Term(filterFieldName, "foo")), iw -> {
            for (int i = 0; i < randomCardinality; i++) {
                String filterValue = "foo";
                if (randomBoolean()) {
                    filterValue = "bar";
                    counter.getAndIncrement();
                }
                iw.addDocument(
                    asList(
                        new KeywordField(filterFieldName, filterValue, Field.Store.NO),
                        new KeywordField(fieldName, String.valueOf(i), Field.Store.NO),
                        new SortedSetDocValuesField(fieldName, new BytesRef(String.valueOf(i)))
                    )
                );
            }
        }, card -> {
            logger.info("expected {}, cardinality: {}", randomCardinality - counter.get(), card.getValue());
            assertEquals(randomCardinality - counter.get(), card.getValue(), 0);
        }, fieldType, 100, (collectCount) -> assertEquals(0, (int) collectCount));
    }

    public void testDynamicPruningRandomDelete() throws IOException {
        final String fieldName = "testField";

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);

        int randomCardinality = randomIntBetween(1, 100);
        AtomicInteger counter = new AtomicInteger();

        testDynamicPruning(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < randomCardinality; i++) {
                iw.addDocument(
                    asList(
                        new KeywordField(fieldName, String.valueOf(i), Field.Store.NO),
                        new SortedSetDocValuesField(fieldName, new BytesRef(String.valueOf(i)))
                    )
                );
                if (randomBoolean()) {
                    iw.deleteDocuments(new Term(fieldName, String.valueOf(i)));
                    counter.getAndIncrement();
                }
            }
        },
            card -> { assertEquals(randomCardinality - counter.get(), card.getValue(), 0); },
            fieldType,
            100,
            (collectCount) -> assertEquals(0, (int) collectCount)
        );
    }

    public void testDynamicPruningFieldMissingInSegment() throws IOException {
        final String fieldName = "testField";
        final String fieldName2 = "testField2";

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);

        int randomNumSegments = randomIntBetween(1, 50);
        logger.info("Indexing [{}] segments", randomNumSegments);

        testDynamicPruning(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < randomNumSegments; i++) {
                iw.addDocument(
                    asList(
                        new KeywordField(fieldName, String.valueOf(i), Field.Store.NO),
                        new SortedSetDocValuesField(fieldName, new BytesRef(String.valueOf(i)))
                    )
                );
                iw.commit();
            }
            iw.addDocument(List.of(new KeywordField(fieldName2, "100", Field.Store.NO)));
            iw.addDocument(List.of(new KeywordField(fieldName2, "101", Field.Store.NO)));
            iw.addDocument(List.of(new KeywordField(fieldName2, "102", Field.Store.NO)));
            iw.commit();
        },
            card -> { assertEquals(randomNumSegments, card.getValue(), 0); },
            fieldType,
            100,
            (collectCount) -> assertEquals(3, (int) collectCount)
        );
    }

    private void testDynamicPruning(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<IndexWriter, IOException> buildIndex,
        Consumer<InternalCardinality> verify,
        MappedFieldType fieldType,
        int pruningThreshold,
        Consumer<Integer> verifyCollectCount
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (
                IndexWriter indexWriter = new IndexWriter(
                    directory,
                    new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setMergePolicy(NoMergePolicy.INSTANCE)
                )
            ) {
                // disable merge so segment number is same as commit times
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                CountingAggregator aggregator = createCountingAggregator(
                    query,
                    aggregationBuilder,
                    indexSearcher,
                    fieldType,
                    pruningThreshold
                );
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();

                MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                    Integer.MAX_VALUE,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );
                InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
                    aggregator.context().bigArrays(),
                    getMockScriptService(),
                    reduceBucketConsumer,
                    PipelineAggregator.PipelineTree.EMPTY
                );
                InternalCardinality topLevel = (InternalCardinality) aggregator.buildTopLevel();
                InternalCardinality card = (InternalCardinality) topLevel.reduce(Collections.singletonList(topLevel), context);
                doAssertReducedMultiBucketConsumer(card, reduceBucketConsumer);

                verify.accept(card);

                logger.info("aggregator collect count {}", aggregator.getCollectCount().get());
                verifyCollectCount.accept(aggregator.getCollectCount().get());
            }
        }
    }

    protected CountingAggregator createCountingAggregator(
        Query query,
        AggregationBuilder builder,
        IndexSearcher searcher,
        MappedFieldType fieldType,
        int pruningThreshold
    ) throws IOException {
        return new CountingAggregator(
            new AtomicInteger(),
            createAggregatorWithCustomizableSearchContext(
                query,
                builder,
                searcher,
                createIndexSettings(),
                new MultiBucketConsumerService.MultiBucketConsumer(
                    DEFAULT_MAX_BUCKETS,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                ),
                (searchContext) -> {
                    when(searchContext.cardinalityAggregationPruningThreshold()).thenReturn(pruningThreshold);
                },
                fieldType
            )
        );
    }

    private void testAggregationExecutionHint(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalCardinality> verify,
        Consumer<LeafBucketCollector> verifyCollector,
        MappedFieldType fieldType
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                CountingAggregator aggregator = new CountingAggregator(new AtomicInteger(), createAggregator(aggregationBuilder, indexSearcher, fieldType));
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();

                MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                    Integer.MAX_VALUE,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );
                InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
                    aggregator.context().bigArrays(),
                    getMockScriptService(),
                    reduceBucketConsumer,
                    PipelineAggregator.PipelineTree.EMPTY
                );
                InternalCardinality topLevel = (InternalCardinality) aggregator.buildTopLevel();
                InternalCardinality card = (InternalCardinality) topLevel.reduce(Collections.singletonList(topLevel), context);
                doAssertReducedMultiBucketConsumer(card, reduceBucketConsumer);

                verify.accept(card);
                verifyCollector.accept(aggregator.getSelectedCollector());
            }
        }
    }

    public void testInvalidExecutionHint() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number").executionHint("invalid");
        assertThrows(IllegalArgumentException.class, () -> testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 9)));
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.DirectCollector);
        }, fieldType));
    }

    public void testNoExecutionHintWithNumericDocValues() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number");
        testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 9)));
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.DirectCollector);
        }, fieldType);
    }

    public void testDirectExecutionHintWithNumericDocValues() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number").executionHint("direct");
        testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 9)));
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.DirectCollector);
        }, fieldType);
    }

    public void testOrdinalsExecutionHintWithNumericDocValues() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number").executionHint("ordinals");
        testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 9)));
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.DirectCollector);
        }, fieldType);
    }

    public void testNoExecutionHintWithByteValues() throws IOException {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("field").executionHint("direct");
        testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField("field", new BytesRef())));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.OrdinalsCollector);
        }, fieldType);
    }

    public void testDirectExecutionHintWithByteValues() throws IOException {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("field").executionHint("direct");
        testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField("field", new BytesRef())));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.OrdinalsCollector);
        }, fieldType);
    }

    public void testOrdinalsExecutionHintWithByteValues() throws IOException {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("field").executionHint("ordinals");
        testAggregationExecutionHint(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField("field", new BytesRef())));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, collector -> {
            assertTrue(collector instanceof CardinalityAggregator.OrdinalsCollector);
        }, fieldType);
    }
}
