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

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NumericHistogramAggregatorTests extends AggregatorTestCase {

    public void testLongs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testDoubles() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 9.3, 3.2, -10, -6.5, 5.3, 15.1 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testDates() throws Exception {
        List<String> dataset = Arrays.asList(
            "2019-11-01T01:07:45",
            "2019-11-02T03:43:34",
            "2019-11-03T04:11:00",
            "2019-11-04T05:11:31",
            "2019-11-05T08:24:05",
            "2019-11-06T13:09:32",
            "2019-11-07T13:47:43",
            "2019-11-08T16:14:34",
            "2019-11-09T17:09:50",
            "2019-11-10T22:55:46"
        );

        String fieldName = "date_field";
        DateFieldMapper.DateFieldType fieldType = dateField(fieldName, DateFieldMapper.Resolution.MILLISECONDS);

        try (Directory dir = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir)) {
            Document document = new Document();
            for (String date : dataset) {
                long instant = fieldType.parse(date);
                document.add(new SortedNumericDocValuesField(fieldName, instant));
                indexWriter.addDocument(document);
                document.clear();
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(fieldName)
                .interval(1000 * 60 * 60 * 24);
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testIrrationalInterval() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 3, 2, -10, 5, -9 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(Math.PI);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-4 * Math.PI, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-3 * Math.PI, histogram.getBuckets().get(1).getKey());
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());
                assertEquals(-2 * Math.PI, histogram.getBuckets().get(2).getKey());
                assertEquals(0, histogram.getBuckets().get(2).getDocCount());
                assertEquals(-Math.PI, histogram.getBuckets().get(3).getKey());
                assertEquals(0, histogram.getBuckets().get(3).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(4).getKey());
                assertEquals(2, histogram.getBuckets().get(4).getDocCount());
                assertEquals(Math.PI, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMinDocCount() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 50 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(10).minDocCount(2);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(2, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
                w.addDocument(new Document());
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).missing(2d);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(7, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMissingUnmappedField() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                Document doc = new Document();
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).missing(2d);
            MappedFieldType type = null;
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, type);

                assertEquals(1, histogram.getBuckets().size());

                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(7, histogram.getBuckets().get(0).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMissingUnmappedFieldBadType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                w.addDocument(new Document());
            }

            String missingValue = "🍌🍌🍌";
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(5)
                .missing(missingValue);
            MappedFieldType type = null;
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Throwable t = expectThrows(
                    IllegalArgumentException.class,
                    () -> { searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, type); }
                );
                // This throws a number format exception (which is a subclass of IllegalArgumentException) and might be ok?
                assertThat(t.getMessage(), containsString(missingValue));
            }
        }
    }

    public void testIncorrectFieldType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (String value : new String[] { "foo", "bar", "baz", "quux" }) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, keywordField("field"));
                });
            }
        }

    }

    public void testOffset() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 9.3, 3.2, -5, -6.5, 5.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).offset(Math.PI);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(4, histogram.getBuckets().size());
                assertEquals(-10 + Math.PI, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5 + Math.PI, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(Math.PI, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5 + Math.PI, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testRandomOffset() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            // Note, these values are carefully chosen to ensure that no matter what offset we pick, no two can end up in the same bucket
            for (double value : new double[] { 9.3, 3.2, -5 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            final double offset = randomDouble();
            final double interval = 5;
            final double expectedOffset = offset % interval;
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(interval)
                .offset(offset);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(4, histogram.getBuckets().size());

                assertEquals(-10 + expectedOffset, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(-5 + expectedOffset, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());

                assertEquals(expectedOffset, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());

                assertEquals(5 + expectedOffset, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testExtendedBounds() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 3.2, -5, -4.5, 4.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(5)
                .extendedBounds(-12, 13);
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-15d, histogram.getBuckets().get(0).getKey());
                assertEquals(0, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-10d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(5).getKey());
                assertEquals(0, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testAsSubAgg() throws IOException {
        AggregationBuilder request = new HistogramAggregationBuilder("outer").field("outer")
            .interval(5)
            .subAggregation(
                new HistogramAggregationBuilder("inner").field("inner")
                    .interval(5)
                    .subAggregation(new MinAggregationBuilder("min").field("n"))
            );
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            List<List<IndexableField>> docs = new ArrayList<>();
            for (int n = 0; n < 10000; n++) {
                docs.add(
                    List.of(
                        new SortedNumericDocValuesField("outer", n % 100),
                        new SortedNumericDocValuesField("inner", n / 100),
                        new SortedNumericDocValuesField("n", n)
                    )
                );
            }
            iw.addDocuments(docs);
        };
        Consumer<InternalHistogram> verify = outer -> {
            assertThat(outer.getBuckets(), hasSize(20));
            for (int outerIdx = 0; outerIdx < 20; outerIdx++) {
                InternalHistogram.Bucket outerBucket = outer.getBuckets().get(outerIdx);
                assertThat(outerBucket.getKey(), equalTo(5.0 * outerIdx));
                InternalHistogram inner = outerBucket.getAggregations().get("inner");
                assertThat(inner.getBuckets(), hasSize(20));
                for (int innerIdx = 0; innerIdx < 20; innerIdx++) {
                    InternalHistogram.Bucket innerBucket = inner.getBuckets().get(innerIdx);
                    assertThat(innerBucket.getKey(), equalTo(5.0 * innerIdx));
                    InternalMin min = innerBucket.getAggregations().get("min");
                    assertThat(min.getValue(), equalTo(outerIdx * 5.0 + innerIdx * 500.0));
                }
            }
        };
        testCase(request, new MatchAllDocsQuery(), buildIndex, verify, longField("outer"), longField("inner"), longField("n"));
    }

    /**
     * Numeric histogram has no filter-rewrite fast path (it collects doc-by-doc, which is naturally
     * partition-safe under the bulk scorer's doc-id bounds), so it always opts into intra-segment search —
     * with or without a sub-aggregation.
     */
    /**
     * The intra-segment gate for histogram: partition only when the filter-rewrite fast path can NOT apply
     * upfront. It applies for a searchable, top-level histogram on a numeric field -> do not partition. It
     * declines for hard bounds and for a non-searchable field -> partition.
     */
    public void testSupportsIntraSegmentSearch() throws IOException {
        // searchable numeric top-level, no sub-agg: fast path applies -> NOT intra-eligible
        assertFalse(supportsIntraSegmentSearch(new HistogramAggregationBuilder("test").field("field").interval(5), true));

        // with a sub-aggregation the fast path still applies (it collects sub-aggs off the point tree)
        assertFalse(
            supportsIntraSegmentSearch(
                new HistogramAggregationBuilder("test").field("field")
                    .interval(5)
                    .subAggregation(new MinAggregationBuilder("min").field("field")),
                true
            )
        );

        // hard bounds stay on the doc values path -> intra-eligible
        assertTrue(
            supportsIntraSegmentSearch(
                new HistogramAggregationBuilder("test").field("field").interval(5).hardBounds(new DoubleBounds(0.0, 100.0)),
                true
            )
        );

        // non-searchable field: there is no point tree to walk -> intra-eligible
        assertTrue(supportsIntraSegmentSearch(new HistogramAggregationBuilder("test").field("field").interval(5), false));
    }

    private boolean supportsIntraSegmentSearch(HistogramAggregationBuilder builder, boolean searchable) throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(List.of(new SortedNumericDocValuesField("field", 7)));
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    "field",
                    NumberFieldMapper.NumberType.LONG,
                    searchable,
                    false,
                    true,
                    false,
                    true,
                    null,
                    Collections.emptyMap()
                );
                AggregatorFactories factories = AggregatorFactories.builder()
                    .addAggregator(builder)
                    .build(
                        createSearchContext(searcher, createIndexSettings(), new MatchAllDocsQuery(), null, fieldType)
                            .getQueryShardContext(),
                        null
                    );
                return factories.allFactoriesSupportIntraSegmentSearch();
            }
        }
    }

    public void testFilterRewriteLongs() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.LONG,
            new double[] { 7, 3, -10, -6, 5, 15 },
            new MatchAllDocsQuery(),
            aggregation -> aggregation.interval(5),
            histogram -> assertBuckets(histogram, new double[] { -10, -5, 0, 5, 10, 15 }, new long[] { 2, 0, 1, 2, 0, 1 }),
            collectCount -> assertEquals("every bucket should come from the point tree", 0, collectCount.intValue())
        );
    }

    public void testFilterRewriteDoubles() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.DOUBLE,
            new double[] { 9.3, 3.2, -10, -6.5, 5.3, 15.1 },
            new MatchAllDocsQuery(),
            aggregation -> aggregation.interval(5),
            histogram -> assertBuckets(histogram, new double[] { -10, -5, 0, 5, 10, 15 }, new long[] { 2, 0, 1, 2, 0, 1 }),
            collectCount -> assertEquals(0, collectCount.intValue())
        );
    }

    public void testFilterRewriteFloats() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.FLOAT,
            new double[] { 0.5f, 2.5f, 4.5f, 6.5f },
            new MatchAllDocsQuery(),
            aggregation -> aggregation.interval(2),
            histogram -> assertBuckets(histogram, new double[] { 0, 2, 4, 6 }, new long[] { 1, 1, 1, 1 }),
            collectCount -> assertEquals(0, collectCount.intValue())
        );
    }

    public void testFilterRewriteWithOffset() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.LONG,
            new double[] { 0, 1, 2, 3, 4, 5, 6, 7 },
            new MatchAllDocsQuery(),
            aggregation -> aggregation.interval(5).offset(2),
            histogram -> assertBuckets(histogram, new double[] { -3, 2, 7 }, new long[] { 2, 5, 1 }),
            collectCount -> assertEquals(0, collectCount.intValue())
        );
    }

    /**
     * The ranges are cut down to the part of the field the query asks for, so documents outside it are never
     * counted even though the point tree walk sees them.
     */
    public void testFilterRewriteWithRangeQuery() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.LONG,
            new double[] { -3, 0, 2, 4, 6, 8, 12, 20 },
            LongPoint.newRangeQuery("field", 0, 9),
            aggregation -> aggregation.interval(5),
            histogram -> assertBuckets(histogram, new double[] { 0, 5 }, new long[] { 3, 2 }),
            collectCount -> assertEquals(0, collectCount.intValue())
        );

        // a range that no document falls into
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.LONG,
            new double[] { -3, 0, 2, 4, 6, 8, 12, 20 },
            LongPoint.newRangeQuery("field", 100, 200),
            aggregation -> aggregation.interval(5),
            histogram -> assertEquals(0, histogram.getBuckets().size()),
            collectCount -> assertEquals(0, collectCount.intValue())
        );
    }

    /**
     * With an interval a long field cannot land on -- every other bucket boundary falls between two longs --
     * the ranges would not agree with the doc values path, so the optimization steps aside.
     */
    public void testFilterRewriteDeclinesOnBoundaryTheFieldCannotHold() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.LONG,
            new double[] { 0, 1, 2, 3 },
            new MatchAllDocsQuery(),
            aggregation -> aggregation.interval(0.5),
            histogram -> assertBuckets(histogram, new double[] { 0, 0.5, 1, 1.5, 2, 2.5, 3 }, new long[] { 1, 0, 1, 0, 1, 0, 1 }),
            collectCount -> assertEquals("the doc values path should have collected every document", 4, collectCount.intValue())
        );
    }

    /**
     * Hard bounds are applied to {@code key * interval} on the doc values path, which leaves the offset out of
     * it, so the fast path stays away from them rather than reproduce that on a second code path.
     */
    public void testFilterRewriteDeclinesOnHardBounds() throws IOException {
        filterRewriteTestCase(
            NumberFieldMapper.NumberType.LONG,
            new double[] { 0, 5, 10, 15, 20 },
            new MatchAllDocsQuery(),
            aggregation -> aggregation.interval(5).hardBounds(new DoubleBounds(0.0, 11.0)),
            histogram -> assertBuckets(histogram, new double[] { 0, 5, 10 }, new long[] { 1, 1, 1 }),
            collectCount -> assertEquals(5, collectCount.intValue())
        );
    }

    /**
     * The point tree and the doc values path have to agree on every document, including the ones sitting right
     * on a bucket boundary, so run the same random aggregation down both and compare.
     */
    public void testFilterRewriteAgreesWithDocValuesPath() throws IOException {
        for (NumberFieldMapper.NumberType numberType : List.of(
            NumberFieldMapper.NumberType.LONG,
            NumberFieldMapper.NumberType.INTEGER,
            NumberFieldMapper.NumberType.FLOAT,
            NumberFieldMapper.NumberType.DOUBLE
        )) {
            final double[] values = new double[randomIntBetween(1, 200)];
            for (int i = 0; i < values.length; i++) {
                values[i] = numberType == NumberFieldMapper.NumberType.LONG || numberType == NumberFieldMapper.NumberType.INTEGER
                    ? randomIntBetween(-1000, 1000)
                    : randomDoubleBetween(-1000, 1000, true);
            }
            final double interval = randomFrom(0.25, 0.5, 1.0, 2.0, 3.0, 7.0, 10.0, 100.0);
            final double offset = randomFrom(0.0, 1.0, 2.5, -3.0);
            final Consumer<HistogramAggregationBuilder> configure = aggregation -> aggregation.interval(interval).offset(offset);

            final List<InternalHistogram.Bucket> withPointTree = new ArrayList<>();
            final List<InternalHistogram.Bucket> withDocValues = new ArrayList<>();
            filterRewriteTestCase(
                numberType,
                values,
                new MatchAllDocsQuery(),
                configure,
                histogram -> withPointTree.addAll(histogram.getBuckets()),
                collectCount -> {}
            );
            docValuesOnlyTestCase(numberType, values, configure, histogram -> withDocValues.addAll(histogram.getBuckets()));

            final String message = numberType.typeName() + " interval=" + interval + " offset=" + offset;
            assertEquals(message, withDocValues.size(), withPointTree.size());
            for (int i = 0; i < withDocValues.size(); i++) {
                assertEquals(message, withDocValues.get(i).getKey(), withPointTree.get(i).getKey());
                assertEquals(message, withDocValues.get(i).getDocCount(), withPointTree.get(i).getDocCount());
            }
        }
    }

    private void assertBuckets(InternalHistogram histogram, double[] keys, long[] docCounts) {
        assertEquals("bucket count", keys.length, histogram.getBuckets().size());
        for (int i = 0; i < keys.length; i++) {
            assertEquals("key of bucket " + i, keys[i], histogram.getBuckets().get(i).getKey());
            assertEquals("doc count of bucket " + i, docCounts[i], histogram.getBuckets().get(i).getDocCount());
        }
    }

    /**
     * Aggregates over values indexed as both points and doc values, so that the filter rewrite optimization is
     * available, and hands the caller the number of documents that were collected one at a time. A collect
     * count of zero means every bucket was answered off the point tree.
     */
    private void filterRewriteTestCase(
        NumberFieldMapper.NumberType numberType,
        double[] values,
        Query query,
        Consumer<HistogramAggregationBuilder> configure,
        Consumer<InternalHistogram> verify,
        Consumer<Integer> verifyCollectCount
    ) throws IOException {
        histogramTestCase(numberType, values, query, configure, verify, verifyCollectCount, true);
    }

    /**
     * The same aggregation with the points left out of the index, which is the doc values path
     */
    private void docValuesOnlyTestCase(
        NumberFieldMapper.NumberType numberType,
        double[] values,
        Consumer<HistogramAggregationBuilder> configure,
        Consumer<InternalHistogram> verify
    ) throws IOException {
        histogramTestCase(
            numberType,
            values,
            new MatchAllDocsQuery(),
            configure,
            verify,
            collectCount -> assertEquals("without points there is no point tree to walk", values.length, collectCount.intValue()),
            false
        );
    }

    private void histogramTestCase(
        NumberFieldMapper.NumberType numberType,
        double[] values,
        Query query,
        Consumer<HistogramAggregationBuilder> configure,
        Consumer<InternalHistogram> verify,
        Consumer<Integer> verifyCollectCount,
        boolean indexPoints
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", numberType);
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (double value : values) {
                    Document document = new Document();
                    addValue(document, numberType, value, indexPoints);
                    indexWriter.addDocument(document);
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                HistogramAggregationBuilder aggregationBuilder = new HistogramAggregationBuilder("_name").field("field");
                configure.accept(aggregationBuilder);

                CountingAggregator aggregator = createCountingAggregator(query, aggregationBuilder, indexSearcher, fieldType);
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
                InternalHistogram topLevel = (InternalHistogram) aggregator.buildTopLevel();
                InternalHistogram histogram = (InternalHistogram) topLevel.reduce(Collections.singletonList(topLevel), context);
                doAssertReducedMultiBucketConsumer(histogram, reduceBucketConsumer);

                verify.accept(histogram);
                verifyCollectCount.accept(aggregator.getCollectCount().get());
            }
        }
    }

    private CountingAggregator createCountingAggregator(
        Query query,
        AggregationBuilder builder,
        IndexSearcher searcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return new CountingAggregator(
            new AtomicInteger(),
            createAggregator(
                query,
                builder,
                searcher,
                new MultiBucketConsumerService.MultiBucketConsumer(
                    DEFAULT_MAX_BUCKETS,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                ),
                fieldTypes
            )
        );
    }

    private void addValue(Document document, NumberFieldMapper.NumberType numberType, double value, boolean indexPoints) {
        switch (numberType) {
            case LONG:
                if (indexPoints) {
                    document.add(new LongPoint("field", (long) value));
                }
                document.add(new SortedNumericDocValuesField("field", (long) value));
                break;
            case INTEGER:
                if (indexPoints) {
                    document.add(new IntPoint("field", (int) value));
                }
                document.add(new SortedNumericDocValuesField("field", (int) value));
                break;
            case FLOAT:
                if (indexPoints) {
                    document.add(new FloatPoint("field", (float) value));
                }
                document.add(new SortedNumericDocValuesField("field", NumericUtils.floatToSortableInt((float) value)));
                break;
            case DOUBLE:
                if (indexPoints) {
                    document.add(new DoublePoint("field", value));
                }
                document.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                break;
            default:
                throw new UnsupportedOperationException(numberType.typeName());
        }
    }
}
