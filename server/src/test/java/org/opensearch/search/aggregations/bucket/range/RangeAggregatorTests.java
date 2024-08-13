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

package org.opensearch.search.aggregations.bucket.range;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
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
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;

public class RangeAggregatorTests extends AggregatorTestCase {

    private static final String NUMBER_FIELD_NAME = "number";
    private static final String DATE_FIELD_NAME = "date";

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("bogus_field_name", 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(0, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertFalse(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testTopLevelTermQuery() throws IOException {
        final String KEYWORD_FIELD_NAME = "route";
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(0);
        builder.add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "route1")), BooleanClause.Occur.MUST);
        Query boolQuery = builder.build();

        Document doc1 = new Document();
        Document doc2 = new Document();
        Document doc3 = new Document();
        doc1.add(new NumericDocValuesField(NUMBER_FIELD_NAME, 3));
        doc2.add(new NumericDocValuesField(NUMBER_FIELD_NAME, 11));
        doc3.add(new NumericDocValuesField(NUMBER_FIELD_NAME, 12));
        doc1.add(new KeywordField(KEYWORD_FIELD_NAME, "route1", Field.Store.NO));
        doc2.add(new KeywordField(KEYWORD_FIELD_NAME, "route1", Field.Store.NO));
        doc3.add(new KeywordField(KEYWORD_FIELD_NAME, "route2", Field.Store.NO));

        testCase(boolQuery, iw -> {
            iw.addDocument(doc1);
            iw.addDocument(doc2);
            iw.addDocument(doc3);
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertEquals(1, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testDateFieldMillisecondResolution() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME);

        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field(DATE_FIELD_NAME)
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/57651")
    public void testDateFieldNanosecondResolution() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(
            DATE_FIELD_NAME,
            true,
            false,
            true,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            DateFieldMapper.Resolution.NANOSECONDS,
            null,
            Collections.emptyMap()
        );

        // These values should work because aggs scale nanosecond up to millisecond always.
        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field(DATE_FIELD_NAME)
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/57651")
    public void testMissingDateWithDateField() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(
            DATE_FIELD_NAME,
            true,
            false,
            true,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            DateFieldMapper.Resolution.NANOSECONDS,
            null,
            Collections.emptyMap()
        );

        // These values should work because aggs scale nanosecond up to millisecond always.
        long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field(DATE_FIELD_NAME)
            .missing("2015-11-13T16:14:34")
            .addRange(milli1 - 1, milli1 + 1);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(DATE_FIELD_NAME, milli2)));
            // Missing will apply to this document
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testMissingDateWithNumberField() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field(NUMBER_FIELD_NAME)
            .addRange(-2d, 5d)
            .missing("1979-01-01T00:00:00");

        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithMissingNumber() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field("does_not_exist")
            .addRange(-2d, 5d)
            .missing(0L);

        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testUnmappedWithMissingDate() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field("does_not_exist")
            .addRange(-2d, 5d)
            .missing("2020-02-13T10:11:12");

        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnsupportedType() {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field("not_a_number").addRange(-2d, 5d);

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, range -> fail("Should have thrown exception"), fieldType)
        );
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [range]", e.getMessage());
    }

    public void testBadMissingField() {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field(NUMBER_FIELD_NAME)
            .addRange(-2d, 5d)
            .missing("bogus");

        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field("does_not_exist")
            .addRange(-2d, 5d)
            .missing("bogus");

        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testSubAggCollectsFromSingleBucketIfOneRange() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test").field(NUMBER_FIELD_NAME)
            .addRange(0d, 10d)
            .subAggregation(aggCardinality("c"));

        simpleTestCase(aggregationBuilder, new MatchAllDocsQuery(), range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            InternalAggCardinality pc = ranges.get(0).getAggregations().get("c");
            assertThat(pc.cardinality(), equalTo(CardinalityUpperBound.ONE));
        });
    }

    public void testSubAggCollectsFromManyBucketsIfManyRanges() throws IOException {
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test").field(NUMBER_FIELD_NAME)
            .addRange(0d, 10d)
            .addRange(10d, 100d)
            .subAggregation(aggCardinality("c"));

        simpleTestCase(aggregationBuilder, new MatchAllDocsQuery(), range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            InternalAggCardinality pc = ranges.get(0).getAggregations().get("c");
            assertThat(pc.cardinality().map(i -> i), equalTo(2));
            pc = ranges.get(1).getAggregations().get("c");
            assertThat(pc.cardinality().map(i -> i), equalTo(2));
        });
    }

    public void testOverlappingRanges() throws IOException {
        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.DOUBLE.typeName(), NumberType.DOUBLE),
            new double[][] { { 1, 2 }, { 1, 1.5 }, { 0, 0.5 } },
            new MatchAllDocsQuery(),
            new Number[] { 0.1, 1.1, 2.1 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(3, ranges.size());
                assertEquals("0.0-0.5", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("1.0-1.5", ranges.get(1).getKeyAsString());
                assertEquals(1, ranges.get(1).getDocCount());
                assertEquals("1.0-2.0", ranges.get(2).getKeyAsString());
                assertEquals(1, ranges.get(2).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            false
        );
    }

    /**
     * @return Map [lower, upper) TO data points
     */
    private Map<double[], double[]> buildRandomRanges(double[][] possibleRanges) {
        Map<double[], double[]> dataSet = new LinkedHashMap<>();
        for (double[] range : possibleRanges) {
            double lower = randomDoubleBetween(range[0], range[1], true);
            double upper = randomDoubleBetween(range[0], range[1], true);
            if (lower > upper) {
                double d = lower;
                lower = upper;
                upper = d;
            }

            int dataNumber = randomInt(200);
            double[] data = new double[dataNumber];
            for (int i = 0; i < dataNumber; i++) {
                data[i] = randomDoubleBetween(lower, upper, true);
            }
            dataSet.put(new double[] { lower, upper }, data);
        }

        return dataSet;
    }

    public void testRandomRanges() throws IOException {
        Map<double[], double[]> dataSet = buildRandomRanges(new double[][] { { 0, 100 }, { 200, 1000 }, { 1000, 3000 } });

        int size = dataSet.size();
        double[][] ranges = new double[size][];
        int[] expected = new int[size];
        List<Number> dataPoints = new LinkedList<>();

        int i = 0;
        for (Map.Entry<double[], double[]> entry : dataSet.entrySet()) {
            ranges[i] = entry.getKey();
            expected[i] = entry.getValue().length;
            for (double dataPoint : entry.getValue()) {
                dataPoints.add(dataPoint);
            }
            i++;
        }

        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.DOUBLE.typeName(), NumberType.DOUBLE),
            ranges,
            new MatchAllDocsQuery(),
            dataPoints.toArray(new Number[0]),
            range -> {
                List<? extends InternalRange.Bucket> rangeBuckets = range.getBuckets();
                assertEquals(size, rangeBuckets.size());
                for (int j = 0; j < rangeBuckets.size(); j++) {
                    assertEquals(expected[j], rangeBuckets.get(j).getDocCount());
                }
            },
            true
        );
    }

    public void testDoubleType() throws IOException {
        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.DOUBLE.typeName(), NumberType.DOUBLE),
            new double[][] { { 1, 2 }, { 2, 3 } },
            new MatchAllDocsQuery(),
            new Number[] { 0.1, 1.1, 2.1 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("1.0-2.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("2.0-3.0", ranges.get(1).getKeyAsString());
                assertEquals(1, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            true
        );
    }

    public void testHalfFloatType() throws IOException {
        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.HALF_FLOAT.typeName(), NumberType.HALF_FLOAT),
            new double[][] { { 1, 2 }, { 2, 3 } },
            new MatchAllDocsQuery(),
            new Number[] { 0.1, 1.1, 2.1 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("1.0-2.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("2.0-3.0", ranges.get(1).getKeyAsString());
                assertEquals(1, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            true
        );
    }

    public void testFloatType() throws IOException {
        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.FLOAT.typeName(), NumberType.FLOAT),
            new double[][] { { 1, 2 }, { 2, 3 } },
            new MatchAllDocsQuery(),
            new Number[] { 0.1, 1.1, 2.1 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("1.0-2.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("2.0-3.0", ranges.get(1).getKeyAsString());
                assertEquals(1, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            true
        );
    }

    public void testTopLevelFilterQuery() throws IOException {
        NumberFieldType fieldType = new NumberFieldType(NumberType.INTEGER.typeName(), NumberType.INTEGER);
        String fieldName = fieldType.numberType().typeName();
        Query query = IntPoint.newRangeQuery(fieldName, 5, 20);

        testRewriteOptimizationCase(
            fieldType,
            new double[][] { { 0.0, 10.0 }, { 10.0, 20.0 } },
            query,
            new Number[] { 0.1, 4.0, 9, 11, 12, 19 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("0.0-10.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("10.0-20.0", ranges.get(1).getKeyAsString());
                assertEquals(3, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            false
        );
    }

    public void testTopLevelRangeQuery() throws IOException {
        NumberFieldType fieldType = new NumberFieldType(NumberType.INTEGER.typeName(), NumberType.INTEGER);
        String fieldName = fieldType.numberType().typeName();
        Query query = IntPoint.newRangeQuery(fieldName, 5, 20);

        testRewriteOptimizationCase(
            fieldType,
            new double[][] { { 0.0, 10.0 }, { 10.0, 20.0 } },
            query,
            new Number[] { 0.1, 4.0, 9, 11, 12, 19 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("0.0-10.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("10.0-20.0", ranges.get(1).getKeyAsString());
                assertEquals(3, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            false
        );
    }

    public void testUnsignedLongType() throws IOException {
        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.UNSIGNED_LONG.typeName(), NumberType.UNSIGNED_LONG),
            new double[][] { { 1, 2 }, { 2, 3 } },
            new MatchAllDocsQuery(),
            new Number[] { 0, 1, 2 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("1.0-2.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("2.0-3.0", ranges.get(1).getKeyAsString());
                assertEquals(1, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            true
        );

        testRewriteOptimizationCase(
            new NumberFieldType(NumberType.UNSIGNED_LONG.typeName(), NumberType.UNSIGNED_LONG),
            new double[][] { { Double.NEGATIVE_INFINITY, 1 }, { 2, Double.POSITIVE_INFINITY } },
            new MatchAllDocsQuery(),
            new Number[] { 0, 1, 2 },
            range -> {
                List<? extends InternalRange.Bucket> ranges = range.getBuckets();
                assertEquals(2, ranges.size());
                assertEquals("*-1.0", ranges.get(0).getKeyAsString());
                assertEquals(1, ranges.get(0).getDocCount());
                assertEquals("2.0-*", ranges.get(1).getKeyAsString());
                assertEquals(1, ranges.get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(range));
            },
            true
        );
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);
        RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(NUMBER_FIELD_NAME);
        aggregationBuilder.addRange(0d, 5d);
        aggregationBuilder.addRange(10d, 20d);
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void simpleTestCase(
        RangeAggregationBuilder aggregationBuilder,
        Query query,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldType(NUMBER_FIELD_NAME, NumberType.INTEGER);

        testCase(aggregationBuilder, query, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD_NAME, 3)));
        }, verify, fieldType);
    }

    private void testCase(
        RangeAggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
        MappedFieldType fieldType
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                InternalRange<? extends InternalRange.Bucket, ? extends InternalRange> agg = searchAndReduce(
                    indexSearcher,
                    query,
                    aggregationBuilder,
                    fieldType
                );
                verify.accept(agg);
            }
        }
    }

    private void testRewriteOptimizationCase(
        NumberFieldType fieldType,
        double[][] ranges,
        Query query,
        Number[] dataPoints,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
        boolean optimized
    ) throws IOException {
        NumberType numberType = fieldType.numberType();
        String fieldName = numberType.typeName();

        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
                for (Number dataPoint : dataPoints) {
                    indexWriter.addDocument(numberType.createFields(fieldName, dataPoint, true, true, false));
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("range").field(fieldName);
                for (double[] range : ranges) {
                    aggregationBuilder.addRange(range[0], range[1]);
                }

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
                InternalRange topLevel = (InternalRange) aggregator.buildTopLevel();
                InternalRange agg = (InternalRange) topLevel.reduce(Collections.singletonList(topLevel), context);
                doAssertReducedMultiBucketConsumer(agg, reduceBucketConsumer);

                verify.accept(agg);

                if (optimized) {
                    assertEquals(0, aggregator.getCollectCount().get());
                } else {
                    assertTrue(aggregator.getCollectCount().get() > 0);
                }
            }
        }
    }

    protected CountingAggregator createCountingAggregator(
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
}
