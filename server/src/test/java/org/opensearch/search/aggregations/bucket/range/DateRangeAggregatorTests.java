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

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class DateRangeAggregatorTests extends AggregatorTestCase {

    private String NUMBER_FIELD_NAME = "number";
    private String UNMAPPED_FIELD_NAME = "field_not_appearing_in_this_index";
    private String DATE_FIELD_NAME = "date";

    private long milli1 = ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
    private long milli2 = ZonedDateTime.of(2016, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

    public void testNoMatchingField() throws IOException {
        testBothResolutions(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField("bogus_field_name", 7)));
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField("bogus_field_name", 2)));
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField("bogus_field_name", 3)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(0, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertFalse(AggregationInspectionHelper.hasValue(range));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/57651")
    public void testMatchesSortedNumericDocValues() throws IOException {
        testBothResolutions(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField(DATE_FIELD_NAME, milli2)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/57651")
    public void testMatchesNumericDocValues() throws IOException {
        testBothResolutions(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(new NumericDocValuesField(DATE_FIELD_NAME, milli2)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(2, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertEquals(0, ranges.get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        });
    }

    public void testMissingDateStringWithDateField() throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD_NAME);

        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(DATE_FIELD_NAME)
            .missing("2015-11-13T16:14:34")
            .addRange("2015-11-13", "2015-11-14");

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField(DATE_FIELD_NAME, milli1)));
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField(DATE_FIELD_NAME, milli2)));
            // Missing will apply to this document
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField(NUMBER_FIELD_NAME, 7)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testNumberFieldDateRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange("2015-11-13", "2015-11-14");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testNumberFieldNumberRanges() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange(0, 5);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> {
            List<? extends InternalRange.Bucket> ranges = range.getBuckets();
            assertEquals(1, ranges.size());
            assertEquals(1, ranges.get(0).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(range));
        }, fieldType);
    }

    public void testMissingDateStringWithNumberField() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange("2015-11-13", "2015-11-14")
            .missing("1979-01-01T00:00:00");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithMissingNumber() throws IOException {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing(1447438575000L); // 2015-11-13 6:16:15

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

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
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist")
            .addRange("2015-11-13", "2015-11-14")
            .missing("2015-11-13T10:11:12");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

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

    public void testKeywordField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("not_a_number")
            .addRange("2015-11-13", "2015-11-14");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, range -> fail("Should have thrown exception"), fieldType)
        );
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [date_range]", e.getMessage());
    }

    public void testBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field(NUMBER_FIELD_NAME)
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("date_range").field("does_not_exist")
            .addRange("2020-01-01T00:00:00", "2020-01-02T00:00:00")
            .missing("bogus");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER);

        expectThrows(OpenSearchParseException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(NUMBER_FIELD_NAME, 1)));
        }, range -> fail("Should have thrown exception"), fieldType));
    }

    private void testBothResolutions(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify
    ) throws IOException {
        testCase(query, buildIndex, verify, DateFieldMapper.Resolution.MILLISECONDS);
        testCase(query, buildIndex, verify, DateFieldMapper.Resolution.NANOSECONDS);
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalRange<? extends InternalRange.Bucket, ? extends InternalRange>> verify,
        DateFieldMapper.Resolution resolution
    ) throws IOException {
        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(
            DATE_FIELD_NAME,
            true,
            false,
            true,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            resolution,
            null,
            Collections.emptyMap()
        );
        DateRangeAggregationBuilder aggregationBuilder = new DateRangeAggregationBuilder("test_range_agg");
        aggregationBuilder.field(DATE_FIELD_NAME);
        aggregationBuilder.addRange("2015-01-01", "2015-12-31");
        aggregationBuilder.addRange("2019-01-01", "2019-12-31");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(
        DateRangeAggregationBuilder aggregationBuilder,
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

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }
}
