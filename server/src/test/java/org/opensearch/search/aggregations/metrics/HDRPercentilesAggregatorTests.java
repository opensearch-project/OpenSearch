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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.opensearch.search.aggregations.AggregationBuilders.percentiles;
import static org.hamcrest.Matchers.equalTo;

public class HDRPercentilesAggregatorTests extends AggregatorTestCase {

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new PercentilesAggregationBuilder("hdr_percentiles").field(fieldName).percentilesConfig(new PercentilesConfig.Hdr());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Arrays.asList(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN);
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    /**
     * Attempting to use HDRPercentileAggregation on a string field throws IllegalArgumentException
     */
    public void testStringField() throws IOException {
        final String fieldName = "string";
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        expectThrows(IllegalArgumentException.class, () -> testCase(new FieldExistsQuery(fieldName), iw -> {
            iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("bogus"))));
            iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("zwomp"))));
            iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foobar"))));
        }, hdr -> {}, fieldType, fieldName));
    }

    /**
     * Attempting to use HDRPercentileAggregation on a range field throws IllegalArgumentException
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/42949")
    public void testRangeField() throws IOException {
        // Currently fails (throws ClassCast exception), but should be fixed once HDRPercentileAggregation uses the ValuesSource registry
        final String fieldName = "range";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, RangeType.DOUBLE);
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(RangeType.DOUBLE, 1.0D, 5.0D, true, true);
        BytesRef encodedRange = RangeType.DOUBLE.encodeRanges(Collections.singleton(range));
        expectThrows(IllegalArgumentException.class, () -> testCase(new FieldExistsQuery(fieldName), iw -> {
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, encodedRange)));
        }, hdr -> {}, fieldType, fieldName));
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 60)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 40)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 20)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, hdr -> {
            assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(10.0d, hdr.percentile(25), approximation);
            assertEquals(20.0d, hdr.percentile(50), approximation);
            assertEquals(40.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 60)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 40)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 20)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, hdr -> {
            assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(10.0d, hdr.percentile(25), approximation);
            assertEquals(20.0d, hdr.percentile(50), approximation);
            assertEquals(40.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testQueryFiltering() throws IOException {
        final CheckedConsumer<RandomIndexWriter, IOException> docs = iw -> {
            iw.addDocument(asList(new LongPoint("row", 4), new SortedNumericDocValuesField("number", 60)));
            iw.addDocument(asList(new LongPoint("row", 3), new SortedNumericDocValuesField("number", 40)));
            iw.addDocument(asList(new LongPoint("row", 2), new SortedNumericDocValuesField("number", 20)));
            iw.addDocument(asList(new LongPoint("row", 1), new SortedNumericDocValuesField("number", 10)));
        };

        testCase(LongPoint.newRangeQuery("row", 0, 2), docs, hdr -> {
            assertEquals(2L, hdr.state.getTotalCount());
            assertEquals(10.0d, hdr.percentile(randomDoubleBetween(1, 50, true)), 0.05d);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });

        testCase(LongPoint.newRangeQuery("row", 5, 10), docs, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testHdrThenTdigestSettings() throws Exception {
        int sigDigits = randomIntBetween(1, 5);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            percentiles("percentiles").numberOfSignificantValueDigits(sigDigits)
                .method(PercentilesMethod.HDR)
                .compression(100.0) // <-- this should trigger an exception
                .field("value");
        });
        assertThat(e.getMessage(), equalTo("Cannot set [compression] because the method has already been configured for HDRHistogram"));
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalHDRPercentiles> verify)
        throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        testCase(query, buildIndex, verify, fieldType, "number");
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalHDRPercentiles> verify,
        MappedFieldType fieldType,
        String fieldName
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                PercentilesAggregationBuilder builder;
                // TODO this randomization path should be removed when the old settings are removed
                if (randomBoolean()) {
                    builder = new PercentilesAggregationBuilder("test").field(fieldName).method(PercentilesMethod.HDR);
                } else {
                    PercentilesConfig hdr = new PercentilesConfig.Hdr();
                    builder = new PercentilesAggregationBuilder("test").field(fieldName).percentilesConfig(hdr);
                }

                HDRPercentilesAggregator aggregator = createAggregator(builder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                verify.accept((InternalHDRPercentiles) aggregator.buildAggregation(0L));

            }
        }
    }
}
