/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class MovAvgAggregatorTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "date";
    private static final String VALUE_FIELD = "value_field";

    public void testWindow() throws IOException {
        check(movAvg(3), Arrays.asList(null, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0));
    }

    public void testWindowAtLeastBucketCount() throws IOException {
        List<Double> expected = Arrays.asList(null, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0);
        check(movAvg(10), expected);
        check(movAvg(Integer.MAX_VALUE), expected);
    }

    private static MovAvgPipelineAggregationBuilder movAvg(int window) {
        return new MovAvgPipelineAggregationBuilder("mov_avg", "avg").window(window).model(new SimpleModel());
    }

    private void check(PipelineAggregationBuilder pipeline, List<Double> expected) throws IOException {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("histo").calendarInterval(DateHistogramInterval.DAY)
            .field(DATE_FIELD)
            .subAggregation(new AvgAggregationBuilder("avg").field(VALUE_FIELD))
            .subAggregation(pipeline);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int day = 1; day <= 10; day++) {
                    Document document = new Document();
                    document.add(
                        new SortedNumericDocValuesField(DATE_FIELD, asLong(String.format(Locale.ROOT, "2017-01-%02dT00:00:00", day)))
                    );
                    document.add(new NumericDocValuesField(VALUE_FIELD, day));
                    indexWriter.addDocument(document);
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                MappedFieldType dateFieldType = new DateFieldMapper.DateFieldType(DATE_FIELD);
                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD, NumberFieldMapper.NumberType.LONG);
                InternalDateHistogram histogram = searchAndReduce(
                    indexSearcher,
                    new MatchAllDocsQuery(),
                    histo,
                    1000,
                    new MappedFieldType[] { dateFieldType, valueFieldType }
                );
                List<Double> actual = new ArrayList<>();
                for (InternalDateHistogram.Bucket bucket : histogram.getBuckets()) {
                    InternalSimpleValue value = bucket.getAggregations().get(pipeline.getName());
                    actual.add(value == null ? null : value.value());
                }
                assertThat(actual, equalTo(expected));
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.getDefaultDateTimeFormatter().parse(dateTime)).toInstant().toEpochMilli();
    }
}
