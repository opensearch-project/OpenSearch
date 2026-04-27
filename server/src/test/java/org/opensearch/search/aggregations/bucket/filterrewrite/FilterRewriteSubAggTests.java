/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

public class FilterRewriteSubAggTests extends AggregatorTestCase {
    private final String longFieldName = "metric";
    private final String dateFieldName = "timestamp";
    private final String nameFieldName = "name";
    private final Query matchAllQuery = new MatchAllDocsQuery();
    private final NumberFieldMapper.NumberFieldType longFieldType = new NumberFieldMapper.NumberFieldType(
        longFieldName,
        NumberFieldMapper.NumberType.LONG
    );
    private final DateFieldMapper.DateFieldType dateFieldType = aggregableDateFieldType(false, true);
    private final KeywordFieldMapper.KeywordFieldType nameFieldType = new KeywordFieldMapper.KeywordFieldType(nameFieldName);
    private final NumberFieldMapper.NumberType numberType = longFieldType.numberType();
    private final String rangeAggName = "range";
    private final String autoDateAggName = "auto";
    private final String dateAggName = "date";
    private final String statsAggName = "stats";
    private final String avgAggName = "avg";
    private final String sumAggName = "sum";
    private final String minAggName = "min";
    private final String maxAggName = "max";
    private final String cardinalityAggName = "cardinality";
    private final List<TestDoc> DEFAULT_DATA = List.of(
        new TestDoc(0, Instant.parse("2020-03-01T00:00:00Z"), "abc"),
        new TestDoc(1, Instant.parse("2020-03-01T00:00:00Z"), "def"),
        new TestDoc(1, Instant.parse("2020-03-01T00:00:01Z"), "ghi"),
        new TestDoc(2, Instant.parse("2020-03-01T01:00:00Z"), "jkl"),
        new TestDoc(3, Instant.parse("2020-03-01T02:00:00Z"), "jkl"),
        new TestDoc(4, Instant.parse("2020-03-01T03:00:00Z"), "mno"),
        new TestDoc(4, Instant.parse("2020-03-01T04:00:00Z"), "prq", true),
        new TestDoc(5, Instant.parse("2020-03-01T04:00:00Z"), "stu"),
        new TestDoc(6, Instant.parse("2020-03-01T04:00:00Z"), "stu")
    );

    public void testRange() throws IOException {
        RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
            .addRange(1, 2)
            .addRange(2, 4)
            .addRange(4, 6)
            .subAggregation(new AutoDateHistogramAggregationBuilder(autoDateAggName).field(dateFieldName).setNumBuckets(3));

        InternalRange result = executeAggregation(DEFAULT_DATA, rangeAggregationBuilder, true);

        // Verify results
        List<? extends InternalRange.Bucket> buckets = result.getBuckets();
        assertEquals(3, buckets.size());

        InternalRange.Bucket firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.getDocCount());
        InternalAutoDateHistogram firstAuto = firstBucket.getAggregations().get(autoDateAggName);
        assertEquals(2, firstAuto.getBuckets().size());

        InternalRange.Bucket secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.getDocCount());
        InternalAutoDateHistogram secondAuto = secondBucket.getAggregations().get(autoDateAggName);
        assertEquals(3, secondAuto.getBuckets().size());

        InternalRange.Bucket thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.getDocCount());
        InternalAutoDateHistogram thirdAuto = thirdBucket.getAggregations().get(autoDateAggName);
        assertEquals(3, thirdAuto.getBuckets().size());
    }

    public void testRangeWithAvgAndSum() throws IOException {
        // Test for sum metric aggregation
        RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
            .addRange(1, 2)
            .addRange(2, 4)
            .addRange(4, 6)
            .subAggregation(AggregationBuilders.sum(sumAggName).field(longFieldName));

        InternalRange result = executeAggregation(DEFAULT_DATA, rangeAggregationBuilder, true);

        // Verify results
        List<? extends InternalRange.Bucket> buckets = result.getBuckets();
        assertEquals(3, buckets.size());

        InternalRange.Bucket firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.getDocCount());
        InternalSum firstSum = firstBucket.getAggregations().get(sumAggName);
        assertEquals(2, firstSum.getValue(), 0);

        InternalRange.Bucket secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.getDocCount());
        InternalSum secondSum = secondBucket.getAggregations().get(sumAggName);
        assertEquals(5, secondSum.getValue(), 0);

        InternalRange.Bucket thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.getDocCount());
        InternalSum thirdSum = thirdBucket.getAggregations().get(sumAggName);
        assertEquals(9, thirdSum.getValue(), 0);

        // Test for average metric aggregation now
        rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
            .addRange(1, 2)
            .addRange(2, 4)
            .addRange(4, 6)
            .subAggregation(AggregationBuilders.avg(avgAggName).field(longFieldName));

        result = executeAggregation(DEFAULT_DATA, rangeAggregationBuilder, true);

        // Verify results
        buckets = result.getBuckets();
        assertEquals(3, buckets.size());

        firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.getDocCount());
        InternalAvg firstAvg = firstBucket.getAggregations().get(avgAggName);
        assertEquals(1, firstAvg.getValue(), 0);

        secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.getDocCount());
        InternalAvg secondAvg = secondBucket.getAggregations().get(avgAggName);
        assertEquals(2.5, secondAvg.getValue(), 0);

        thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.getDocCount());
        InternalAvg thirdAvg = thirdBucket.getAggregations().get(avgAggName);
        assertEquals(4.5, thirdAvg.getValue(), 0);
    }

    public void testRangeWithMinAndMax() throws IOException {
        // Test for min metric aggregation
        RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
            .addRange(1, 2)
            .addRange(2, 4)
            .addRange(4, 6)
            .subAggregation(AggregationBuilders.min(minAggName).field(longFieldName));

        InternalRange result = executeAggregation(DEFAULT_DATA, rangeAggregationBuilder, true);

        // Verify results
        List<? extends InternalRange.Bucket> buckets = result.getBuckets();
        assertEquals(3, buckets.size());

        InternalRange.Bucket firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.getDocCount());
        InternalMin firstMin = firstBucket.getAggregations().get(minAggName);
        assertEquals(1, firstMin.getValue(), 0);

        InternalRange.Bucket secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.getDocCount());
        InternalMin secondMin = secondBucket.getAggregations().get(minAggName);
        assertEquals(2, secondMin.getValue(), 0);

        InternalRange.Bucket thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.getDocCount());
        InternalMin thirdMin = thirdBucket.getAggregations().get(minAggName);
        assertEquals(4, thirdMin.getValue(), 0);

        // Test for max metric aggregation now
        rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
            .addRange(1, 2)
            .addRange(2, 4)
            .addRange(4, 6)
            .subAggregation(AggregationBuilders.max(maxAggName).field(longFieldName));

        result = executeAggregation(DEFAULT_DATA, rangeAggregationBuilder, true);

        // Verify results
        buckets = result.getBuckets();
        assertEquals(3, buckets.size());

        firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.getDocCount());
        InternalMax firstMax = firstBucket.getAggregations().get(maxAggName);
        assertEquals(1, firstMax.getValue(), 0);

        secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.getDocCount());
        InternalMax secondMax = secondBucket.getAggregations().get(maxAggName);
        assertEquals(3, secondMax.getValue(), 0);

        thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.getDocCount());
        InternalMax thirdMax = thirdBucket.getAggregations().get(maxAggName);
        assertEquals(5, thirdMax.getValue(), 0);
    }

    public void testRangeWithCard() throws IOException {
        RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
            .addRange(1, 2)
            .addRange(2, 4)
            .addRange(4, 6)
            .subAggregation(AggregationBuilders.cardinality(cardinalityAggName).field(nameFieldName).executionHint("ordinals"));

        InternalRange result = executeAggregation(DEFAULT_DATA, rangeAggregationBuilder, true);

        // Verify results
        List<? extends InternalRange.Bucket> buckets = result.getBuckets();
        assertEquals(3, buckets.size());

        InternalRange.Bucket firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.getDocCount());
        InternalCardinality firstCardinality = firstBucket.getAggregations().get(cardinalityAggName);
        assertEquals(2, firstCardinality.getValue(), 0);

        InternalRange.Bucket secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.getDocCount());
        InternalCardinality secondCardinality = secondBucket.getAggregations().get(cardinalityAggName);
        assertEquals(1, secondCardinality.getValue(), 0);

        InternalRange.Bucket thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.getDocCount());
        InternalCardinality thirdCardinality = thirdBucket.getAggregations().get(cardinalityAggName);
        assertEquals(2, thirdCardinality.getValue(), 0);
    }

    /**
     * Test that verifies skiplist-based collection works correctly with range aggregations
     * that have date histogram sub-aggregations.
     *
     * This test exercises the following code paths:
     * 1. HistogramSkiplistLeafCollector.collect() - skiplist-based document collection
     * 2. HistogramSkiplistLeafCollector.advanceSkipper() - skiplist advancement with upToBucket logic
     * 3. SubAggRangeCollector.collect() - sub-aggregation collection path
     *
     * The test uses:
     * - Index sort on date field to enable skiplist functionality
     * - Multiple segments created via explicit commits
     * - Searchable date field type
     * - Documents distributed across multiple date ranges (2015-2017)
     */
    public void testRangeDate() throws IOException {
        // Setup index with skiplist configuration
        Settings settings = getSettingsWithIndexSort();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);

        // Create searchable date field type (isSearchable=true) to enable skiplist
        DateFieldMapper.DateFieldType searchableDateFieldType = aggregableDateFieldType(false, true);

        // Use custom index setup instead of setupIndex() method
        try (Directory directory = newDirectory()) {
            // Index documents in batches with commits to create multiple segments
            indexDocsForSkiplist(directory, searchableDateFieldType);

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                // Verify we have multiple segments (required for skiplist testing)
                assertTrue("Should have multiple segments for skiplist testing", indexReader.leaves().size() > 1);

                // Create IndexSearcher with the reader
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                // Build RangeAggregationBuilder with DateHistogramAggregationBuilder sub-aggregation
                // Use YEAR interval to align with our test data structure (2015, 2016, 2017)
                RangeAggregationBuilder rangeAggregationBuilder = new RangeAggregationBuilder(rangeAggName).field(longFieldName)
                    .addRange(1, 2)
                    .addRange(2, 4)
                    .addRange(4, 6)
                    .subAggregation(
                        new DateHistogramAggregationBuilder(dateAggName).field(dateFieldName).calendarInterval(DateHistogramInterval.YEAR)
                    );

                // Execute aggregation on reader with IndexSettings (enables skiplist)
                InternalRange result = executeAggregationOnReader(indexReader, rangeAggregationBuilder, indexSettings);

                // Verify results - this confirms skiplist collection worked correctly
                List<? extends InternalRange.Bucket> buckets = result.getBuckets();
                assertEquals(3, buckets.size());

                // Range bucket 1: expect 5 docs, 1 date histogram bucket (2015)
                InternalRange.Bucket firstBucket = buckets.get(0);
                assertEquals(5, firstBucket.getDocCount());
                InternalDateHistogram firstDate = firstBucket.getAggregations().get(dateAggName);
                assertNotNull("Sub-aggregation should be present (verifies SubAggRangeCollector.collect() was called)", firstDate);
                assertEquals(1, firstDate.getBuckets().size());
                assertEquals(5, firstDate.getBuckets().get(0).getDocCount());

                // Range bucket 2: expect 8 docs, 2 date histogram buckets (2015, 2016)
                InternalRange.Bucket secondBucket = buckets.get(1);
                assertEquals(8, secondBucket.getDocCount());
                InternalDateHistogram secondDate = secondBucket.getAggregations().get(dateAggName);
                assertNotNull("Sub-aggregation should be present (verifies SubAggRangeCollector.collect() was called)", secondDate);
                assertEquals(2, secondDate.getBuckets().size());
                assertEquals(5, secondDate.getBuckets().get(0).getDocCount());
                assertEquals(3, secondDate.getBuckets().get(1).getDocCount());

                // Range bucket 3: expect 7 docs, 1 date histogram bucket (2017)
                InternalRange.Bucket thirdBucket = buckets.get(2);
                assertEquals(7, thirdBucket.getDocCount());
                InternalDateHistogram thirdDate = thirdBucket.getAggregations().get(dateAggName);
                assertNotNull("Sub-aggregation should be present (verifies SubAggRangeCollector.collect() was called)", thirdDate);
                assertEquals(1, thirdDate.getBuckets().size());
                assertEquals(7, thirdDate.getBuckets().get(0).getDocCount());
            }
        }
    }

    public void testDateHisto() throws IOException {
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder(dateAggName).field(
            dateFieldName
        ).calendarInterval(DateHistogramInterval.HOUR).subAggregation(AggregationBuilders.stats(statsAggName).field(longFieldName));

        InternalDateHistogram result = executeAggregation(DEFAULT_DATA, dateHistogramAggregationBuilder, false);

        // Verify results
        List<? extends InternalDateHistogram.Bucket> buckets = result.getBuckets();
        assertEquals(5, buckets.size());

        InternalDateHistogram.Bucket firstBucket = buckets.get(0);
        assertEquals("2020-03-01T00:00:00.000Z", firstBucket.getKeyAsString());
        assertEquals(3, firstBucket.getDocCount());
        InternalStats firstStats = firstBucket.getAggregations().get(statsAggName);
        assertEquals(3, firstStats.getCount());
        assertEquals(1, firstStats.getMax(), 0);
        assertEquals(0, firstStats.getMin(), 0);
        assertEquals(2, firstStats.getSum(), 0);

        InternalDateHistogram.Bucket secondBucket = buckets.get(1);
        assertEquals("2020-03-01T01:00:00.000Z", secondBucket.getKeyAsString());
        assertEquals(1, secondBucket.getDocCount());
        InternalStats secondStats = secondBucket.getAggregations().get(statsAggName);
        assertEquals(1, secondStats.getCount());
        assertEquals(2, secondStats.getMax(), 0);
        assertEquals(2, secondStats.getMin(), 0);
        assertEquals(2, secondStats.getSum(), 0);

        InternalDateHistogram.Bucket thirdBucket = buckets.get(2);
        assertEquals("2020-03-01T02:00:00.000Z", thirdBucket.getKeyAsString());
        assertEquals(1, thirdBucket.getDocCount());
        InternalStats thirdStats = thirdBucket.getAggregations().get(statsAggName);
        assertEquals(1, thirdStats.getCount());
        assertEquals(3, thirdStats.getMax(), 0);
        assertEquals(3, thirdStats.getMin(), 0);
        assertEquals(3, thirdStats.getSum(), 0);

        InternalDateHistogram.Bucket fourthBucket = buckets.get(3);
        assertEquals("2020-03-01T03:00:00.000Z", fourthBucket.getKeyAsString());
        assertEquals(1, fourthBucket.getDocCount());
        InternalStats fourthStats = fourthBucket.getAggregations().get(statsAggName);
        assertEquals(1, fourthStats.getCount());
        assertEquals(4, fourthStats.getMax(), 0);
        assertEquals(4, fourthStats.getMin(), 0);
        assertEquals(4, fourthStats.getSum(), 0);

        InternalDateHistogram.Bucket fifthBucket = buckets.get(4);
        assertEquals("2020-03-01T04:00:00.000Z", fifthBucket.getKeyAsString());
        assertEquals(2, fifthBucket.getDocCount());
        InternalStats fifthStats = fifthBucket.getAggregations().get(statsAggName);
        assertEquals(2, fifthStats.getCount());
        assertEquals(6, fifthStats.getMax(), 0);
        assertEquals(5, fifthStats.getMin(), 0);
        assertEquals(11, fifthStats.getSum(), 0);
    }

    public void testAutoDateHisto() throws IOException {
        AutoDateHistogramAggregationBuilder autoDateHistogramAggregationBuilder = new AutoDateHistogramAggregationBuilder(dateAggName)
            .field(dateFieldName)
            .setNumBuckets(5)
            .subAggregation(AggregationBuilders.stats(statsAggName).field(longFieldName));

        InternalAutoDateHistogram result = executeAggregation(DEFAULT_DATA, autoDateHistogramAggregationBuilder, true);

        // Verify results
        List<? extends Histogram.Bucket> buckets = result.getBuckets();
        assertEquals(5, buckets.size());

        Histogram.Bucket firstBucket = buckets.get(0);
        assertEquals("2020-03-01T00:00:00.000Z", firstBucket.getKeyAsString());
        assertEquals(3, firstBucket.getDocCount());
        InternalStats firstStats = firstBucket.getAggregations().get(statsAggName);
        assertEquals(3, firstStats.getCount());
        assertEquals(1, firstStats.getMax(), 0);
        assertEquals(0, firstStats.getMin(), 0);

        Histogram.Bucket secondBucket = buckets.get(1);
        assertEquals("2020-03-01T01:00:00.000Z", secondBucket.getKeyAsString());
        assertEquals(1, secondBucket.getDocCount());
        InternalStats secondStats = secondBucket.getAggregations().get(statsAggName);
        assertEquals(1, secondStats.getCount());
        assertEquals(2, secondStats.getMax(), 0);
        assertEquals(2, secondStats.getMin(), 0);

        Histogram.Bucket thirdBucket = buckets.get(2);
        assertEquals("2020-03-01T02:00:00.000Z", thirdBucket.getKeyAsString());
        assertEquals(1, thirdBucket.getDocCount());
        InternalStats thirdStats = thirdBucket.getAggregations().get(statsAggName);
        assertEquals(1, thirdStats.getCount());
        assertEquals(3, thirdStats.getMax(), 0);
        assertEquals(3, thirdStats.getMin(), 0);

        Histogram.Bucket fourthBucket = buckets.get(3);
        assertEquals("2020-03-01T03:00:00.000Z", fourthBucket.getKeyAsString());
        assertEquals(1, fourthBucket.getDocCount());
        InternalStats fourthStats = fourthBucket.getAggregations().get(statsAggName);
        assertEquals(1, fourthStats.getCount());
        assertEquals(4, fourthStats.getMax(), 0);
        assertEquals(4, fourthStats.getMin(), 0);

        Histogram.Bucket fifthBucket = buckets.get(4);
        assertEquals("2020-03-01T04:00:00.000Z", fifthBucket.getKeyAsString());
        assertEquals(2, fifthBucket.getDocCount());
        InternalStats fifthStats = fifthBucket.getAggregations().get(statsAggName);
        assertEquals(2, fifthStats.getCount());
        assertEquals(6, fifthStats.getMax(), 0);
        assertEquals(5, fifthStats.getMin(), 0);

    }

    public void testRandom() throws IOException {
        Map<String, Integer> dataset = new HashMap<>();
        dataset.put("2017-02-01T09:02:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T09:59:59.999Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T10:00:00.001Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T13:06:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T14:04:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T14:05:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T15:59:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T16:06:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T16:48:00.000Z", randomIntBetween(100, 2000));
        dataset.put("2017-02-01T16:59:00.000Z", randomIntBetween(100, 2000));

        Map<String, SubAggToVerify> subAggToVerify = new HashMap<>();
        List<TestDoc> docs = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : dataset.entrySet()) {
            String date = entry.getKey();
            int docCount = entry.getValue();
            // loop value times and generate TestDoc
            if (!subAggToVerify.containsKey(date)) {
                subAggToVerify.put(date, new SubAggToVerify());
            }
            SubAggToVerify subAgg = subAggToVerify.get(date);
            subAgg.count = docCount;
            for (int i = 0; i < docCount; i++) {
                Instant instant = Instant.parse(date);
                int docValue = randomIntBetween(0, 10_000);
                subAgg.min = Math.min(subAgg.min, docValue);
                subAgg.max = Math.max(subAgg.max, docValue);
                docs.add(new TestDoc(docValue, instant));
            }
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder(dateAggName).field(
            dateFieldName
        )
            .calendarInterval(DateHistogramInterval.HOUR)
            .minDocCount(1L)
            .subAggregation(AggregationBuilders.stats(statsAggName).field(longFieldName));

        InternalDateHistogram result = executeAggregation(docs, dateHistogramAggregationBuilder, true);
        List<? extends InternalDateHistogram.Bucket> buckets = result.getBuckets();
        assertEquals(6, buckets.size());
        for (InternalDateHistogram.Bucket bucket : buckets) {
            String date = bucket.getKeyAsString();
            SubAggToVerify subAgg = subAggToVerify.get(date);
            if (subAgg == null) continue;
            InternalStats stats = bucket.getAggregations().get(statsAggName);
            assertEquals(subAgg.count, stats.getCount());
            assertEquals(subAgg.max, stats.getMax(), 0);
            assertEquals(subAgg.min, stats.getMin(), 0);
        }
    }

    public void testLeafTraversal() throws IOException {
        Map<String, Integer> dataset = new HashMap<>();
        dataset.put("2017-02-01T09:02:00.000Z", 512);
        dataset.put("2017-02-01T09:59:59.999Z", 256);
        dataset.put("2017-02-01T10:00:00.001Z", 256);
        dataset.put("2017-02-01T13:06:00.000Z", 512);
        dataset.put("2017-02-01T14:04:00.000Z", 256);
        dataset.put("2017-02-01T14:05:00.000Z", 256);
        dataset.put("2017-02-01T15:59:00.000Z", 768);

        Map<String, SubAggToVerify> subAggToVerify = new HashMap<>();
        List<TestDoc> docs = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : dataset.entrySet()) {
            String date = entry.getKey();
            int docCount = entry.getValue();
            // loop value times and generate TestDoc
            if (!subAggToVerify.containsKey(date)) {
                subAggToVerify.put(date, new SubAggToVerify());
            }
            SubAggToVerify subAgg = subAggToVerify.get(date);
            subAgg.count = docCount;
            for (int i = 0; i < docCount; i++) {
                Instant instant = Instant.parse(date);
                int docValue = randomIntBetween(0, 10_000);
                subAgg.min = Math.min(subAgg.min, docValue);
                subAgg.max = Math.max(subAgg.max, docValue);
                docs.add(new TestDoc(docValue, instant));
            }
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new DateHistogramAggregationBuilder(dateAggName).field(
            dateFieldName
        )
            .calendarInterval(DateHistogramInterval.HOUR)
            .minDocCount(1L)
            .subAggregation(AggregationBuilders.stats(statsAggName).field(longFieldName));

        InternalDateHistogram result = executeAggregation(docs, dateHistogramAggregationBuilder, false);
        List<? extends InternalDateHistogram.Bucket> buckets = result.getBuckets();
        assertEquals(5, buckets.size());
        for (InternalDateHistogram.Bucket bucket : buckets) {
            String date = bucket.getKeyAsString();
            SubAggToVerify subAgg = subAggToVerify.get(date);
            if (subAgg == null) continue;
            InternalStats stats = bucket.getAggregations().get(statsAggName);
            assertEquals(subAgg.count, stats.getCount());
            assertEquals(subAgg.max, stats.getMax(), 0);
            assertEquals(subAgg.min, stats.getMin(), 0);
        }
    }

    private <IA extends InternalAggregation> IA executeAggregation(
        List<TestDoc> docs,
        AggregationBuilder aggregationBuilder,
        boolean random
    ) throws IOException {
        try (Directory directory = setupIndex(docs, random)) {
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                return executeAggregationOnReader(indexReader, aggregationBuilder);
            }
        }
    }

    private Directory setupIndex(List<TestDoc> docs, boolean random) throws IOException {
        Directory directory = newDirectory();
        if (!random) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
                for (TestDoc doc : docs) {
                    indexWriter.addDocument(doc.toDocument());
                }

                indexWriter.commit();
            }

            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
                for (TestDoc doc : docs) {
                    if (doc.deleted) {
                        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
                        booleanQueryBuilder.add(LongPoint.newRangeQuery(longFieldName, doc.metric, doc.metric), BooleanClause.Occur.MUST);
                        booleanQueryBuilder.add(
                            LongField.newRangeQuery(
                                dateFieldName,
                                dateFieldType.parse(doc.timestamp.toString()),
                                dateFieldType.parse(doc.timestamp.toString())
                            ),
                            BooleanClause.Occur.MUST
                        );
                        indexWriter.deleteDocuments(booleanQueryBuilder.build());
                    }
                }

                indexWriter.commit();
            }
        } else {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (TestDoc doc : docs) {
                    if (!doc.deleted) {
                        indexWriter.addDocument(doc.toDocument());
                    }
                }
            }
        }
        return directory;
    }

    private <IA extends InternalAggregation> IA executeAggregationOnReader(
        DirectoryReader indexReader,
        AggregationBuilder aggregationBuilder
    ) throws IOException {
        return executeAggregationOnReader(indexReader, aggregationBuilder, null);
    }

    private <IA extends InternalAggregation> IA executeAggregationOnReader(
        DirectoryReader indexReader,
        AggregationBuilder aggregationBuilder,
        IndexSettings indexSettings
    ) throws IOException {
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = createBucketConsumer();
        SearchContext searchContext = createSearchContext(
            indexSearcher,
            indexSettings != null ? indexSettings : createIndexSettings(),
            matchAllQuery,
            bucketConsumer,
            longFieldType,
            dateFieldType,
            nameFieldType
        );
        Aggregator aggregator = createAggregator(aggregationBuilder, searchContext);
        CountingAggregator countingAggregator = new CountingAggregator(new AtomicInteger(), aggregator);

        // Execute aggregation
        countingAggregator.preCollection();
        indexSearcher.search(matchAllQuery, countingAggregator);
        countingAggregator.postCollection();

        // Reduce results
        IA topLevel = (IA) countingAggregator.buildTopLevel();
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = createReduceBucketConsumer();
        InternalAggregation.ReduceContext context = createReduceContext(countingAggregator, reduceBucketConsumer);

        IA result = (IA) topLevel.reduce(Collections.singletonList(topLevel), context);
        doAssertReducedMultiBucketConsumer(result, reduceBucketConsumer);

        assertEquals("Expect not using collect to do aggregation", 0, countingAggregator.getCollectCount().get());

        return result;
    }

    private MultiBucketConsumerService.MultiBucketConsumer createBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
    }

    private MultiBucketConsumerService.MultiBucketConsumer createReduceBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            Integer.MAX_VALUE,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
    }

    private InternalAggregation.ReduceContext createReduceContext(
        Aggregator aggregator,
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer
    ) {
        return InternalAggregation.ReduceContext.forFinalReduction(
            aggregator.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    private class TestDoc {
        private final long metric;
        private final Instant timestamp;
        private final String name;
        private final boolean deleted;

        public TestDoc(long metric, Instant timestamp) {
            this(metric, timestamp, "abc", false);
        }

        public TestDoc(long metric, Instant timestamp, String name) {
            this(metric, timestamp, name, false);
        }

        public TestDoc(long metric, Instant timestamp, String name, boolean deleted) {
            this.metric = metric;
            this.timestamp = timestamp;
            this.name = name;
            this.deleted = deleted;
        }

        public ParseContext.Document toDocument() {
            ParseContext.Document doc = new ParseContext.Document();

            List<Field> fieldList = numberType.createFields(longFieldName, metric, true, true, false, false);
            for (Field fld : fieldList)
                doc.add(fld);
            doc.add(new LongField(dateFieldName, dateFieldType.parse(timestamp.toString()), Field.Store.NO));
            doc.add(new KeywordField(nameFieldName, name, Field.Store.NO));

            return doc;
        }
    }

    private static class SubAggToVerify {
        int min;
        int max;
        int count;
    }

    protected final DateFieldMapper.DateFieldType aggregableDateFieldType(boolean useNanosecondResolution, boolean isSearchable) {
        return new DateFieldMapper.DateFieldType(
            dateFieldName,
            isSearchable,
            false,
            true,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            useNanosecondResolution ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
    }

    /**
     * Helper method to create Settings with index sort on date field for skiplist testing
     */
    private Settings getSettingsWithIndexSort() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .putList("index.sort.field", dateFieldName)
            .build();
    }

    /**
     * Helper method to index documents in batches with commits for skiplist structure
     */
    private void indexDocsForSkiplist(Directory directory, DateFieldMapper.DateFieldType dateFieldType) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setMergePolicy(NoMergePolicy.INSTANCE);

        // Create sort field for index sort
        IndexNumericFieldData fieldData = (IndexNumericFieldData) dateFieldType.fielddataBuilder("index", () -> {
            throw new UnsupportedOperationException();
        }).build(null, null);
        SortField sortField = fieldData.sortField(null, MultiValueMode.MIN, null, false);
        config.setIndexSort(new Sort(sortField));

        try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
            // First commit - documents for range bucket 1 (metric values 1-2)
            // Dates: 2015 (5 docs with metric=1)
            for (int i = 0; i < 5; i++) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                long timestamp = asLong("2015-02-13T13:09:32Z", dateFieldType);
                doc.add(SortedNumericDocValuesField.indexedField(dateFieldName, timestamp));
                doc.add(new LongPoint(dateFieldName, timestamp));
                doc.add(new NumericDocValuesField(longFieldName, 1));
                doc.add(new LongPoint(longFieldName, 1));
                indexWriter.addDocument(doc);
            }
            indexWriter.commit();

            // Second commit - documents for range bucket 2 (metric values 2-4)
            // Dates: 2015-2016 (5 docs with metric=2, 3 docs with metric=3)
            for (int i = 0; i < 5; i++) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                long timestamp = asLong("2015-11-13T16:14:34Z", dateFieldType);
                doc.add(SortedNumericDocValuesField.indexedField(dateFieldName, timestamp));
                doc.add(new LongPoint(dateFieldName, timestamp));
                doc.add(new NumericDocValuesField(longFieldName, 2));
                doc.add(new LongPoint(longFieldName, 2));
                indexWriter.addDocument(doc);
            }
            for (int i = 0; i < 3; i++) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                long timestamp = asLong("2016-03-04T17:09:50Z", dateFieldType);
                doc.add(SortedNumericDocValuesField.indexedField(dateFieldName, timestamp));
                doc.add(new LongPoint(dateFieldName, timestamp));
                doc.add(new NumericDocValuesField(longFieldName, 3));
                doc.add(new LongPoint(longFieldName, 3));
                indexWriter.addDocument(doc);
            }
            indexWriter.commit();

            // Third commit - documents for range bucket 3 (metric values 4-6)
            // Dates: 2017 (4 docs with metric=4, 3 docs with metric=5)
            for (int i = 0; i < 4; i++) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                long timestamp = asLong("2017-12-12T22:55:46Z", dateFieldType);
                doc.add(SortedNumericDocValuesField.indexedField(dateFieldName, timestamp));
                doc.add(new LongPoint(dateFieldName, timestamp));
                doc.add(new NumericDocValuesField(longFieldName, 4));
                doc.add(new LongPoint(longFieldName, 4));
                indexWriter.addDocument(doc);
            }
            for (int i = 0; i < 3; i++) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                long timestamp = asLong("2017-12-12T22:55:46Z", dateFieldType);
                doc.add(SortedNumericDocValuesField.indexedField(dateFieldName, timestamp));
                doc.add(new LongPoint(dateFieldName, timestamp));
                doc.add(new NumericDocValuesField(longFieldName, 5));
                doc.add(new LongPoint(longFieldName, 5));
                indexWriter.addDocument(doc);
            }
            indexWriter.commit();
        }
    }

    /**
     * Helper method to parse date strings to long values
     */
    private long asLong(String dateTime, DateFieldMapper.DateFieldType fieldType) {
        return fieldType.parse(dateTime);
    }
}
