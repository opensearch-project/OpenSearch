/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.dsl.aggregation.DateHistogramGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class DateHistogramBucketTranslatorTests {

    private DateHistogramBucketTranslator shape;
    private DateHistogramAggregationBuilder agg;

    @Before
    public void setUp() {
        shape = new DateHistogramBucketTranslator();
        agg = new DateHistogramAggregationBuilder("test_date_histogram")
            .field("timestamp")
            .calendarInterval(DateHistogramInterval.MONTH);
    }

    @Test
    public void testGetAggregationType() {
        assertEquals(DateHistogramAggregationBuilder.class, shape.getAggregationType());
    }

    @Test
    public void testGetGrouping() {
        GroupingInfo grouping = shape.getGrouping(agg);

        assertNotNull(grouping);
        assertTrue(grouping instanceof DateHistogramGrouping);
        assertEquals(List.of("timestamp"), grouping.getFieldNames());
    }

    @Test
    public void testGetOrder() {
        BucketOrder order = BucketOrder.key(true);
        agg.order(order);

        assertEquals(order, shape.getOrder(agg));
    }

    @Test
    public void testGetSubAggregations() {
        agg.subAggregation(new AvgAggregationBuilder("avg_value").field("value"));

        Collection<AggregationBuilder> subAggs = shape.getSubAggregations(agg);

        assertNotNull(subAggs);
        assertEquals(1, subAggs.size());
        assertEquals("avg_value", subAggs.iterator().next().getName());
    }

    @Test
    public void testToBucketAggregationWithEmptyBuckets() {
        List<BucketEntry> buckets = new ArrayList<>();

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        assertTrue(result instanceof InternalDateHistogram);
        InternalDateHistogram histogram = (InternalDateHistogram) result;
        assertEquals("test_date_histogram", histogram.getName());
        assertEquals(0, histogram.getBuckets().size());
    }

    @Test
    public void testToBucketAggregation() {
        long timestamp = 1704067200000L;
        BucketEntry entry = new BucketEntry(List.of(timestamp), 10L, InternalAggregations.EMPTY);
        List<BucketEntry> buckets = List.of(entry);

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        assertTrue(result instanceof InternalDateHistogram);
        InternalDateHistogram histogram = (InternalDateHistogram) result;
        assertEquals(1, histogram.getBuckets().size());
        assertEquals(10L, histogram.getBuckets().get(0).getDocCount());
    }

    @Test
    public void testGapFillingWithMinDocCountZero() {
        agg.calendarInterval(DateHistogramInterval.DAY);
        agg.minDocCount(0);

        // 3 days apart with 1d interval = should fill 1 gap
        List<BucketEntry> buckets = List.of(
            new BucketEntry(List.of(1704067200000L), 5L, InternalAggregations.EMPTY),  // 2024-01-01
            new BucketEntry(List.of(1704240000000L), 3L, InternalAggregations.EMPTY)   // 2024-01-03
        );

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        InternalDateHistogram histogram = (InternalDateHistogram) result;
        assertEquals(3, histogram.getBuckets().size());

        assertEquals(5L, histogram.getBuckets().get(0).getDocCount());
        assertEquals(0L, histogram.getBuckets().get(1).getDocCount());  // Gap filled
        assertEquals(3L, histogram.getBuckets().get(2).getDocCount());
    }
}
