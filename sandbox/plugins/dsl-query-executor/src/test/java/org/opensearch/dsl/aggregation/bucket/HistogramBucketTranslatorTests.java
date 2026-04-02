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
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.aggregation.HistogramGrouping;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class HistogramBucketTranslatorTests {

    private HistogramBucketTranslator shape;
    private HistogramAggregationBuilder agg;

    @Before
    public void setUp() {
        shape = new HistogramBucketTranslator();
        agg = new HistogramAggregationBuilder("test_histogram")
            .field("price")
            .interval(100);
    }

    @Test
    public void testGetAggregationType() {
        assertEquals(HistogramAggregationBuilder.class, shape.getAggregationType());
    }

    @Test
    public void testGetGrouping() {
        GroupingInfo grouping = shape.getGrouping(agg);

        assertNotNull(grouping);
        assertTrue(grouping instanceof HistogramGrouping);
        assertEquals(List.of("price"), grouping.getFieldNames());
    }

    @Test
    public void testGetOrder() {
        BucketOrder order = BucketOrder.key(true);
        agg.order(order);

        assertEquals(order, shape.getOrder(agg));
    }

    @Test
    public void testGetSubAggregations() {
        agg.subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        Collection<AggregationBuilder> subAggs = shape.getSubAggregations(agg);

        assertNotNull(subAggs);
        assertEquals(1, subAggs.size());
        assertEquals("avg_price", subAggs.iterator().next().getName());
    }

    @Test
    public void testToBucketAggregationWithEmptyBuckets() {
        List<BucketEntry> buckets = new ArrayList<>();

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        assertTrue(result instanceof InternalHistogram);
        InternalHistogram histogram = (InternalHistogram) result;
        assertEquals("test_histogram", histogram.getName());
        assertEquals(0, histogram.getBuckets().size());
    }

    @Test
    public void testToBucketAggregation() {
        BucketEntry entry = new BucketEntry(List.of(100.0), 10L, InternalAggregations.EMPTY);
        List<BucketEntry> buckets = List.of(entry);

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        assertTrue(result instanceof InternalHistogram);
        InternalHistogram histogram = (InternalHistogram) result;
        assertEquals(1, histogram.getBuckets().size());
        assertEquals(100.0, (Double) histogram.getBuckets().get(0).getKey(), 0.001);
        assertEquals(10L, histogram.getBuckets().get(0).getDocCount());
    }

    @Test
    public void testToBucketAggregationWithIntegerKey() {
        BucketEntry entry = new BucketEntry(List.of(50), 3L, InternalAggregations.EMPTY);
        List<BucketEntry> buckets = List.of(entry);

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        InternalHistogram histogram = (InternalHistogram) result;
        assertEquals(50.0, (Double) histogram.getBuckets().get(0).getKey(), 0.001);
    }

    @Test
    public void testGapFillingWithMinDocCountZero() {
        agg.minDocCount(0);

        List<BucketEntry> buckets = List.of(
            new BucketEntry(List.of(100.0), 5L, InternalAggregations.EMPTY),
            new BucketEntry(List.of(300.0), 3L, InternalAggregations.EMPTY)
        );

        InternalAggregation result = shape.toBucketAggregation(agg, buckets);

        assertNotNull(result);
        InternalHistogram histogram = (InternalHistogram) result;
        assertEquals(3, histogram.getBuckets().size());

        assertEquals(100.0, (Double) histogram.getBuckets().get(0).getKey(), 0.001);
        assertEquals(5L, histogram.getBuckets().get(0).getDocCount());

        assertEquals(200.0, (Double) histogram.getBuckets().get(1).getKey(), 0.001);
        assertEquals(0L, histogram.getBuckets().get(1).getDocCount());

        assertEquals(300.0, (Double) histogram.getBuckets().get(2).getKey(), 0.001);
        assertEquals(3L, histogram.getBuckets().get(2).getDocCount());
    }
}
