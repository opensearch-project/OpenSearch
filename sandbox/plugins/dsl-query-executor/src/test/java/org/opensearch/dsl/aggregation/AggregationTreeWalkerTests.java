/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregationTreeWalkerTests extends OpenSearchTestCase {

    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final ConversionContext ctx = TestUtils.createContext();

    public void testMetricOnly() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(new AvgAggregationBuilder("avg_price").field("price"));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(1, result.size());
        assertTrue(result.get(0).getGroupByBitSet().isEmpty());
        // No implicit _count for global (no-bucket) granularity
        assertEquals(1, result.get(0).getAggregateCalls().size());
        assertEquals("avg_price", result.get(0).getAggregateFieldNames().get(0));
    }

    public void testMultipleMetricsSameGranularity() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("avg_price").field("price"),
            new SumAggregationBuilder("total_price").field("price")
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        assertEquals(List.of("avg_price", "total_price"), result.get(0).getAggregateFieldNames());
    }

    public void testBucketWithMetric() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(1, result.size());
        assertFalse(result.get(0).getGroupByBitSet().isEmpty());
        assertTrue(result.get(0).getGroupByBitSet().get(2)); // brand is index 2
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        // avg_price + implicit _count
        assertEquals(2, result.get(0).getAggregateCalls().size());
        assertTrue(result.get(0).getAggregateFieldNames().contains("_count"));
        assertTrue(result.get(0).getAggregateFieldNames().contains("avg_price"));
    }

    public void testBucketOnlyProducesImplicitCount() throws ConversionException {
        // terms bucket with no explicit metrics — still produces metadata with implicit _count
        List<AggregationBuilder> aggs = List.of(new TermsAggregationBuilder("by_brand").field("brand"));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getAggregateCalls().size());
        assertEquals("_count", result.get(0).getAggregateFieldNames().get(0));
    }

    public void testNestedBucketsProduceMultipleGranularities() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new SumAggregationBuilder("total").field("price"))
                .subAggregation(
                    new TermsAggregationBuilder("by_name").field("name")
                        .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        // Brand granularity: SUM + implicit _count
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        // Brand+name granularity: AVG + implicit _count
        assertEquals(List.of("brand", "name"), result.get(1).getGroupByFieldNames());
        assertEquals(2, result.get(1).getAggregateCalls().size());
    }

    public void testMetricAtMultipleGranularities() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("global_avg").field("price"),
            new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("brand_avg").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        // Root: no GROUP BY, no implicit _count
        assertTrue(result.get(0).getGroupByBitSet().isEmpty());
        assertEquals(1, result.get(0).getAggregateCalls().size());
        assertEquals(List.of("global_avg"), result.get(0).getAggregateFieldNames());
        // Brand: GROUP BY brand, AVG + implicit _count
        assertEquals(List.of("brand"), result.get(1).getGroupByFieldNames());
        assertEquals(2, result.get(1).getAggregateCalls().size());
    }

    public void testSiblingBucketsProduceSeparateGranularities() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("brand_avg").field("price")),
            new TermsAggregationBuilder("by_name").field("name").subAggregation(new SumAggregationBuilder("name_total").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        // brand granularity: AVG + _count
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        // name granularity: SUM + _count
        assertEquals(List.of("name"), result.get(1).getGroupByFieldNames());
        assertEquals(2, result.get(1).getAggregateCalls().size());
    }

    public void testBucketOrderCollected() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .order(BucketOrder.key(true))
                .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(result.get(0).hasBucketOrders());
        assertFalse(result.get(0).getBucketOrders().isEmpty());
    }

    public void testDefaultBucketOrderCollected() throws ConversionException {
        // Default terms order is _count desc — should still be collected
        List<AggregationBuilder> aggs = List.of(new TermsAggregationBuilder("by_brand").field("brand"));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(result.get(0).hasBucketOrders());
    }

    public void testMetricOnlyHasNoBucketOrders() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(new AvgAggregationBuilder("avg_price").field("price"));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertFalse(result.get(0).hasBucketOrders());
    }

    public void testNestedBucketsCollectOrdersPerGranularity() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .order(BucketOrder.count(false))
                .subAggregation(
                    new TermsAggregationBuilder("by_name").field("name")
                        .order(BucketOrder.key(true))
                        .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        // Both granularities have bucket orders
        assertTrue(result.get(0).hasBucketOrders());
        assertTrue(result.get(1).hasBucketOrders());
    }

    public void testThrowsForUnsupportedAggregation() {
        List<AggregationBuilder> aggs = List.of(new HistogramAggregationBuilder("by_price").field("price").interval(100));

        expectThrows(ConversionException.class, () -> walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory()));
    }
}
