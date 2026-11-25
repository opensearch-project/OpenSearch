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
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

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

    // ---- Fetch pushdown eligibility, HAVING, and missing (top-K pushdown) ----

    public void testRootTermsGranularityGetsFetch() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(new TermsAggregationBuilder("by_brand").field("brand").size(7));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(Integer.valueOf(7), result.get(0).getFetch());
        assertNull(result.get(0).getHavingMinDocCount());
        assertTrue(result.get(0).getMissingValues().isEmpty());
    }

    public void testMinDocCountAboveOneGetsHavingAndFetch() throws ConversionException {
        // Both bounds bake into the plan: HAVING filters before the LIMIT truncates. The
        // eligible count (which must exclude below-threshold groups) comes from the
        // aggregation's own HAVING-filtered COUNT plan, built by the converter.
        List<AggregationBuilder> aggs = List.of(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(Long.valueOf(5), result.get(0).getHavingMinDocCount());
        assertNotNull(result.get(0).getFetch());
    }

    public void testNestedGranularityGetsPerParentFetch() throws ConversionException {
        // A flat LIMIT on the child level would keep top groups globally, not per parent —
        // nested levels are bounded per parent instead (ROW_NUMBER over the parent partition).
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new TermsAggregationBuilder("by_name").field("name").size(3))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        assertNotNull("root level carries the flat limit", result.get(0).getFetch());
        assertNull(result.get(0).getPerParentFetch());
        assertEquals(List.of("brand", "name"), result.get(1).getGroupByFieldNames());
        assertNull("nested level must not carry a flat limit", result.get(1).getFetch());
        assertEquals("nested level is bounded per parent", Integer.valueOf(3), result.get(1).getPerParentFetch());
    }

    public void testSameFieldSiblingsGetSeparateBoundedPlans() throws ConversionException {
        // A plan is defined by its aggregation, not its GROUP BY columns: siblings over the
        // same field each get their own plan with their own order and LIMIT baked in.
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("top_brands").field("brand").order(BucketOrder.count(false)),
            new TermsAggregationBuilder("brands_by_key").field("brand").order(BucketOrder.key(true))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        assertEquals(List.of("top_brands"), result.get(0).getAggNamePath());
        assertNotNull(result.get(0).getFetch());
        assertEquals(List.of("brands_by_key"), result.get(1).getAggNamePath());
        assertNotNull(result.get(1).getFetch());
    }

    public void testMissingCapturedAndFetchKept() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(new TermsAggregationBuilder("by_brand").field("brand").missing("unknown"));

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(Map.of("brand", "unknown"), result.get(0).getMissingValues());
        assertNotNull(result.get(0).getFetch());
    }

    public void testSameFieldSiblingsCarryTheirOwnMissing() throws ConversionException {
        // Separate plans per sibling: each substitutes its own missing value — no conflict possible.
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("first").field("brand").missing("a"),
            new TermsAggregationBuilder("second").field("brand").missing("b")
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertEquals(2, result.size());
        assertEquals(Map.of("brand", "a"), result.get(0).getMissingValues());
        assertEquals(Map.of("brand", "b"), result.get(1).getMissingValues());
    }

    // ---- Parameter gate: silently ignored parameters must reject loudly ----

    public void testValidateRejectsIncludeExclude() {
        TermsAggregationBuilder agg = new TermsAggregationBuilder("by_brand").field("brand");
        agg.includeExclude(new IncludeExclude("Brand.*", null));

        ConversionException e = expectThrows(
            ConversionException.class,
            () -> walker.walk(List.of(agg), ctx.getRowType(), ctx.getCluster().getTypeFactory())
        );
        assertTrue(e.getMessage().contains("include"));
    }

    public void testValidateRejectsTermsScript() {
        TermsAggregationBuilder agg = new TermsAggregationBuilder("by_brand").script(new Script("doc['brand'].value"));

        expectThrows(ConversionException.class, () -> walker.walk(List.of(agg), ctx.getRowType(), ctx.getCluster().getTypeFactory()));
    }

    public void testValidateRejectsMinDocCountZero() {
        TermsAggregationBuilder agg = new TermsAggregationBuilder("by_brand").field("brand").minDocCount(0);

        ConversionException e = expectThrows(
            ConversionException.class,
            () -> walker.walk(List.of(agg), ctx.getRowType(), ctx.getCluster().getTypeFactory())
        );
        assertTrue(e.getMessage().contains("min_doc_count"));
    }

    public void testValidateRejectsMetricMissing() {
        AvgAggregationBuilder agg = new AvgAggregationBuilder("avg_price").field("price").missing(42);

        ConversionException e = expectThrows(
            ConversionException.class,
            () -> walker.walk(List.of(agg), ctx.getRowType(), ctx.getCluster().getTypeFactory())
        );
        assertTrue(e.getMessage().contains("missing"));
    }
}
