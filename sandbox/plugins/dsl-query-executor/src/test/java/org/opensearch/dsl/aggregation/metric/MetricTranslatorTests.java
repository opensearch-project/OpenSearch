/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.LiteralColumns;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class MetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testAvgTranslator() throws ConversionException {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        List<AggregateCall> calls = translator.toAggregateCalls(new AvgAggregationBuilder("avg_price").field("price"), ctx.getRowType());

        assertEquals(1, calls.size());
        AggregateCall call = calls.get(0);
        assertEquals(SqlKind.AVG, call.getAggregation().getKind());
        assertEquals("avg_price", call.getName());
        assertEquals(1, call.getArgList().size());
        assertEquals(1, call.getArgList().get(0).intValue()); // price is index 1
    }

    public void testSumTranslator() throws ConversionException {
        SumMetricTranslator translator = new SumMetricTranslator();
        List<AggregateCall> calls = translator.toAggregateCalls(new SumAggregationBuilder("total").field("price"), ctx.getRowType());

        assertEquals(1, calls.size());
        AggregateCall call = calls.get(0);
        assertEquals(SqlKind.SUM, call.getAggregation().getKind());
        assertEquals("total", call.getName());
    }

    public void testMinTranslator() throws ConversionException {
        MinMetricTranslator translator = new MinMetricTranslator();
        List<AggregateCall> calls = translator.toAggregateCalls(new MinAggregationBuilder("min_price").field("price"), ctx.getRowType());

        assertEquals(1, calls.size());
        AggregateCall call = calls.get(0);
        assertEquals(SqlKind.MIN, call.getAggregation().getKind());
        assertEquals("min_price", call.getName());
    }

    public void testMaxTranslator() throws ConversionException {
        MaxMetricTranslator translator = new MaxMetricTranslator();
        List<AggregateCall> calls = translator.toAggregateCalls(new MaxAggregationBuilder("max_price").field("price"), ctx.getRowType());

        assertEquals(1, calls.size());
        AggregateCall call = calls.get(0);
        assertEquals(SqlKind.MAX, call.getAggregation().getKind());
        assertEquals("max_price", call.getName());
    }

    public void testThrowsForUnknownField() {
        AvgMetricTranslator translator = new AvgMetricTranslator();

        expectThrows(
            ConversionException.class,
            () -> translator.toAggregateCalls(new AvgAggregationBuilder("bad").field("nonexistent"), ctx.getRowType())
        );
    }

    public void testNonNumericFieldRejectedWithClassicMessage() {
        AvgMetricTranslator translator = new AvgMetricTranslator();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translator.toAggregateCalls(new AvgAggregationBuilder("bad").field("brand"), ctx.getRowType())
        );

        assertEquals("Field [brand] of type [VARCHAR] is not supported for aggregation [avg]", e.getMessage());
    }

    public void testNonNumericFieldRejectedForAllArithmeticMetrics() {
        assertNonNumericRejected(new SumMetricTranslator(), new SumAggregationBuilder("s").field("brand"), "sum");
        assertNonNumericRejected(new MinMetricTranslator(), new MinAggregationBuilder("m").field("brand"), "min");
        assertNonNumericRejected(new MaxMetricTranslator(), new MaxAggregationBuilder("x").field("brand"), "max");
    }

    private <T extends org.opensearch.search.aggregations.AggregationBuilder> void assertNonNumericRejected(
        MetricTranslator<T> translator,
        T agg,
        String aggregationType
    ) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
        assertTrue(e.getMessage().contains("is not supported for aggregation [" + aggregationType + "]"));
    }

    public void testAggregateFieldName() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        List<String> names = translator.getAggregateFieldNames(new AvgAggregationBuilder("avg_price").field("price"));
        assertEquals(1, names.size());
        assertEquals("avg_price", names.get(0));
    }

    /** User-supplied meta must be echoed back on the response aggregation, like classic search. */
    public void testMetadataEchoedInInternalAggregation() {
        Map<String, Object> meta = Map.of("owner", "pricing-team", "revision", 3);

        AvgAggregationBuilder avg = new AvgAggregationBuilder("a").field("price");
        avg.setMetadata(meta);
        assertEquals(meta, new AvgMetricTranslator().toInternalAggregation(avg, Map.of("a", 1.0)).getMetadata());

        SumAggregationBuilder sum = new SumAggregationBuilder("s").field("price");
        sum.setMetadata(meta);
        assertEquals(meta, new SumMetricTranslator().toInternalAggregation(sum, Map.of("s", 1.0)).getMetadata());

        MinAggregationBuilder min = new MinAggregationBuilder("mn").field("price");
        min.setMetadata(meta);
        assertEquals(meta, new MinMetricTranslator().toInternalAggregation(min, Map.of("mn", 1.0)).getMetadata());

        MaxAggregationBuilder max = new MaxAggregationBuilder("mx").field("price");
        max.setMetadata(meta);
        assertEquals(meta, new MaxMetricTranslator().toInternalAggregation(max, Map.of("mx", 1.0)).getMetadata());

        // Echoed even when the metric has no value (empty result)
        assertEquals(meta, new AvgMetricTranslator().toInternalAggregation(avg, null).getMetadata());
    }

    /** Requests without meta keep rendering without a meta section. */
    public void testNullMetadataStaysNull() {
        assertNull(
            new AvgMetricTranslator().toInternalAggregation(new AvgAggregationBuilder("a").field("price"), Map.of("a", 1.0)).getMetadata()
        );
        assertNull(new MaxMetricTranslator().toInternalAggregation(new MaxAggregationBuilder("mx").field("price"), null).getMetadata());
    }

    public void testMissingAggregatesOverCoalescedColumn() throws ConversionException {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AvgAggregationBuilder agg = new AvgAggregationBuilder("avg_price").field("price");
        agg.missing(0);
        int baseFieldCount = ctx.getRowType().getFieldCount();
        LiteralColumns allocator = new AggregationMetadataBuilder().literalColumns(baseFieldCount);

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator);

        assertEquals(List.of(baseFieldCount), calls.get(0).getArgList());
    }

    public void testMissingWithoutAllocatorRejected() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AvgAggregationBuilder agg = new AvgAggregationBuilder("avg_price").field("price");
        agg.missing(0);

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testNonNumericMissingRejected() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AvgAggregationBuilder agg = new AvgAggregationBuilder("avg_price").field("price");
        agg.missing("not-a-number");
        LiteralColumns allocator = new AggregationMetadataBuilder().literalColumns(ctx.getRowType().getFieldCount());

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator));
    }

    public void testFormatAppliedToResponse() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AvgAggregationBuilder agg = new AvgAggregationBuilder("avg_price").field("price");
        agg.format("0.00");

        InternalAvg result = (InternalAvg) translator.toInternalAggregation(agg, Map.of("avg_price", 30.2));

        assertEquals("30.20", result.getValueAsString());
    }

    public void testInvalidFormatRejected() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AvgAggregationBuilder agg = new AvgAggregationBuilder("avg_price").field("price");
        agg.format("0.0.0");
        LiteralColumns allocator = new AggregationMetadataBuilder().literalColumns(ctx.getRowType().getFieldCount());

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator));
    }
}
