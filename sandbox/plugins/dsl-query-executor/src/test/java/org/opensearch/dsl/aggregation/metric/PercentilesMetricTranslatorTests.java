/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.LiteralColumnAllocator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class PercentilesMetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();
    private final PercentilesMetricTranslator translator = new PercentilesMetricTranslator();

    private LiteralColumnAllocator allocator() {
        return new AggregationMetadataBuilder().literalColumnAllocator(ctx.getRowType().getFieldCount());
    }

    public void testGetAggregationType() {
        assertEquals(PercentilesAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCallsOnePerPercent() throws ConversionException {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 95, 99);
        int baseFieldCount = ctx.getRowType().getFieldCount();

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        assertEquals(3, calls.size());
        for (AggregateCall call : calls) {
            assertEquals("PERCENTILE_APPROX", call.getAggregation().getName());
            assertEquals(2, call.getArgList().size());
        }
        assertEquals("pcts_p50_0", calls.get(0).getName());
        assertEquals("pcts_p95_0", calls.get(1).getName());
        assertEquals("pcts_p99_0", calls.get(2).getName());
        // Literal columns are appended after the base fields, one per distinct percent.
        assertEquals(List.of(1, baseFieldCount), calls.get(0).getArgList());
        assertEquals(List.of(1, baseFieldCount + 1), calls.get(1).getArgList());
        assertEquals(List.of(1, baseFieldCount + 2), calls.get(2).getArgList());
    }

    public void testLiteralColumnsDeduplicateAcrossAggregations() throws ConversionException {
        LiteralColumnAllocator shared = allocator();
        int baseFieldCount = ctx.getRowType().getFieldCount();

        PercentilesAggregationBuilder first = new PercentilesAggregationBuilder("a").field("price").percentiles(50, 95);
        PercentilesAggregationBuilder second = new PercentilesAggregationBuilder("b").field("rating").percentiles(95, 99);

        List<AggregateCall> firstCalls = translator.toAggregateCalls(first, ctx.getRowType(), shared);
        List<AggregateCall> secondCalls = translator.toAggregateCalls(second, ctx.getRowType(), shared);

        // 95.0 is shared: 50.0→base, 95.0→base+1, 99.0→base+2.
        assertEquals(List.of(1, baseFieldCount), firstCalls.get(0).getArgList());
        assertEquals(List.of(1, baseFieldCount + 1), firstCalls.get(1).getArgList());
        assertEquals(List.of(3, baseFieldCount + 1), secondCalls.get(0).getArgList());
        assertEquals(List.of(3, baseFieldCount + 2), secondCalls.get(1).getArgList());
    }

    public void testToAggregateCallsInvalidField() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("invalid").percentiles(50);

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator()));
    }

    public void testNonNumericFieldRejectedWithClassicMessage() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("brand").percentiles(50);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator())
        );

        assertEquals("Field [brand] of type [VARCHAR] is not supported for aggregation [percentiles]", e.getMessage());
    }

    public void testTwoArgVariantRejected() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testHdrMethodRejected() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price")
            .percentiles(50)
            .percentilesConfig(new PercentilesConfig.Hdr());

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator()));
    }

    public void testGetAggregateFieldNames() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 99.9);

        assertEquals(List.of("pcts_p50_0", "pcts_p99_9"), translator.getAggregateFieldNames(agg));
    }

    public void testToInternalAggregationWithValues() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 99);
        Map<String, Object> values = Map.of("pcts_p50_0", 899, "pcts_p99_0", 1299.0);

        InternalDslPercentiles result = (InternalDslPercentiles) translator.toInternalAggregation(agg, values);

        assertEquals(899.0, result.percentile(50), 0.001);
        assertEquals(1299.0, result.percentile(99), 0.001);
    }

    public void testToInternalAggregationWithNull() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 99);

        InternalDslPercentiles result = (InternalDslPercentiles) translator.toInternalAggregation(agg, null);

        assertTrue(Double.isNaN(result.percentile(50)));
        assertTrue(Double.isNaN(result.percentile(99)));
    }

    public void testToInternalAggregationWithPartialValues() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 99);
        Map<String, Object> values = Map.of("pcts_p50_0", 899.0);

        InternalDslPercentiles result = (InternalDslPercentiles) translator.toInternalAggregation(agg, values);

        assertEquals(899.0, result.percentile(50), 0.001);
        assertTrue(Double.isNaN(result.percentile(99)));
    }

    public void testMissingAllocatesSharedCoalescedColumn() throws ConversionException {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 95);
        agg.missing(0);
        int baseFieldCount = ctx.getRowType().getFieldCount();

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        // One coalesced column (allocated first) shared by all percents; percent literals follow.
        assertEquals(2, calls.size());
        assertEquals(List.of(baseFieldCount, baseFieldCount + 1), calls.get(0).getArgList());
        assertEquals(List.of(baseFieldCount, baseFieldCount + 2), calls.get(1).getArgList());
    }

    public void testStringMissingValueParsed() throws ConversionException {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.missing("2.5");
        int baseFieldCount = ctx.getRowType().getFieldCount();

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        assertEquals(List.of(baseFieldCount, baseFieldCount + 1), calls.get(0).getArgList());
    }

    public void testNonNumericMissingRejected() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.missing("not-a-number");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator()));
    }

    public void testInvalidFormatRejected() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.format("0.0.0");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator()));
    }

    public void testFormatAppliedToResponse() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.format("0.00");
        Map<String, Object> values = Map.of("pcts_p50_0", 899.0);

        InternalDslPercentiles result = (InternalDslPercentiles) translator.toInternalAggregation(agg, values);

        assertEquals("899.00", result.percentileAsString(50.0));
    }

    public void testExplicitCompressionEmitsThreeArgForm() throws ConversionException {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50, 95);
        agg.percentilesConfig(new PercentilesConfig.TDigest(200));
        int baseFieldCount = ctx.getRowType().getFieldCount();

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        // One centroids column (allocated first) shared by all percents; percent literals follow.
        assertEquals(2, calls.size());
        for (AggregateCall call : calls) {
            assertEquals("PERCENTILE_APPROX_N", call.getAggregation().getName());
        }
        assertEquals(List.of(1, baseFieldCount + 1, baseFieldCount), calls.get(0).getArgList());
        assertEquals(List.of(1, baseFieldCount + 2, baseFieldCount), calls.get(1).getArgList());
    }

    public void testDefaultConfigKeepsTwoArgForm() throws ConversionException {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        assertEquals("PERCENTILE_APPROX", calls.get(0).getAggregation().getName());
        assertEquals(2, calls.get(0).getArgList().size());
    }

    public void testDefaultCompressionValueKeepsTwoArgForm() throws ConversionException {
        // The request parser injects TDigest(100) when the JSON names no method; must equal no-config.
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.percentilesConfig(new PercentilesConfig.TDigest(100));

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        assertEquals("PERCENTILE_APPROX", calls.get(0).getAggregation().getName());
        assertEquals(2, calls.get(0).getArgList().size());
    }

    public void testZeroCompressionRejected() {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.percentilesConfig(new PercentilesConfig.TDigest(0));

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType(), allocator()));
    }

    public void testCompressionCombinesWithMissing() throws ConversionException {
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pcts").field("price").percentiles(50);
        agg.missing(0);
        agg.percentilesConfig(new PercentilesConfig.TDigest(150));
        int baseFieldCount = ctx.getRowType().getFieldCount();

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator());

        // Allocation order: coalesced field, centroids, percent.
        assertEquals("PERCENTILE_APPROX_N", calls.get(0).getAggregation().getName());
        assertEquals(List.of(baseFieldCount, baseFieldCount + 2, baseFieldCount + 1), calls.get(0).getArgList());
    }
}
