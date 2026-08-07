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
import org.opensearch.dsl.aggregation.LiteralColumnAllocator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class StatsMetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();
    private final StatsMetricTranslator translator = new StatsMetricTranslator();

    public void testGetAggregationType() {
        assertEquals(StatsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCalls() throws ConversionException {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(4, calls.size());
        assertEquals(SqlKind.COUNT, calls.get(0).getAggregation().getKind());
        assertEquals(SqlKind.MIN, calls.get(1).getAggregation().getKind());
        assertEquals(SqlKind.MAX, calls.get(2).getAggregation().getKind());
        assertEquals(SqlKind.SUM, calls.get(3).getAggregation().getKind());
        assertEquals("price_stats_count", calls.get(0).getName());
    }

    public void testToAggregateCallsInvalidField() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("invalid");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testScriptRejected() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").script(new Script("doc['price'].value"));

        ConversionException e = expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));

        assertEquals("aggregation [price_stats] with a script is not supported", e.getMessage());
    }

    public void testNonNumericFieldRejected() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("brand_stats").field("brand");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));

        assertEquals("Field [brand] of type [VARCHAR] is not supported for aggregation [stats]", e.getMessage());
    }

    public void testMissingAggregatesAllCallsOverCoalescedColumn() throws ConversionException {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");
        agg.missing(0);
        int baseFieldCount = ctx.getRowType().getFieldCount();
        LiteralColumnAllocator allocator = new AggregationMetadataBuilder().literalColumnAllocator(baseFieldCount);

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType(), allocator);

        assertEquals(4, calls.size());
        for (AggregateCall call : calls) {
            assertEquals(List.of(baseFieldCount), call.getArgList());
        }
    }

    public void testFormatAppliedToResponse() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");
        agg.format("0.00");
        Map<String, Object> values = Map.of(
            "price_stats_count",
            2L,
            "price_stats_min",
            1.0,
            "price_stats_max",
            3.0,
            "price_stats_sum",
            4.0
        );

        InternalStats result = (InternalStats) translator.toInternalAggregation(agg, values);

        assertEquals("2.00", result.getAvgAsString());
    }

    public void testGetAggregateFieldNames() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(List.of("price_stats_count", "price_stats_min", "price_stats_max", "price_stats_sum"), names);
    }

    public void testToInternalAggregationWithValidValues() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");
        Map<String, Object> values = Map.of(
            "price_stats_count",
            10L,
            "price_stats_min",
            5.0,
            "price_stats_max",
            100.0,
            "price_stats_sum",
            550.0
        );

        InternalAggregation result = translator.toInternalAggregation(agg, values);

        InternalStats stats = (InternalStats) result;
        assertEquals(10, stats.getCount());
        assertEquals(5.0, stats.getMin(), 0.001);
        assertEquals(100.0, stats.getMax(), 0.001);
        assertEquals(550.0, stats.getSum(), 0.001);
        assertEquals(55.0, stats.getAvg(), 0.001);
    }

    public void testToInternalAggregationWithNull() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        InternalAggregation result = translator.toInternalAggregation(agg, null);

        InternalStats stats = (InternalStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
    }

    public void testToInternalAggregationWithPartialNulls() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");
        Map<String, Object> values = Map.of("price_stats_count", 5L, "price_stats_sum", 100.0);

        InternalAggregation result = translator.toInternalAggregation(agg, values);

        InternalStats stats = (InternalStats) result;
        assertEquals(5, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(100.0, stats.getSum(), 0.001);
    }
}
