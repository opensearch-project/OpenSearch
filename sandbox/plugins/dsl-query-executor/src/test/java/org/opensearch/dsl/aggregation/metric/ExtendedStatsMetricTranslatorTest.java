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
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class ExtendedStatsMetricTranslatorTest extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetAggregationType() {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();
        assertEquals(ExtendedStatsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCalls() throws ConversionException {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price");

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(5, calls.size());
        assertEquals(SqlKind.COUNT, calls.get(0).getAggregation().getKind());
        assertEquals(SqlKind.MIN, calls.get(1).getAggregation().getKind());
        assertEquals(SqlKind.MAX, calls.get(2).getAggregation().getKind());
        assertEquals(SqlKind.SUM, calls.get(3).getAggregation().getKind());
        assertEquals("price_stats_variance", calls.get(4).getName());
    }

    public void testToAggregateCallsInvalidField() {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("invalid");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testGetAggregateFieldNames() {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(5, names.size());
        assertEquals("price_stats_count", names.get(0));
        assertEquals("price_stats_min", names.get(1));
        assertEquals("price_stats_max", names.get(2));
        assertEquals("price_stats_sum", names.get(3));
        assertEquals("price_stats_variance", names.get(4));
    }

    public void testToInternalAggregationWithValidValues() {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();
        Map<String, Object> values = Map.of(
            "price_stats_count", 10L,
            "price_stats_min", 5.0,
            "price_stats_max", 100.0,
            "price_stats_sum", 550.0,
            "price_stats_variance", 900.0
        );

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        assertTrue(result instanceof InternalExtendedStats);
        InternalExtendedStats stats = (InternalExtendedStats) result;
        assertEquals(10, stats.getCount());
        assertEquals(5.0, stats.getMin(), 0.001);
        assertEquals(100.0, stats.getMax(), 0.001);
        assertEquals(550.0, stats.getSum(), 0.001);
        assertEquals(39250.0, stats.getSumOfSquares(), 0.001);
    }

    public void testToInternalAggregationWithNull() {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();

        InternalAggregation result = translator.toInternalAggregation("price_stats", null);

        assertNotNull(result);
        assertTrue(result instanceof InternalExtendedStats);
        InternalExtendedStats stats = (InternalExtendedStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
        assertEquals(0.0, stats.getSumOfSquares(), 0.001);
        assertTrue(Double.isNaN(stats.getVariance()));
    }

    public void testToInternalAggregationWithPartialNulls() {
        ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();
        Map<String, Object> values = Map.of(
            "price_stats_count", 5L,
            "price_stats_sum", 100.0
        );

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        InternalExtendedStats stats = (InternalExtendedStats) result;
        assertEquals(5, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(100.0, stats.getSum(), 0.001);
    }
}
