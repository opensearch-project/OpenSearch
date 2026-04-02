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
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class StatsMetricTranslatorTest extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetAggregationType() {
        StatsMetricTranslator translator = new StatsMetricTranslator();
        assertEquals(StatsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCalls() throws ConversionException {
        StatsMetricTranslator translator = new StatsMetricTranslator();
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(4, calls.size());
        assertEquals(SqlKind.COUNT, calls.get(0).getAggregation().getKind());
        assertEquals(SqlKind.MIN, calls.get(1).getAggregation().getKind());
        assertEquals(SqlKind.MAX, calls.get(2).getAggregation().getKind());
        assertEquals(SqlKind.SUM, calls.get(3).getAggregation().getKind());
    }

    public void testToAggregateCallsInvalidField() {
        StatsMetricTranslator translator = new StatsMetricTranslator();
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("invalid");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testGetAggregateFieldNames() {
        StatsMetricTranslator translator = new StatsMetricTranslator();
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(4, names.size());
        assertEquals("price_stats_count", names.get(0));
        assertEquals("price_stats_min", names.get(1));
        assertEquals("price_stats_max", names.get(2));
        assertEquals("price_stats_sum", names.get(3));
    }

    public void testToInternalAggregationWithValidValues() {
        StatsMetricTranslator translator = new StatsMetricTranslator();
        Map<String, Object> values = Map.of(
            "price_stats_count", 10L,
            "price_stats_min", 5.0,
            "price_stats_max", 100.0,
            "price_stats_sum", 550.0
        );

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        assertTrue(result instanceof InternalStats);
        InternalStats stats = (InternalStats) result;
        assertEquals(10, stats.getCount());
        assertEquals(5.0, stats.getMin(), 0.001);
        assertEquals(100.0, stats.getMax(), 0.001);
        assertEquals(550.0, stats.getSum(), 0.001);
    }

    public void testToInternalAggregationWithNull() {
        StatsMetricTranslator translator = new StatsMetricTranslator();

        InternalAggregation result = translator.toInternalAggregation("price_stats", null);

        assertNotNull(result);
        assertTrue(result instanceof InternalStats);
        InternalStats stats = (InternalStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
    }

    public void testToInternalAggregationWithPartialNulls() {
        StatsMetricTranslator translator = new StatsMetricTranslator();
        Map<String, Object> values = Map.of(
            "price_stats_count", 5L,
            "price_stats_sum", 100.0
        );

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        InternalStats stats = (InternalStats) result;
        assertEquals(5, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(100.0, stats.getSum(), 0.001);
    }
}
