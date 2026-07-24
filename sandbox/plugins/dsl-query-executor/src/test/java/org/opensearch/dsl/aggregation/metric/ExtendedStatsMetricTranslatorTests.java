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

public class ExtendedStatsMetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();
    private final ExtendedStatsMetricTranslator translator = new ExtendedStatsMetricTranslator();

    public void testGetAggregationType() {
        assertEquals(ExtendedStatsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCalls() throws ConversionException {
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
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("invalid");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testGetAggregateFieldNames() {
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(List.of("price_stats_count", "price_stats_min", "price_stats_max", "price_stats_sum", "price_stats_variance"), names);
    }

    public void testToInternalAggregationWithValidValues() {
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price");
        Map<String, Object> values = Map.of(
            "price_stats_count",
            10L,
            "price_stats_min",
            5.0,
            "price_stats_max",
            100.0,
            "price_stats_sum",
            550.0,
            "price_stats_variance",
            900.0
        );

        InternalAggregation result = translator.toInternalAggregation(agg, values);

        InternalExtendedStats stats = (InternalExtendedStats) result;
        assertEquals(10, stats.getCount());
        assertEquals(5.0, stats.getMin(), 0.001);
        assertEquals(100.0, stats.getMax(), 0.001);
        assertEquals(550.0, stats.getSum(), 0.001);
        // sumOfSquares = (variance + avg^2) * count = (900 + 55^2) * 10
        assertEquals(39250.0, stats.getSumOfSquares(), 0.001);
        assertEquals(900.0, stats.getVariance(), 0.001);
        assertEquals(30.0, stats.getStdDeviation(), 0.001);
    }

    public void testSigmaFromRequestShapesBounds() {
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price").sigma(3.0);
        Map<String, Object> values = Map.of(
            "price_stats_count",
            10L,
            "price_stats_min",
            5.0,
            "price_stats_max",
            100.0,
            "price_stats_sum",
            550.0,
            "price_stats_variance",
            900.0
        );

        InternalExtendedStats stats = (InternalExtendedStats) translator.toInternalAggregation(agg, values);

        assertEquals(3.0, stats.getSigma(), 0.001);
        // upper bound = avg + sigma * stddev = 55 + 3 * 30
        assertEquals(145.0, stats.getStdDeviationBound(InternalExtendedStats.Bounds.UPPER), 0.001);
    }

    public void testToInternalAggregationWithNull() {
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price");

        InternalAggregation result = translator.toInternalAggregation(agg, null);

        InternalExtendedStats stats = (InternalExtendedStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
        assertEquals(0.0, stats.getSumOfSquares(), 0.001);
        assertTrue(Double.isNaN(stats.getVariance()));
    }

    public void testToInternalAggregationWithPartialNulls() {
        ExtendedStatsAggregationBuilder agg = new ExtendedStatsAggregationBuilder("price_stats").field("price");
        Map<String, Object> values = Map.of("price_stats_count", 5L, "price_stats_sum", 100.0);

        InternalAggregation result = translator.toInternalAggregation(agg, values);

        InternalExtendedStats stats = (InternalExtendedStats) result;
        assertEquals(5, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(100.0, stats.getSum(), 0.001);
    }
}
