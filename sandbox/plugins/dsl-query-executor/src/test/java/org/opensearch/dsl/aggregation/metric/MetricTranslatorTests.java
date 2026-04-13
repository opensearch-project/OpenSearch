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
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

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

        expectThrows(ConversionException.class,
            () -> translator.toAggregateCalls(new AvgAggregationBuilder("bad").field("nonexistent"), ctx.getRowType()));
    }

    public void testAggregateFieldName() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        List<String> names = translator.getAggregateFieldNames(new AvgAggregationBuilder("avg_price").field("price"));
        assertEquals(1, names.size());
        assertEquals("avg_price", names.get(0));
    }
}
