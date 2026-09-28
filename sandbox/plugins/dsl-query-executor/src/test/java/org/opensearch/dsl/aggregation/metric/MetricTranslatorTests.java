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

import java.util.Map;

public class MetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testAvgTranslator() throws ConversionException {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new AvgAggregationBuilder("avg_price").field("price"), ctx.getRowType());

        assertEquals(SqlKind.AVG, call.getAggregation().getKind());
        assertEquals("avg_price", call.getName());
        assertEquals(1, call.getArgList().size());
        assertEquals(1, call.getArgList().get(0).intValue()); // price is index 1
    }

    public void testSumTranslator() throws ConversionException {
        SumMetricTranslator translator = new SumMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new SumAggregationBuilder("total").field("price"), ctx.getRowType());

        assertEquals(SqlKind.SUM, call.getAggregation().getKind());
        assertEquals("total", call.getName());
    }

    public void testMinTranslator() throws ConversionException {
        MinMetricTranslator translator = new MinMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new MinAggregationBuilder("min_price").field("price"), ctx.getRowType());

        assertEquals(SqlKind.MIN, call.getAggregation().getKind());
        assertEquals("min_price", call.getName());
    }

    public void testMaxTranslator() throws ConversionException {
        MaxMetricTranslator translator = new MaxMetricTranslator();
        AggregateCall call = translator.toAggregateCall(new MaxAggregationBuilder("max_price").field("price"), ctx.getRowType());

        assertEquals(SqlKind.MAX, call.getAggregation().getKind());
        assertEquals("max_price", call.getName());
    }

    public void testThrowsForUnknownField() {
        AvgMetricTranslator translator = new AvgMetricTranslator();

        expectThrows(
            ConversionException.class,
            () -> translator.toAggregateCall(new AvgAggregationBuilder("bad").field("nonexistent"), ctx.getRowType())
        );
    }

    public void testAggregateFieldName() {
        AvgMetricTranslator translator = new AvgMetricTranslator();
        assertEquals("avg_price", translator.getAggregateFieldName(new AvgAggregationBuilder("avg_price").field("price")));
    }

    /** User-supplied meta must be echoed back on the response aggregation, like classic search. */
    public void testMetadataEchoedInInternalAggregation() {
        Map<String, Object> meta = Map.of("owner", "pricing-team", "revision", 3);

        assertEquals(meta, new AvgMetricTranslator().toInternalAggregation("a", 1.0, meta).getMetadata());
        assertEquals(meta, new SumMetricTranslator().toInternalAggregation("s", 1.0, meta).getMetadata());
        assertEquals(meta, new MinMetricTranslator().toInternalAggregation("mn", 1.0, meta).getMetadata());
        assertEquals(meta, new MaxMetricTranslator().toInternalAggregation("mx", 1.0, meta).getMetadata());

        // Echoed even when the metric has no value (empty result)
        assertEquals(meta, new AvgMetricTranslator().toInternalAggregation("a", null, meta).getMetadata());
    }

    /** Requests without meta keep rendering without a meta section. */
    public void testNullMetadataStaysNull() {
        assertNull(new AvgMetricTranslator().toInternalAggregation("a", 1.0, null).getMetadata());
        assertNull(new MaxMetricTranslator().toInternalAggregation("mx", null, null).getMetadata());
    }
}
