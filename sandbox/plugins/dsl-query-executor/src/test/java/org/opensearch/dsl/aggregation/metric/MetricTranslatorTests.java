/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
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

    /**
     * Every metric translator declares the input column's OWN type — {@code sum} included. This is the
     * "widened exactly once" pin: {@code DslTypeSystems.NANO_TIMESTAMP} widens a {@code SUM} to the
     * engine's accumulator width, and the ONE place that widening is applied to a declared type is
     * {@code AggregationMetadataBuilder#build} (see
     * {@code AggregationMetadataBuilderTests#testSumOverAnIntegerColumnIsDeclaredBigint}). A translator
     * that also widened would apply {@code deriveSumType} twice on the same call, so this test fails if
     * that second mechanism is ever reintroduced here.
     */
    public void testEveryMetricDeclaresTheInputColumnType() throws ConversionException {
        RelDataType priceType = ctx.getRowType().getField("price", false, false).getType();
        assertEquals(SqlTypeName.INTEGER, priceType.getSqlTypeName());

        assertEquals(
            "sum must NOT widen here — AggregationMetadataBuilder is the single widening point",
            priceType,
            new SumMetricTranslator().toAggregateCall(new SumAggregationBuilder("s").field("price"), ctx.getRowType()).getType()
        );
        assertEquals(
            priceType,
            new AvgMetricTranslator().toAggregateCall(new AvgAggregationBuilder("a").field("price"), ctx.getRowType()).getType()
        );
        assertEquals(
            priceType,
            new MinMetricTranslator().toAggregateCall(new MinAggregationBuilder("mn").field("price"), ctx.getRowType()).getType()
        );
        assertEquals(
            priceType,
            new MaxMetricTranslator().toAggregateCall(new MaxAggregationBuilder("mx").field("price"), ctx.getRowType()).getType()
        );
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
