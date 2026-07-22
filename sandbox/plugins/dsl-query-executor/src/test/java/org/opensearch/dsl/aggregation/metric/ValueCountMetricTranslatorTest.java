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
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class ValueCountMetricTranslatorTest extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetAggregationType() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        assertEquals(ValueCountAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCallsReturnsCountFunction() throws ConversionException {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(1, calls.size());
        AggregateCall call = calls.get(0);
        assertEquals(SqlKind.COUNT, call.getAggregation().getKind());
        assertEquals("price_count", call.getName());
    }

    public void testToAggregateCallsInvalidField() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("count").field("invalid");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testGetAggregateFieldNames() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(1, names.size());
        assertEquals("price_count", names.get(0));
    }

    public void testToInternalAggregationWithValidValue() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        Map<String, Object> values = Map.of("price_count", 42L);

        InternalAggregation result = translator.toInternalAggregation("price_count", values);

        assertNotNull(result);
        assertTrue(result instanceof InternalValueCount);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(42L, count.getValue());
    }

    public void testToInternalAggregationWithZero() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        Map<String, Object> values = Map.of("price_count", 0L);

        InternalAggregation result = translator.toInternalAggregation("price_count", values);

        assertNotNull(result);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(0L, count.getValue());
    }

    public void testToInternalAggregationWithNull() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();

        InternalAggregation result = translator.toInternalAggregation("price_count", null);

        assertNotNull(result);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(0L, count.getValue());
    }

    public void testToInternalAggregationWithNumberType() {
        ValueCountMetricTranslator translator = new ValueCountMetricTranslator();
        Map<String, Object> values = Map.of("price_count", 100);

        InternalAggregation result = translator.toInternalAggregation("price_count", values);

        assertNotNull(result);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(100L, count.getValue());
    }
}
