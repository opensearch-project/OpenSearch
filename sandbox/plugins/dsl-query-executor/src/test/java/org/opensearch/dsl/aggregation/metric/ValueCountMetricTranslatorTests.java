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

public class ValueCountMetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();
    private final ValueCountMetricTranslator translator = new ValueCountMetricTranslator();

    public void testGetAggregationType() {
        assertEquals(ValueCountAggregationBuilder.class, translator.getAggregationType());
    }

    public void testToAggregateCallsReturnsCountFunction() throws ConversionException {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(1, calls.size());
        AggregateCall call = calls.get(0);
        assertEquals(SqlKind.COUNT, call.getAggregation().getKind());
        assertEquals("price_count", call.getName());
    }

    public void testToAggregateCallsInvalidField() {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("count").field("invalid");

        expectThrows(ConversionException.class, () -> translator.toAggregateCalls(agg, ctx.getRowType()));
    }

    public void testGetAggregateFieldNames() {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(List.of("price_count"), names);
    }

    public void testToInternalAggregationWithValidValue() {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        InternalAggregation result = translator.toInternalAggregation(agg, Map.of("price_count", 42L));

        InternalValueCount count = (InternalValueCount) result;
        assertEquals(42L, count.getValue());
    }

    public void testToInternalAggregationWithZero() {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        InternalAggregation result = translator.toInternalAggregation(agg, Map.of("price_count", 0L));

        InternalValueCount count = (InternalValueCount) result;
        assertEquals(0L, count.getValue());
    }

    public void testToInternalAggregationWithNull() {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        InternalAggregation result = translator.toInternalAggregation(agg, null);

        InternalValueCount count = (InternalValueCount) result;
        assertEquals(0L, count.getValue());
    }

    public void testToInternalAggregationWithNumberType() {
        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        InternalAggregation result = translator.toInternalAggregation(agg, Map.of("price_count", 100));

        InternalValueCount count = (InternalValueCount) result;
        assertEquals(100L, count.getValue());
    }
}
