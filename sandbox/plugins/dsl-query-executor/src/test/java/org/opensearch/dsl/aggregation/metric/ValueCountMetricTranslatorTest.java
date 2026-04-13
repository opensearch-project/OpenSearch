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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ValueCountMetricTranslator.
 */
public class ValueCountMetricTranslatorTest {

    private ValueCountMetricTranslator translator;
    private AggregationConversionContext context;
    private RelDataTypeFactory typeFactory;

    @Before
    public void setUp() {
        translator = new ValueCountMetricTranslator();
        context = mock(AggregationConversionContext.class);
        typeFactory = mock(RelDataTypeFactory.class);

        RelDataType bigintType = mock(RelDataType.class);
        when(typeFactory.createSqlType(SqlTypeName.BIGINT)).thenReturn(bigintType);
        when(context.getTypeFactory()).thenReturn(typeFactory);
    }

    @Test
    public void testGetAggregationType() {
        assertEquals(ValueCountAggregationBuilder.class, translator.getAggregationType());
    }

    @Test
    public void testToAggregateCallReturnsCountFunction() throws Exception {
        RelDataType fieldType = mock(RelDataType.class);
        RelDataTypeField field = new RelDataTypeFieldImpl("price", 2, fieldType);
        RelRecordType rowType = mock(RelRecordType.class);
        when(rowType.getField("price", true, false)).thenReturn(field);
        when(context.getRowType()).thenReturn(rowType);

        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("price_count").field("price");

        AggregateCall call = translator.toAggregateCall(agg, context);

        assertNotNull(call);
        assertEquals(SqlStdOperatorTable.COUNT, call.getAggregation());
    }

    @Test(expected = ConversionException.class)
    public void testToAggregateCallInvalidField() throws Exception {
        RelRecordType rowType = mock(RelRecordType.class);
        when(rowType.getField("invalid", true, false)).thenReturn(null);
        when(context.getRowType()).thenReturn(rowType);

        ValueCountAggregationBuilder agg = new ValueCountAggregationBuilder("count").field("invalid");

        translator.toAggregateCall(agg, context);
    }

    @Test
    public void testToInternalAggregationWithValidValue() {
        InternalAggregation result = translator.toInternalAggregation("price_count", 42L);

        assertNotNull(result);
        assertTrue(result instanceof InternalValueCount);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(42L, count.getValue());
    }

    @Test
    public void testToInternalAggregationWithZero() {
        InternalAggregation result = translator.toInternalAggregation("price_count", 0L);

        assertNotNull(result);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(0L, count.getValue());
    }

    @Test
    public void testToInternalAggregationWithNull() {
        InternalAggregation result = translator.toInternalAggregation("price_count", null);

        assertNotNull(result);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(0L, count.getValue());
    }

    @Test
    public void testToInternalAggregationWithNumberType() {
        // Test with Integer instead of Long
        InternalAggregation result = translator.toInternalAggregation("price_count", 100);

        assertNotNull(result);
        InternalValueCount count = (InternalValueCount) result;
        assertEquals(100L, count.getValue());
    }
}
