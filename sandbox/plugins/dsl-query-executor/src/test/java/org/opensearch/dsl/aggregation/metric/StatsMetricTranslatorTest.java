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
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StatsMetricTranslator.
 * Tests conversion of Stats aggregation to Calcite AggregateCalls and back to InternalStats.
 */
public class StatsMetricTranslatorTest {

    private StatsMetricTranslator translator;
    private AggregationConversionContext context;
    private RelDataTypeFactory typeFactory;

    @Before
    public void setUp() {
        translator = new StatsMetricTranslator();
        context = mock(AggregationConversionContext.class);
        typeFactory = mock(RelDataTypeFactory.class);

        RelDataType bigintType = mock(RelDataType.class);
        RelDataType doubleType = mock(RelDataType.class);
        when(typeFactory.createSqlType(SqlTypeName.BIGINT)).thenReturn(bigintType);
        when(typeFactory.createSqlType(SqlTypeName.DOUBLE)).thenReturn(doubleType);
        when(context.getTypeFactory()).thenReturn(typeFactory);
    }

    @Test
    public void testGetAggregationType() {
        assertEquals(StatsAggregationBuilder.class, translator.getAggregationType());
    }

    @Test
    public void testToAggregateCalls() throws Exception {
        RelDataType fieldType = mock(RelDataType.class);
        RelDataTypeField field = new RelDataTypeFieldImpl("price", 2, fieldType);
        RelRecordType rowType = mock(RelRecordType.class);
        when(rowType.getField("price", true, false)).thenReturn(field);
        when(context.getRowType()).thenReturn(rowType);

        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        List<AggregateCall> calls = translator.toAggregateCalls(agg, context);

        assertEquals(4, calls.size());
        assertEquals(SqlStdOperatorTable.COUNT, calls.get(0).getAggregation());
        assertEquals(SqlStdOperatorTable.MIN, calls.get(1).getAggregation());
        assertEquals(SqlStdOperatorTable.MAX, calls.get(2).getAggregation());
        assertEquals(SqlStdOperatorTable.SUM, calls.get(3).getAggregation());
    }

    @Test(expected = ConversionException.class)
    public void testToAggregateCallsInvalidField() throws Exception {
        RelRecordType rowType = mock(RelRecordType.class);
        when(rowType.getField("invalid", true, false)).thenReturn(null);
        when(context.getRowType()).thenReturn(rowType);

        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("invalid");

        translator.toAggregateCalls(agg, context);
    }

    @Test
    public void testGetAggregateFieldNames() {
        StatsAggregationBuilder agg = new StatsAggregationBuilder("price_stats").field("price");

        List<String> names = translator.getAggregateFieldNames(agg);

        assertEquals(4, names.size());
        assertEquals("price_stats_count", names.get(0));
        assertEquals("price_stats_min", names.get(1));
        assertEquals("price_stats_max", names.get(2));
        assertEquals("price_stats_sum", names.get(3));
    }

    @Test
    public void testBuildEmptyAggregation() {
        InternalAggregation result = translator.buildEmptyAggregation("price_stats");

        assertNotNull(result);
        assertTrue(result instanceof InternalStats);
        InternalStats stats = (InternalStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
    }

    @Test
    public void testToInternalAggregationWithValidValues() {
        List<Object> values = Arrays.asList(10L, 5.0, 100.0, 550.0);

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        assertTrue(result instanceof InternalStats);
        InternalStats stats = (InternalStats) result;
        assertEquals(10, stats.getCount());
        assertEquals(5.0, stats.getMin(), 0.001);
        assertEquals(100.0, stats.getMax(), 0.001);
        assertEquals(550.0, stats.getSum(), 0.001);
    }

    @Test
    public void testToInternalAggregationWithNullList() {
        InternalAggregation result = translator.toInternalAggregation("price_stats", null);

        assertNotNull(result);
        assertTrue(result instanceof InternalStats);
        InternalStats stats = (InternalStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
    }

    @Test
    public void testToInternalAggregationWithWrongSize() {
        List<Object> values = Arrays.asList(10L, 5.0);

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        InternalStats stats = (InternalStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
    }

    @Test
    public void testToInternalAggregationWithNullValues() {
        List<Object> values = Arrays.asList(null, null, null, null);

        InternalAggregation result = translator.toInternalAggregation("price_stats", values);

        assertNotNull(result);
        InternalStats stats = (InternalStats) result;
        assertEquals(0, stats.getCount());
        assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0.001);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0.001);
        assertEquals(0.0, stats.getSum(), 0.001);
    }
}
