/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.List;

import static org.junit.Assert.*;

public class DateHistogramGroupingTest {

    private RexBuilder rexBuilder;
    private RelDataType inputRowType;

    @Before
    public void setUp() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        inputRowType = new RelRecordType(List.of(
            new RelDataTypeFieldImpl("timestamp", 0, timestampType)
        ));
    }

    @Test
    public void testGetFieldNames() {
        DateHistogramGrouping grouping = new DateHistogramGrouping("test_date_histogram", "timestamp", DateHistogramInterval.MONTH, null);
        assertEquals(List.of("timestamp"), grouping.getFieldNames());
    }

    @Test
    public void testGetProjectedColumnName() {
        DateHistogramGrouping grouping = new DateHistogramGrouping("test_date_histogram", "timestamp", DateHistogramInterval.MONTH, null);
        assertEquals("test_date_histogram$$timestamp$$1d826efa$$date_histogram_bucket", grouping.getProjectedColumnName());
    }

    @Test
    public void testBuildExpressionWithCalendarInterval() throws ConversionException {
        DateHistogramGrouping grouping = new DateHistogramGrouping("test_date_histogram", "timestamp", DateHistogramInterval.MONTH, null);
        RexNode expression = grouping.buildExpression(inputRowType, rexBuilder);

        assertNotNull(expression);
        assertTrue(expression instanceof RexCall);
        RexCall call = (RexCall) expression;
        assertEquals(SqlStdOperatorTable.FLOOR, call.getOperator());
        assertEquals(2, call.getOperands().size());
    }

    @Test
    public void testBuildExpressionWithFixedInterval() throws ConversionException {
        DateHistogramGrouping grouping = new DateHistogramGrouping("test_date_histogram", "timestamp", null, DateHistogramInterval.hours(2));
        RexNode expression = grouping.buildExpression(inputRowType, rexBuilder);

        assertNotNull(expression);
        assertTrue(expression instanceof RexCall);
        RexCall outerCall = (RexCall) expression;
        assertEquals(SqlStdOperatorTable.MULTIPLY, outerCall.getOperator());
        assertEquals(2, outerCall.getOperands().size());

        RexNode floorNode = outerCall.getOperands().get(0);
        assertTrue(floorNode instanceof RexCall);
        assertEquals(SqlStdOperatorTable.FLOOR, ((RexCall) floorNode).getOperator());

        RexNode divideNode = ((RexCall) floorNode).getOperands().get(0);
        assertTrue(divideNode instanceof RexCall);
        assertEquals(SqlStdOperatorTable.DIVIDE, ((RexCall) divideNode).getOperator());
    }

    @Test(expected = ConversionException.class)
    public void testBuildExpressionWithInvalidField() throws ConversionException {
        DateHistogramGrouping grouping = new DateHistogramGrouping("test_date_histogram", "invalid_field", DateHistogramInterval.MONTH, null);
        grouping.buildExpression(inputRowType, rexBuilder);
    }
}
