/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

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

import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.*;

public class HistogramGroupingTest {

    private RexBuilder rexBuilder;
    private RelDataType inputRowType;

    @Before
    public void setUp() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        inputRowType = new RelRecordType(List.of(
            new RelDataTypeFieldImpl("price", 0, doubleType)
        ));
    }

    @Test
    public void testGetFieldNames() {
        HistogramGrouping grouping = new HistogramGrouping("test_histogram", "price", 100.0, 0.0);
        assertEquals(List.of("price"), grouping.getFieldNames());
    }

    @Test
    public void testGetProjectedColumnName() {
        HistogramGrouping grouping = new HistogramGrouping("test_histogram", "price", 100.0, 0.0);
        assertEquals("test_histogram$$price$$d7efe4f7$$histogram_bucket", grouping.getProjectedColumnName());
    }

    @Test
    public void testBuildExpression() throws ConversionException {
        HistogramGrouping grouping = new HistogramGrouping("test_histogram", "price", 100.0, 5.0);
        RexNode expression = grouping.buildExpression(inputRowType, rexBuilder);

        assertNotNull(expression);
        assertTrue(expression instanceof RexCall);
        RexCall outerCall = (RexCall) expression;
        assertEquals(SqlStdOperatorTable.PLUS, outerCall.getOperator());
        assertEquals(2, outerCall.getOperands().size());

        RexNode multiplyNode = outerCall.getOperands().get(0);
        assertTrue(multiplyNode instanceof RexCall);
        assertEquals(SqlStdOperatorTable.MULTIPLY, ((RexCall) multiplyNode).getOperator());

        RexNode floorNode = ((RexCall) multiplyNode).getOperands().get(0);
        assertTrue(floorNode instanceof RexCall);
        assertEquals(SqlStdOperatorTable.FLOOR, ((RexCall) floorNode).getOperator());

        RexNode divideNode = ((RexCall) floorNode).getOperands().get(0);
        assertTrue(divideNode instanceof RexCall);
        assertEquals(SqlStdOperatorTable.DIVIDE, ((RexCall) divideNode).getOperator());

        RexNode minusNode = ((RexCall) divideNode).getOperands().get(0);
        assertTrue(minusNode instanceof RexCall);
        assertEquals(SqlStdOperatorTable.MINUS, ((RexCall) minusNode).getOperator());

        assertTrue(((RexCall) multiplyNode).getOperands().get(1).toString().startsWith("100.0"));
        assertTrue(outerCall.getOperands().get(1).toString().startsWith("5.0"));
    }

    @Test(expected = ConversionException.class)
    public void testBuildExpressionWithInvalidField() throws ConversionException {
        HistogramGrouping grouping = new HistogramGrouping("test_histogram", "invalid_field", 100.0, 0.0);
        grouping.buildExpression(inputRowType, rexBuilder);
    }
}

