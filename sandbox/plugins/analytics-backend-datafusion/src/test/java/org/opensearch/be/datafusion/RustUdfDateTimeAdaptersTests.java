/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link RustUdfDateTimeAdapters}. Pins the {@code opensearch_extract}
 * rename and the TIME→VARCHAR coercion in {@link RustUdfDateTimeAdapters.ExtractAdapter}.
 */
public class RustUdfDateTimeAdaptersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /** Pins the rename: substrait core's enum-arg `extract` would crash isthmus if we shared the name. */
    public void testExtractOperatorNameAvoidsSubstraitCoreCollision() {
        assertEquals("opensearch_extract", RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP.getName());
    }

    /** {@code extract(<unit> FROM <TIME(p)>)} value operand must be CAST to VARCHAR (precision_time has no yaml overload). */
    public void testExtractAdapterCastsTimeOperandToVarchar() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType time9 = typeFactory.createSqlType(SqlTypeName.TIME, 9);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode unit = rexBuilder.makeLiteral("MINUTE", varchar, true);
        RexNode timeVal = rexBuilder.makeInputRef(time9, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, List.of(unit, timeVal));

        RexNode adapted = new RustUdfDateTimeAdapters.ExtractAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertEquals(unit, adaptedCall.getOperands().get(0));

        RexNode coercedValue = adaptedCall.getOperands().get(1);
        assertEquals(SqlTypeName.VARCHAR, coercedValue.getType().getSqlTypeName());
        assertTrue(coercedValue instanceof RexCall && ((RexCall) coercedValue).getKind() == SqlKind.CAST);
    }

    /** Non-TIME operands (DATE / TIMESTAMP / VARCHAR) pass through unchanged. */
    public void testExtractAdapterDoesNotCoerceTimestampOperand() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType timestamp6 = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode unit = rexBuilder.makeLiteral("YEAR", varchar, true);
        RexNode tsVal = rexBuilder.makeInputRef(timestamp6, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, List.of(unit, tsVal));

        RexNode adapted = new RustUdfDateTimeAdapters.ExtractAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertSame(tsVal, adaptedCall.getOperands().get(1));
    }
}
