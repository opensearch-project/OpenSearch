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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RustUdfDateTimeAdapters.TimeFormatAdapter}. The adapter must coerce a
 * stock-Calcite {@code TIME} first operand to {@code TIMESTAMP} before isthmus serialises it,
 * because substrait-java 0.89.1 has no {@code precision_time<P>} signature in the yaml. Other
 * operand kinds (TIMESTAMP, DATE, VARCHAR) bind directly and must pass through unchanged.
 */
public class TimeFormatAdapterTests extends OpenSearchTestCase {

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

    private RelDataType nullable(SqlTypeName t) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(t), true);
    }

    private RexCall timeFormatCall(SqlTypeName firstOperandType) {
        SqlFunction op = new SqlFunction(
            "TIME_FORMAT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode value = rexBuilder.makeInputRef(nullable(firstOperandType), 0);
        RexNode fmt = rexBuilder.makeLiteral("%H:%i:%s", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        return (RexCall) rexBuilder.makeCall(nullable(SqlTypeName.VARCHAR), op, List.of(value, fmt));
    }

    /**
     * TIME first operand triggers the today-anchor coercion: the rewritten call's first operand
     * is TIMESTAMP-typed, so isthmus binds the {@code time_format(precision_timestamp<P>, string)}
     * yaml arm instead of failing on {@code precision_time<P>}.
     */
    public void testTimeOperandCoercedToTimestamp() {
        RexCall call = timeFormatCall(SqlTypeName.TIME);
        RexNode result = new RustUdfDateTimeAdapters.TimeFormatAdapter().adapt(call, List.of(), cluster);

        assertTrue("rewrite must produce a RexCall", result instanceof RexCall);
        RexCall rewritten = (RexCall) result;
        assertSame("operator must remain time_format", RustUdfDateTimeAdapters.LOCAL_TIME_FORMAT_OP, rewritten.getOperator());
        assertEquals(
            "first operand must be TIMESTAMP after coercion",
            SqlTypeName.TIMESTAMP,
            rewritten.getOperands().get(0).getType().getSqlTypeName()
        );
        assertEquals("format-string operand must pass through unchanged at index 1", 2, rewritten.getOperands().size());
    }

    /** TIMESTAMP first operand binds directly to the yaml — no coercion, parent {@code adapt} runs. */
    public void testTimestampOperandPassesThroughUnchanged() {
        RexCall call = timeFormatCall(SqlTypeName.TIMESTAMP);
        RexNode result = new RustUdfDateTimeAdapters.TimeFormatAdapter().adapt(call, List.of(), cluster);

        RexCall rewritten = (RexCall) result;
        assertSame(RustUdfDateTimeAdapters.LOCAL_TIME_FORMAT_OP, rewritten.getOperator());
        assertEquals(
            "TIMESTAMP operand must remain TIMESTAMP",
            SqlTypeName.TIMESTAMP,
            rewritten.getOperands().get(0).getType().getSqlTypeName()
        );
    }

    /** VARCHAR first operand must not trip the TIME coercion path. */
    public void testVarcharOperandSkipsCoercion() {
        RexCall call = timeFormatCall(SqlTypeName.VARCHAR);
        RexNode result = new RustUdfDateTimeAdapters.TimeFormatAdapter().adapt(call, List.of(), cluster);

        RexCall rewritten = (RexCall) result;
        assertSame(RustUdfDateTimeAdapters.LOCAL_TIME_FORMAT_OP, rewritten.getOperator());
        assertEquals(
            "VARCHAR operand must remain VARCHAR (not coerced to TIMESTAMP)",
            SqlTypeName.VARCHAR,
            rewritten.getOperands().get(0).getType().getSqlTypeName()
        );
    }
}
