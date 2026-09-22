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

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link RustUdfDateTimeAdapters.OsYearweekAdapter}. The adapter must (a) coerce a
 * VARCHAR / TIME first operand to TIMESTAMP via {@link DatePartAdapters#coerceCharacterOperandToTimestamp}
 * before binding the {@code precision_timestamp<P>} signature, and (b) preserve a 2-arg call's mode
 * operand unchanged. The 0-arg branch falls back to the parent {@code AbstractNameMappingAdapter}.
 */
public class OsYearweekAdapterTests extends OpenSearchTestCase {

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

    private RexCall yearweekCall(List<RexNode> operands, int operandTypeCount) {
        SqlFunction op = new SqlFunction(
            "YEARWEEK",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            operandTypeCount == 2 ? OperandTypes.ANY_ANY : OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        return (RexCall) rexBuilder.makeCall(nullable(SqlTypeName.INTEGER), op, operands);
    }

    /**
     * 1-arg form with a VARCHAR operand: the rewrite must (a) replace YEARWEEK with the os_yearweek
     * UDF op, and (b) replace the raw VARCHAR operand with a TIMESTAMP-typed expression.
     */
    public void testYearweekVarcharOperandCoercedToTimestamp() {
        RexNode lit = rexBuilder.makeLiteral("2003-10-03", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall call = yearweekCall(List.of(lit), 1);

        RexNode result = new RustUdfDateTimeAdapters.OsYearweekAdapter().adapt(call, List.of(), cluster);

        assertTrue("YEARWEEK must be rewritten", result instanceof RexCall);
        RexCall rewritten = (RexCall) result;
        assertSame("rewrite must target os_yearweek", RustUdfDateTimeAdapters.LOCAL_OS_YEARWEEK_OP, rewritten.getOperator());
        assertEquals(
            "coerced operand must be TIMESTAMP-typed",
            SqlTypeName.TIMESTAMP,
            rewritten.getOperands().get(0).getType().getSqlTypeName()
        );
    }

    /**
     * 2-arg form: the date operand is coerced, but the integer mode operand passes through unchanged
     * — its type must NOT be transformed and the value must be preserved.
     */
    public void testYearweekModeOperandPassesThroughUnchanged() {
        RexNode dateLit = rexBuilder.makeLiteral("2003-10-03", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode mode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(3), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall call = yearweekCall(List.of(dateLit, mode), 2);

        RexNode result = new RustUdfDateTimeAdapters.OsYearweekAdapter().adapt(call, List.of(), cluster);

        RexCall rewritten = (RexCall) result;
        assertSame(RustUdfDateTimeAdapters.LOCAL_OS_YEARWEEK_OP, rewritten.getOperator());
        assertEquals("mode operand must be preserved at index 1", 2, rewritten.getOperands().size());
        assertSame("mode operand must be the same RexNode (identity), not a re-wrapped copy", mode, rewritten.getOperands().get(1));
    }

    /** TIMESTAMP first operand must skip the coercion path and route directly to os_yearweek. */
    public void testYearweekTimestampOperandTargetsOsYearweek() {
        RexNode tsRef = rexBuilder.makeInputRef(nullable(SqlTypeName.TIMESTAMP), 0);
        RexCall call = yearweekCall(List.of(tsRef), 1);

        RexNode result = new RustUdfDateTimeAdapters.OsYearweekAdapter().adapt(call, List.of(), cluster);

        RexCall rewritten = (RexCall) result;
        assertSame(RustUdfDateTimeAdapters.LOCAL_OS_YEARWEEK_OP, rewritten.getOperator());
        assertEquals(
            "TIMESTAMP operand must remain TIMESTAMP after coercion (no-op)",
            SqlTypeName.TIMESTAMP,
            rewritten.getOperands().get(0).getType().getSqlTypeName()
        );
    }

    /**
     * 0-arg call must fall back to the parent name-mapping adapter — coercion logic only runs when
     * there is at least one operand to coerce.
     */
    public void testYearweekZeroArgFallsThroughToParent() {
        RexCall call = (RexCall) rexBuilder.makeCall(
            nullable(SqlTypeName.INTEGER),
            new SqlFunction(
                "YEARWEEK",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER_NULLABLE,
                null,
                OperandTypes.NILADIC,
                SqlFunctionCategory.TIMEDATE
            ),
            List.of()
        );
        RexNode result = new RustUdfDateTimeAdapters.OsYearweekAdapter().adapt(call, List.of(), cluster);
        assertTrue("0-arg YEARWEEK should still produce a RexCall (parent's name-mapping result)", result instanceof RexCall);
        assertSame(RustUdfDateTimeAdapters.LOCAL_OS_YEARWEEK_OP, ((RexCall) result).getOperator());
    }
}
