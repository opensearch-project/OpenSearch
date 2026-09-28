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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link CoalesceAdapter}.
 */
public class CoalesceAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private RelDataType nullableVarchar;
    private RelDataType notNullVarchar;
    private RelDataType nullableBigint;

    private final CoalesceAdapter adapter = new CoalesceAdapter();
    private SqlOperator coalesce;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);

        nullableVarchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        notNullVarchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        nullableBigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        coalesce = new SqlFunction(
            "COALESCE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(nullableVarchar),
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.SYSTEM
        );
    }

    /** Builds an COALESCE call over the provided operand types. */
    private RexCall buildCoalesce(RelDataType... operandTypes) {
        List<RexNode> operands = new ArrayList<>(operandTypes.length);
        for (int i = 0; i < operandTypes.length; i++) {
            operands.add(rexBuilder.makeInputRef(operandTypes[i], i));
        }
        RelDataType returnType = typeFactory.leastRestrictive(List.of(operandTypes));
        if (returnType == null) {
            returnType = nullableVarchar;
        }
        return (RexCall) rexBuilder.makeCall(returnType, coalesce, operands);
    }

    // ── operator identity rewrite ──────────────────────────────────────────

    public void testAdaptRewritesOperatorToStockCoalesce() {
        RexCall original = buildCoalesce(nullableVarchar, nullableVarchar);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("expected RexCall, got " + adapted.getClass().getSimpleName(), adapted instanceof RexCall);
        RexCall rewritten = (RexCall) adapted;
        assertSame(
            "operator must be rewritten to stock SqlStdOperatorTable.COALESCE — isthmus' default "
                + "catalog binds this constant to substrait's coalesce, the UDF binding is opaque",
            SqlStdOperatorTable.COALESCE,
            rewritten.getOperator()
        );
    }

    // ── type preservation ──────────────────────────────────────────────────

    public void testAdaptPreservesOriginalReturnType() {
        RexCall original = buildCoalesce(nullableVarchar, nullableVarchar);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertEquals(
            "rewritten COALESCE must declare the same return type as the original ENHANCED_COALESCE",
            original.getType(),
            adapted.getType()
        );
    }

    // ── uniform-typed happy path ───────────────────────────────────────────

    public void testAdaptOnUniformTypeOperandsSkipsCast() {
        RexCall original = buildCoalesce(nullableVarchar, nullableVarchar, nullableVarchar);
        RexCall rewritten = (RexCall) adapter.adapt(original, List.of(), cluster);

        // No CAST inserted: operand list must reference-equal the original operands.
        assertEquals(3, rewritten.getOperands().size());
        for (int i = 0; i < 3; i++) {
            assertSame(
                "uniform-typed operand " + i + " must pass through without CAST",
                original.getOperands().get(i),
                rewritten.getOperands().get(i)
            );
        }
    }

    // ── mixed-type coercion ────────────────────────────────────────────────

    public void testAdaptOnMixedTypeOperandsInsertsCastToLeastRestrictive() {
        // VARCHAR + BIGINT + VARCHAR: leastRestrictive → VARCHAR (string wins over numeric in
        // Calcite's common-type resolution). The BIGINT operand must get a CAST to VARCHAR
        RexCall original = buildCoalesce(nullableVarchar, nullableBigint, nullableVarchar);
        RexCall rewritten = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals(3, rewritten.getOperands().size());
        assertSame(
            "VARCHAR operand [0] already matches leastRestrictive — no CAST needed",
            original.getOperands().get(0),
            rewritten.getOperands().get(0)
        );

        RexNode coercedMiddle = rewritten.getOperands().get(1);
        assertTrue("BIGINT operand must be wrapped in CAST", coercedMiddle instanceof RexCall);
        RexCall castCall = (RexCall) coercedMiddle;
        assertEquals("wrapper must be CAST", SqlKind.CAST, castCall.getKind());
        assertEquals(
            "CAST target must be VARCHAR family (leastRestrictive resolution of VARCHAR + BIGINT)",
            SqlTypeName.VARCHAR,
            castCall.getType().getSqlTypeName()
        );
        assertSame("CAST must wrap the original BIGINT operand", original.getOperands().get(1), castCall.getOperands().get(0));

        assertSame(
            "VARCHAR operand [2] already matches leastRestrictive — no CAST needed",
            original.getOperands().get(2),
            rewritten.getOperands().get(2)
        );
    }

    // ── nullability unification in leastRestrictive ────────────────────────

    public void testAdaptOnNullablePlusNotNullVarcharUnifiesToNullable() {
        // leastRestrictive(nullable VARCHAR, NOT NULL VARCHAR) = nullable VARCHAR (nullability is
        // widened to accept both). The NOT NULL operand gets CAST to nullable so both operands
        // share a type.
        RexCall original = buildCoalesce(nullableVarchar, notNullVarchar);
        RexCall rewritten = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals(2, rewritten.getOperands().size());
        assertSame(
            "already-nullable operand [0] must pass through untouched",
            original.getOperands().get(0),
            rewritten.getOperands().get(0)
        );

        RexNode coerced = rewritten.getOperands().get(1);
        assertTrue("NOT NULL VARCHAR operand must be wrapped in CAST to nullable-VARCHAR", coerced instanceof RexCall);
        assertEquals(SqlKind.CAST, coerced.getKind());
        assertTrue("CAST target must be nullable (leastRestrictive of nullable+not-null is nullable)", coerced.getType().isNullable());
    }

    // ── empty-operand pass-through ─────────────────────────────────────────

    public void testAdaptOnEmptyOperandListPassesThrough() {
        RexCall original = (RexCall) rexBuilder.makeCall(nullableVarchar, coalesce, List.of());
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("empty-operand call must pass through unmodified", original, adapted);
    }
}
