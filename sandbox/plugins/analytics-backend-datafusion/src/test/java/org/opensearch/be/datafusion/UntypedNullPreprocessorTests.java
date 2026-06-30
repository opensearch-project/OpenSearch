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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link UntypedNullPreprocessor}. Constructs Calcite RelNode trees that contain
 * {@code SqlTypeName.NULL} literals in CASE branches and asserts the rewriter widens those
 * to typed nulls matching the CASE's resolved return type.
 */
public class UntypedNullPreprocessorTests extends OpenSearchTestCase {

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

    /**
     * The motivating shape: {@code COUNT(CASE WHEN cond THEN 1 ELSE NULL END)} — Calcite
     * leaves the implicit ELSE arm as {@link SqlTypeName#NULL}, which isthmus rejects.
     * After rewrite the ELSE literal must carry the CASE's resolved return type.
     */
    public void testCountEvalCaseRewritesElseNullToTypedNull() {
        // Build: VALUES(true) → Project(CASE WHEN $0 THEN 1 ELSE null END as col)
        RelDataType nullableInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        RelNode values = LogicalValues.createOneRow(cluster);
        RexNode boolLit = rexBuilder.makeLiteral(true, boolType);
        RexNode oneLit = rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE, nullableInt);
        // Untyped NULL — RexBuilder.constantNull() returns a literal whose type is NULL.
        RexNode untypedNull = rexBuilder.constantNull();
        // Sanity: the source literal is genuinely SqlTypeName.NULL.
        assertEquals(SqlTypeName.NULL, untypedNull.getType().getSqlTypeName());

        RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, boolLit, oneLit, untypedNull);
        RelDataType caseType = caseExpr.getType();
        // Calcite resolves the CASE return type to the leastRestrictive of {INT, NULL} — so
        // the CASE itself is already a nullable INT, but its untyped-NULL child operand is
        // what isthmus chokes on.
        assertEquals(SqlTypeName.INTEGER, caseType.getSqlTypeName());

        RelNode project = LogicalProject.create(values, List.of(), List.of(caseExpr), List.of("col"), java.util.Set.of());

        RelNode rewritten = UntypedNullPreprocessor.rewrite(project);

        // Walk the rewritten Project's only expression: the CASE's ELSE arm must now be a
        // typed null whose type matches the CASE's return type (nullable INT), not NULL.
        LogicalProject rewrittenProj = (LogicalProject) rewritten;
        RexCall rewrittenCase = (RexCall) rewrittenProj.getProjects().get(0);
        RexNode rewrittenElse = rewrittenCase.getOperands().get(2);
        assertTrue("ELSE arm must remain a literal", rewrittenElse instanceof RexLiteral);
        assertEquals(
            "ELSE arm type must be widened to the CASE return type, not NULL",
            SqlTypeName.INTEGER,
            rewrittenElse.getType().getSqlTypeName()
        );
        assertTrue("ELSE arm must still be null", ((RexLiteral) rewrittenElse).isNull());
    }

    /**
     * THEN-arm null is rewritten the same way (the operand layout treats odd-indexed
     * positions and the trailing operand as values; both can host an untyped NULL).
     */
    public void testCaseWithThenNullIsAlsoRewritten() {
        RelDataType nullableInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RelNode values = LogicalValues.createOneRow(cluster);

        RexNode boolLit = rexBuilder.makeLiteral(true, boolType);
        RexNode untypedNull = rexBuilder.constantNull();
        RexNode oneLit = rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE, nullableInt);

        // CASE WHEN cond THEN <untyped null> ELSE 1 END — value-arm at index 1.
        RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, boolLit, untypedNull, oneLit);
        RelNode project = LogicalProject.create(values, List.of(), List.of(caseExpr), List.of("col"), java.util.Set.of());

        RelNode rewritten = UntypedNullPreprocessor.rewrite(project);
        LogicalProject rewrittenProj = (LogicalProject) rewritten;
        RexCall rewrittenCase = (RexCall) rewrittenProj.getProjects().get(0);
        RexNode rewrittenThen = rewrittenCase.getOperands().get(1);
        assertEquals(
            "THEN arm null must also be widened to the CASE return type",
            SqlTypeName.INTEGER,
            rewrittenThen.getType().getSqlTypeName()
        );
    }

    /**
     * The condition operand at even indices (except the trailing else) is *not* a value
     * arm — leave it alone. (We don't expect untyped NULLs as conditions, but the operand
     * classifier should not touch even-index operands regardless.)
     */
    public void testCaseConditionOperandUnchanged() {
        RelDataType nullableInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RelNode values = LogicalValues.createOneRow(cluster);

        RexNode boolLit = rexBuilder.makeLiteral(true, boolType);
        RexNode oneLit = rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE, nullableInt);
        RexNode twoLit = rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(2), nullableInt);

        // CASE WHEN true THEN 1 ELSE 2 END — no untyped nulls; rewriter must be a no-op.
        RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, boolLit, oneLit, twoLit);
        RelNode project = LogicalProject.create(values, List.of(), List.of(caseExpr), List.of("col"), java.util.Set.of());

        RelNode rewritten = UntypedNullPreprocessor.rewrite(project);
        LogicalProject rewrittenProj = (LogicalProject) rewritten;
        RexCall rewrittenCase = (RexCall) rewrittenProj.getProjects().get(0);
        // Whole CASE expression is structurally unchanged when no untyped nulls are present
        // — the rewriter only fires on SqlTypeName.NULL operands.
        assertEquals("CASE expression should be unchanged when no untyped null is present", caseExpr.toString(), rewrittenCase.toString());
    }

    /**
     * End-to-end shape: the Project that motivates the rewrite usually feeds an Aggregate
     * (e.g. {@code COUNT(case_col)}). Verify the Aggregate over a rewritten Project
     * still type-checks and exposes the expected output schema.
     */
    public void testCountOverRewrittenCaseProjectionTypechecks() {
        RelDataType nullableInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RelNode values = LogicalValues.createOneRow(cluster);

        RexNode boolLit = rexBuilder.makeLiteral(true, boolType);
        RexNode oneLit = rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE, nullableInt);
        RexNode untypedNull = rexBuilder.constantNull();
        RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, boolLit, oneLit, untypedNull);

        RelNode project = LogicalProject.create(values, List.of(), List.of(caseExpr), List.of("case_col"), java.util.Set.of());
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(0),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "good_count"
        );
        LogicalAggregate agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(), null, List.of(countCall));

        RelNode rewritten = UntypedNullPreprocessor.rewrite(agg);
        // The aggregate's input is the rewritten project; the project's CASE ELSE arm must
        // now have a typed null. Walk one level down to verify.
        LogicalAggregate rewrittenAgg = (LogicalAggregate) rewritten;
        LogicalProject rewrittenProj = (LogicalProject) rewrittenAgg.getInput();
        RexCall rewrittenCase = (RexCall) rewrittenProj.getProjects().get(0);
        assertEquals(
            "After Aggregate→Project recursion, the CASE's ELSE arm null must be typed",
            SqlTypeName.INTEGER,
            rewrittenCase.getOperands().get(2).getType().getSqlTypeName()
        );
        // And the COUNT aggregate output schema should still be a single BIGINT column.
        assertEquals(1, rewrittenAgg.getRowType().getFieldCount());
        assertEquals(SqlTypeName.BIGINT, rewrittenAgg.getRowType().getFieldList().get(0).getType().getSqlTypeName());
    }
}
