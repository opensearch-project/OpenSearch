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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link ConcatFunctionAdapter}. The adapter rewrites Calcite's binary
 * {@code ||(a, b)} (a.k.a. {@code SqlStdOperatorTable.CONCAT}) into a null-propagating
 * {@code CASE WHEN IS_NULL(a) OR IS_NULL(b) THEN NULL ELSE ||(a, b) END}, restoring
 * SQL-standard null semantics that DataFusion's substrait-mapped {@code concat()}
 * function deviates from.
 *
 * <p>Each test pins one structural invariant of the rewrite — a regression that drops
 * the CASE wrapper, mis-orders the IS_NULL operands, or swaps the THEN/ELSE branches
 * surfaces here rather than at IT-level row-mismatch failures.
 */
public class ConcatFunctionAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType varcharType;

    private final ConcatFunctionAdapter adapter = new ConcatFunctionAdapter();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    /** Builds {@code ||(field0, field1)} — Calcite's binary string concat operator. */
    private RexCall buildBinaryConcat() {
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode field1 = rexBuilder.makeInputRef(varcharType, 1);
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, field0, field1);
    }

    /**
     * Builds an n-ary {@code CONCAT(field0, field1, field2)} via {@code SqlLibraryOperators.CONCAT_FUNCTION}
     * to exercise the multi-operand IS_NULL chain path. The binary {@code ||} only ever appears with
     * arity 2 in production, but the adapter's loop handles N — this test guards that path.
     */
    private RexCall buildTernaryConcat() {
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode field1 = rexBuilder.makeInputRef(varcharType, 1);
        RexNode field2 = rexBuilder.makeInputRef(varcharType, 2);
        return (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0, field1, field2);
    }

    // ── core rewrite shape ──────────────────────────────────────────────────

    public void testAdaptBinaryConcatProducesCaseWrapper() {
        RexCall concat = buildBinaryConcat();
        RexNode adapted = adapter.adapt(concat, List.of(), cluster);

        assertTrue("expected RexCall, got " + adapted.getClass().getSimpleName(), adapted instanceof RexCall);
        RexCall caseCall = (RexCall) adapted;
        assertEquals("rewritten root must be CASE", SqlKind.CASE, caseCall.getKind());
        assertEquals("CASE must have exactly three operands [condition, then, else]", 3, caseCall.getOperands().size());
    }

    public void testAdaptedCaseElseBranchIsOriginalConcat() {
        RexCall concat = buildBinaryConcat();
        RexCall caseCall = (RexCall) adapter.adapt(concat, List.of(), cluster);

        // Else branch must be the original RexCall, untouched — by reference, not just equal.
        // Substrait conversion downstream relies on seeing the same object the resolver annotated.
        assertSame("else branch must be the original CONCAT call", concat, caseCall.getOperands().get(2));
    }

    public void testAdaptedCaseThenBranchIsNullLiteralOfMatchingSqlType() {
        RexCall concat = buildBinaryConcat();
        RexCall caseCall = (RexCall) adapter.adapt(concat, List.of(), cluster);

        RexNode thenBranch = caseCall.getOperands().get(1);
        assertTrue("then branch must be a literal", thenBranch instanceof RexLiteral);
        RexLiteral literal = (RexLiteral) thenBranch;
        assertNull("then branch literal must be NULL-valued", literal.getValue());
        // RexBuilder.makeNullLiteral promotes nullability on the literal's type even when the
        // original isn't nullable, so the full RelDataType objects differ. The SQL type name
        // (VARCHAR vs INTEGER vs ...) is the load-bearing invariant — overall CASE return type
        // identity to the original is asserted in testAdaptPreservesReturnType.
        assertEquals(
            "NULL literal SQL type must match the original CONCAT's SQL type",
            concat.getType().getSqlTypeName(),
            literal.getType().getSqlTypeName()
        );
    }

    public void testAdaptedCaseConditionIsOrOfIsNullChecks() {
        RexCall concat = buildBinaryConcat();
        RexCall caseCall = (RexCall) adapter.adapt(concat, List.of(), cluster);

        RexNode condition = caseCall.getOperands().get(0);
        assertEquals("condition must be OR(IS_NULL(a), IS_NULL(b))", SqlKind.OR, condition.getKind());

        RexCall orCall = (RexCall) condition;
        assertEquals(2, orCall.getOperands().size());
        for (int i = 0; i < orCall.getOperands().size(); i++) {
            RexNode disjunct = orCall.getOperands().get(i);
            assertEquals("OR operand " + i + " must be IS_NULL", SqlKind.IS_NULL, disjunct.getKind());
            // Each IS_NULL must wrap the corresponding original operand — order matters for the
            // null-propagation contract.
            assertSame(
                "IS_NULL operand " + i + " must reference the original CONCAT operand " + i,
                concat.getOperands().get(i),
                ((RexCall) disjunct).getOperands().get(0)
            );
        }
    }

    public void testAdaptPreservesReturnType() {
        RexCall concat = buildBinaryConcat();
        RexNode adapted = adapter.adapt(concat, List.of(), cluster);

        assertEquals("CASE return type must equal the original CONCAT return type", concat.getType(), adapted.getType());
    }

    // ── n-ary path ──────────────────────────────────────────────────────────

    public void testAdaptNaryConcatChainsIsNullChecksLeftAssociative() {
        RexCall concat = buildTernaryConcat();
        RexCall caseCall = (RexCall) adapter.adapt(concat, List.of(), cluster);

        // Condition shape: OR(OR(IS_NULL(a), IS_NULL(b)), IS_NULL(c)) — left-fold.
        RexNode condition = caseCall.getOperands().get(0);
        assertEquals(SqlKind.OR, condition.getKind());

        // Right child is IS_NULL(c) — the most recently appended operand in the fold.
        RexCall outerOr = (RexCall) condition;
        assertEquals(2, outerOr.getOperands().size());
        RexNode rightChild = outerOr.getOperands().get(1);
        assertEquals(SqlKind.IS_NULL, rightChild.getKind());
        assertSame(concat.getOperands().get(2), ((RexCall) rightChild).getOperands().get(0));

        // Left child is OR(IS_NULL(a), IS_NULL(b)) — the previously folded prefix.
        RexNode leftChild = outerOr.getOperands().get(0);
        assertEquals(SqlKind.OR, leftChild.getKind());
        RexCall innerOr = (RexCall) leftChild;
        assertEquals(SqlKind.IS_NULL, innerOr.getOperands().get(0).getKind());
        assertEquals(SqlKind.IS_NULL, innerOr.getOperands().get(1).getKind());
        assertSame(concat.getOperands().get(0), ((RexCall) innerOr.getOperands().get(0)).getOperands().get(0));
        assertSame(concat.getOperands().get(1), ((RexCall) innerOr.getOperands().get(1)).getOperands().get(0));
    }

    // ── pass-through guard ─────────────────────────────────────────────────

    public void testAdaptSingleOperandConcatPassesThroughUnchanged() {
        // Built via the variadic CONCAT_FUNCTION since SqlStdOperatorTable.CONCAT is binary and
        // can't represent a single-operand call. The adapter's contract is that a 1-operand call
        // is a no-op — concat with one input equals that input, no null handling needed.
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexCall singleOperand = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0);

        RexNode adapted = adapter.adapt(singleOperand, List.of(), cluster);

        assertSame("single-operand call must pass through unmodified", singleOperand, adapted);
    }
}
