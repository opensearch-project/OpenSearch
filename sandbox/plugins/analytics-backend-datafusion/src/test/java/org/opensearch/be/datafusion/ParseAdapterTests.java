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
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link ParseAdapter}. The adapter has two plan-time jobs that
 * both surface user errors before any UDF runs: literal validation on the
 * pattern + method operands, and method gating to {@code regex} only. After
 * validation it rewrites the call to the local UDF operator with the original
 * call's return type preserved (mirrors the ConvertTzAdapter pattern so the
 * enclosing Project's compatibleTypes check stays consistent).
 */
public class ParseAdapterTests extends OpenSearchTestCase {

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
     * Mirrors {@code ParseFunction}'s explicit {@code MAP<VARCHAR, VARCHAR>}
     * return type so the rewrite-preserves-return-type assertion has a
     * non-trivial type to check against.
     */
    private RelDataType mapVarcharType() {
        return SqlTypeUtil.createMapType(
            typeFactory,
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            false
        );
    }

    private SqlFunction parseOp(RelDataType returnType) {
        return new SqlFunction(
            "PARSE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(returnType),
            null,
            OperandTypes.STRING_STRING_STRING,
            SqlFunctionCategory.STRING
        );
    }

    private RexCall buildParse(RexNode pattern, RexNode method) {
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode inputRef = rexBuilder.makeInputRef(varcharType, 0);
        return (RexCall) rexBuilder.makeCall(parseOp(mapVarcharType()), List.of(inputRef, pattern, method));
    }

    private RexCall buildParseLiterals(String pattern, String method) {
        return buildParse(rexBuilder.makeLiteral(pattern), rexBuilder.makeLiteral(method));
    }

    // ── adapt() happy path ────────────────────────────────────────────────

    /**
     * The expected flow: input column ref + pattern literal + method literal
     * "regex" rewrites to {@link ParseAdapter#LOCAL_PARSE_OP} so
     * {@code FunctionMappings.Sig} resolves it to the {@code parse} extension.
     */
    public void testAdaptValidRegexCallRoutesThroughLocalUdfOperator() {
        RexCall original = buildParseLiterals("(?<group>\\w+)", "regex");
        RexNode adapted = new ParseAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target LOCAL_PARSE_OP so FunctionMappings.Sig binds",
            ParseAdapter.LOCAL_PARSE_OP,
            call.getOperator()
        );
        assertEquals(3, call.getOperands().size());
        assertSame("input column ref must pass through unchanged", original.getOperands().get(0), call.getOperands().get(0));
        assertSame("pattern literal must pass through unchanged", original.getOperands().get(1), call.getOperands().get(1));
        assertSame("method literal must pass through unchanged", original.getOperands().get(2), call.getOperands().get(2));
    }

    /**
     * Method literal comparison is case-insensitive: PPL parses {@code regex}
     * as a method symbol and may surface it uppercased through the legacy AST
     * path. The Rust UDF then receives the user-supplied casing — its check
     * is exact-lower-case but the literal pass-through preserves the original
     * casing so users see their own input in any error.
     */
    public void testAdaptAcceptsUppercaseRegexMethod() {
        RexCall original = buildParseLiterals("(?<g>\\w+)", "REGEX");
        RexNode adapted = new ParseAdapter().adapt(original, List.of(), cluster);
        assertTrue(adapted instanceof RexCall);
        assertSame(ParseAdapter.LOCAL_PARSE_OP, ((RexCall) adapted).getOperator());
    }

    /**
     * Adapter preserves the original call's return type — matches the
     * AbstractNameMappingAdapter regression guard. If the rewritten call's
     * Calcite-inferred type differs from the original, the enclosing
     * {@code Project.isValid} compatibleTypes check breaks at fragment
     * conversion.
     */
    public void testAdaptedCallPreservesOriginalMapReturnType() {
        RexCall original = buildParseLiterals("(?<g>\\w+)", "regex");
        RexNode adapted = new ParseAdapter().adapt(original, List.of(), cluster);
        assertEquals("adapted call's return type must equal the original Map<varchar, varchar>", original.getType(), adapted.getType());
    }

    // ── adapt() validation errors ─────────────────────────────────────────

    public void testAdaptRejectsNonLiteralPattern() {
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        // Pattern slot pulls from a column reference instead of a literal — Rust
        // UDF compiles the pattern once per call, so per-row patterns are
        // ill-defined. Adapter must reject up front.
        RexCall original = buildParse(rexBuilder.makeInputRef(varcharType, 1), rexBuilder.makeLiteral("regex"));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParseAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue("error must name the offending slot: " + ex.getMessage(), ex.getMessage().contains("pattern"));
    }

    public void testAdaptRejectsNonLiteralMethod() {
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall original = buildParse(rexBuilder.makeLiteral("(?<g>\\w+)"), rexBuilder.makeInputRef(varcharType, 1));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParseAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(ex.getMessage().contains("method"));
    }

    public void testAdaptRejectsGrokMethodWithActionableMessage() {
        RexCall original = buildParseLiterals("%{NUMBER:n}", "grok");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParseAdapter().adapt(original, List.of(), cluster)
        );
        // Error must mention both the rejected method (so users see what's wrong)
        // and the supported alternative ("regex") so they know the workaround.
        assertTrue("error must name the rejected method: " + ex.getMessage(), ex.getMessage().contains("grok"));
        assertTrue("error must mention the supported method: " + ex.getMessage(), ex.getMessage().contains("regex"));
    }

    public void testAdaptRejectsPatternsMethodWithActionableMessage() {
        RexCall original = buildParseLiterals("any", "patterns");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParseAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(ex.getMessage().contains("patterns"));
        assertTrue(ex.getMessage().contains("regex"));
    }

    public void testAdaptRejectsNullPatternLiteral() {
        // Null literal in the pattern slot is rejected — runtime would have to
        // either skip every row or produce an opaque NPE inside the Rust UDF.
        RexNode nullLit = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RexCall original = buildParse(nullLit, rexBuilder.makeLiteral("regex"));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParseAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(ex.getMessage().contains("pattern"));
    }

    public void testAdaptRejectsWrongArity() {
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode inputRef = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("(?<g>\\w+)");
        SqlFunction binaryParse = new SqlFunction(
            "PARSE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(mapVarcharType()),
            null,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING
        );
        RexCall original = (RexCall) rexBuilder.makeCall(binaryParse, List.of(inputRef, pattern));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ParseAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue("error must mention arity: " + ex.getMessage(), ex.getMessage().contains("3 operands"));
    }
}
