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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RegexpReplaceAdapter}. Pins the {@code \Q…\E} → per-char-escape
 * rewrite that bridges the SQL plugin's Java-style wildcard regex output (from
 * {@code WildcardUtils.convertWildcardPatternToRegex()}) to the Rust regex syntax expected
 * by DataFusion's {@code regexp_replace} UDF.
 *
 * <p>Each test pins one rewrite invariant. A regression that loses {@code \Q…\E} expansion,
 * mishandles unterminated quotes, or swaps operand positions in the rebuilt
 * {@code REGEXP_REPLACE} call surfaces here rather than at IT-level "regex parse error"
 * failures.
 */
public class RegexpReplaceAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType varcharType;

    private final RegexpReplaceAdapter adapter = new RegexpReplaceAdapter();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    // ── unquoteJavaRegex — the substantive transform ────────────────────────────

    public void testUnquoteEmptyQuoteBlock() {
        // \Q\E produces empty string in Java; should disappear entirely.
        assertEquals("", RegexpReplaceAdapter.unquoteJavaRegex("\\Q\\E"));
    }

    public void testUnquotePreservesNonQuotedPortions() {
        // Standard regex outside any \Q…\E passes through unchanged.
        assertEquals("^(.*?)$", RegexpReplaceAdapter.unquoteJavaRegex("^(.*?)$"));
    }

    public void testUnquoteSimpleLiteral() {
        // \QBOARDS\E → BOARDS (no special chars to escape).
        assertEquals("BOARDS", RegexpReplaceAdapter.unquoteJavaRegex("\\QBOARDS\\E"));
    }

    public void testUnquoteWildcardSuffixShape() {
        // SQL plugin's WildcardUtils output for `*BOARDS` — empty prefix, capture, literal suffix.
        assertEquals("^(.*?)BOARDS$", RegexpReplaceAdapter.unquoteJavaRegex("^\\Q\\E(.*?)\\QBOARDS\\E$"));
    }

    public void testUnquoteWildcardPrefixShape() {
        // SQL plugin's WildcardUtils output for `BUSINESS*` — literal prefix, capture, empty suffix.
        assertEquals("^BUSINESS(.*?)$", RegexpReplaceAdapter.unquoteJavaRegex("^\\QBUSINESS\\E(.*?)\\Q\\E$"));
    }

    public void testUnquoteEscapesMetacharsInsideQuote() {
        // \Q a.b+c \E — inside a Java literal block `.` and `+` are not regex metas; in standard
        // regex they are. Rewrite must escape every metachar so semantics are preserved.
        assertEquals("a\\.b\\+c", RegexpReplaceAdapter.unquoteJavaRegex("\\Qa.b+c\\E"));
    }

    public void testUnquoteHandlesMultipleQuoteBlocks() {
        // Two \Q…\E spans separated by a regex fragment.
        assertEquals("FOO(.*?)BAR", RegexpReplaceAdapter.unquoteJavaRegex("\\QFOO\\E(.*?)\\QBAR\\E"));
    }

    public void testUnquoteUnterminatedRunsToEnd() {
        // Per Java Pattern semantics, \Q without a closing \E quotes through end of string.
        assertEquals("\\.\\+", RegexpReplaceAdapter.unquoteJavaRegex("\\Q.+"));
    }

    public void testUnquoteIdempotentOnRustCompatibleRegex() {
        // No \Q in input → output identical to input.
        String input = "^(foo|bar).*$";
        assertEquals(input, RegexpReplaceAdapter.unquoteJavaRegex(input));
    }

    // ── braceBackreferences — replacement-string transform ──────────────────────

    public void testBraceWrapsBareNumeric() {
        // $1 → ${1}; trivial smoke check.
        assertEquals("${1}", RegexpReplaceAdapter.braceBackreferences("$1"));
    }

    public void testBraceCriticalCaseFollowedByUnderscore() {
        // $1_$2 — the failing wildcard-replacement case. Rust parses $1_ as named group "1_",
        // so the brace rewrite is what makes group-1 + literal underscore + group-2 work.
        assertEquals("${1}_${2}", RegexpReplaceAdapter.braceBackreferences("$1_$2"));
    }

    public void testBraceFollowedByLetter() {
        // $1foo — Rust would parse "1foo" as the group name. Braces force the boundary.
        assertEquals("${1}foo", RegexpReplaceAdapter.braceBackreferences("$1foo"));
    }

    public void testBraceMultiDigitGroup() {
        // $12 (group twelve) — wrap entire numeric run.
        assertEquals("${12}", RegexpReplaceAdapter.braceBackreferences("$12"));
    }

    public void testBracePreservesLiteralDollar() {
        // $$ stays $$ (Rust regex's literal-dollar escape, same as Java).
        assertEquals("$$10", RegexpReplaceAdapter.braceBackreferences("$$10"));
    }

    public void testBracePreservesAlreadyBraced() {
        // ${1} input is already braced — must not be re-wrapped or otherwise mangled.
        assertEquals("${1}_${2}", RegexpReplaceAdapter.braceBackreferences("${1}_${2}"));
    }

    public void testBraceIdempotentOnNonBackrefReplacement() {
        // No $ at all → output identical to input.
        String input = "plain literal";
        assertEquals(input, RegexpReplaceAdapter.braceBackreferences(input));
    }

    // ── adapter integration: RexCall in / RexCall out ───────────────────────────

    public void testAdaptRewritesPatternLiteral() {
        // Build REGEXP_REPLACE(field, '^\\QBUSINESS\\E(.*?)\\Q\\E$', 'BIZ') and verify the
        // rebuilt call has the expanded pattern, original input, and original replacement.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("^\\QBUSINESS\\E(.*?)\\Q\\E$");
        RexNode replacement = rexBuilder.makeLiteral("BIZ");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.REGEXP_REPLACE_3, List.of(field, pattern, replacement));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("adapted node must remain a RexCall", adapted instanceof RexCall);
        RexCall result = (RexCall) adapted;
        assertEquals("operator preserved", original.getOperator(), result.getOperator());
        assertEquals("input operand preserved", field, result.getOperands().get(0));
        assertEquals("replacement operand preserved", replacement, result.getOperands().get(2));

        RexNode newPatternNode = result.getOperands().get(1);
        assertTrue("pattern must remain a literal", newPatternNode instanceof RexLiteral);
        assertEquals("Java \\Q…\\E rewritten to plain regex", "^BUSINESS(.*?)$", ((RexLiteral) newPatternNode).getValueAs(String.class));
    }

    public void testAdaptPassesThroughWhenNoQuoteBlock() {
        // Pattern doesn't contain \Q — adapter must return the call unchanged (identity).
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("^OFFICE.*$");
        RexNode replacement = rexBuilder.makeLiteral("OFC");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.REGEXP_REPLACE_3, List.of(field, pattern, replacement));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("identity — no rewrite when pattern has no \\Q", original, adapted);
    }

    public void testAdaptPassesThroughNonLiteralPattern() {
        // Pattern is a column reference (not a literal) — adapter cannot rewrite at planning
        // time; pass through and let DataFusion error at runtime if the value is incompatible.
        // Replacement is a plain literal with no $, so neither transform fires.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode patternRef = rexBuilder.makeInputRef(varcharType, 1);
        RexNode replacement = rexBuilder.makeLiteral("X");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.REGEXP_REPLACE_3, List.of(field, patternRef, replacement));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("non-literal pattern must pass through", original, adapted);
    }

    public void testAdaptRewritesReplacementOnly() {
        // Rust-compatible pattern but Java-style $1_$2 replacement — adapter rewrites only
        // the replacement, leaves the pattern untouched.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("^(.*?) (.*?)$");
        RexNode replacement = rexBuilder.makeLiteral("$1_$2");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.REGEXP_REPLACE_3, List.of(field, pattern, replacement));

        RexCall result = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals(
            "pattern unchanged when no \\Q present",
            "^(.*?) (.*?)$",
            ((RexLiteral) result.getOperands().get(1)).getValueAs(String.class)
        );
        assertEquals("$1_$2 wrapped to ${1}_${2}", "${1}_${2}", ((RexLiteral) result.getOperands().get(2)).getValueAs(String.class));
    }

    public void testAdaptRewritesBothPatternAndReplacement() {
        // The full failing-IT shape: Java-quoted pattern AND bare $N replacement. Both must
        // be rewritten in a single pass so the resulting call matches DataFusion semantics.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("^\\Q\\E(.*?)\\Q \\E(.*?)\\Q\\E$");
        RexNode replacement = rexBuilder.makeLiteral("$1_$2");
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.REGEXP_REPLACE_3, List.of(field, pattern, replacement));

        RexCall result = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("pattern unquoted", "^(.*?) (.*?)$", ((RexLiteral) result.getOperands().get(1)).getValueAs(String.class));
        assertEquals("replacement braced", "${1}_${2}", ((RexLiteral) result.getOperands().get(2)).getValueAs(String.class));
    }

    public void testAdapt4ArgRewritesPatternAndPassesFlagsThrough() {
        // 4-arg REGEXP_REPLACE_PG_4 — emitted by PPL `rex mode=sed` with /g or /i flags.
        // Pattern + replacement get rewritten as in the 3-arg case; the trailing flags
        // operand passes through unchanged.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("^\\QFOO\\E");
        RexNode replacement = rexBuilder.makeLiteral("$1");
        RexNode flags = rexBuilder.makeLiteral("gi");
        RexCall original = (RexCall) rexBuilder.makeCall(
            SqlLibraryOperators.REGEXP_REPLACE_PG_4,
            List.of(field, pattern, replacement, flags)
        );

        RexCall result = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("4-arg call preserved", 4, result.getOperands().size());
        assertEquals("pattern unquoted", "^FOO", ((RexLiteral) result.getOperands().get(1)).getValueAs(String.class));
        assertEquals("replacement braced", "${1}", ((RexLiteral) result.getOperands().get(2)).getValueAs(String.class));
        assertSame("flags operand passes through verbatim", flags, result.getOperands().get(3));
    }

    public void testAdapt4ArgPassesThroughWhenNoRewriteNeeded() {
        // 4-arg call with Rust-compatible pattern and no $N — adapter must return identity.
        RexNode field = rexBuilder.makeInputRef(varcharType, 0);
        RexNode pattern = rexBuilder.makeLiteral("^foo$");
        RexNode replacement = rexBuilder.makeLiteral("bar");
        RexNode flags = rexBuilder.makeLiteral("g");
        RexCall original = (RexCall) rexBuilder.makeCall(
            SqlLibraryOperators.REGEXP_REPLACE_PG_4,
            List.of(field, pattern, replacement, flags)
        );

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("identity — 4-arg call with no \\Q and no $N passes through", original, adapted);
    }
}
