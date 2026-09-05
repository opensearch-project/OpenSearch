/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link WildcardQueryTranslator} — asserts delegation via WILDCARD_QUERY_DSL RexCall
 * with the Lucene wildcard pattern emitted unchanged.
 */
public class WildcardQueryTranslatorTests extends OpenSearchTestCase {

    private final WildcardQueryTranslator translator = new WildcardQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    // ── RexCall shape assertions ────────────────────────────────────────────────

    public void testEmitsWildcardQueryDslRexCall() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l*pt?p"), ctx);

        assertTrue("Result must be a RexCall", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("WILDCARD_QUERY_DSL", call.getOperator().getName());
        assertEquals(SqlKind.OTHER_FUNCTION, call.getKind());
    }

    public void testFieldOperandContainsInputRef() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l*"), ctx);
        RexCall call = (RexCall) result;

        RexCall fieldMap = (RexCall) call.getOperands().get(0);
        assertEquals("MAP", fieldMap.getOperator().getName());
        RexLiteral fieldKey = (RexLiteral) fieldMap.getOperands().get(0);
        assertEquals("field", fieldKey.getValueAs(String.class));
        assertTrue("field value must be an InputRef", fieldMap.getOperands().get(1) instanceof RexInputRef);
        RexInputRef inputRef = (RexInputRef) fieldMap.getOperands().get(1);
        assertEquals(0, inputRef.getIndex()); // 'name' is index 0
    }

    public void testQueryOperandContainsLiteralPattern() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l*pt?p"), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        assertEquals("MAP", queryMap.getOperator().getName());
        RexLiteral queryKey = (RexLiteral) queryMap.getOperands().get(0);
        assertEquals("query", queryKey.getValueAs(String.class));
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("l*pt?p", queryValue.getValueAs(String.class));
    }

    // ── Verbatim passthrough — pattern is emitted UNCHANGED ─────────────────────

    public void testWildcardPatternPassedVerbatim() throws ConversionException {
        // Lucene wildcard pattern: *, ?, escaped backslash — all pass through untouched
        String pattern = "test\\*file\\?name\\\\end";
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", pattern), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals(pattern, queryValue.getValueAs(String.class));
    }

    public void testPatternWithSqlMetacharsPassedVerbatim() throws ConversionException {
        // SQL metacharacters % and _ are NOT special in Lucene wildcard syntax — pass through unchanged
        String pattern = "50%_done*";
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", pattern), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals(pattern, queryValue.getValueAs(String.class));
    }

    public void testPatternWithBackslashPassedVerbatim() throws ConversionException {
        // A pattern with backslash-escaped chars — no conversion occurs
        String pattern = "C:\\Users\\test";
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", pattern), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals(pattern, queryValue.getValueAs(String.class));
    }

    public void testEmptyPatternPassedVerbatim() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", ""), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("", queryValue.getValueAs(String.class));
    }

    // ── case_insensitive param ──────────────────────────────────────────────────

    public void testCaseInsensitiveEmitsParam() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "LAP*").caseInsensitive(true), ctx);
        RexCall call = (RexCall) result;

        assertEquals(3, call.getOperands().size());
        RexCall paramMap = (RexCall) call.getOperands().get(2);
        assertEquals("MAP", paramMap.getOperator().getName());
        RexLiteral paramKey = (RexLiteral) paramMap.getOperands().get(0);
        assertEquals("case_insensitive", paramKey.getValueAs(String.class));
        RexLiteral paramValue = (RexLiteral) paramMap.getOperands().get(1);
        assertEquals("true", paramValue.getValueAs(String.class));
    }

    public void testCaseSensitiveHasNoExtraOperand() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l*"), ctx);
        RexCall call = (RexCall) result;

        assertEquals(2, call.getOperands().size());
    }

    public void testCaseInsensitivePatternNotLowered() throws ConversionException {
        // Pattern should NOT be lowercased — case_insensitive is now a param, not LOWER() wrapping
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "LAP*").caseInsensitive(true), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("LAP*", queryValue.getValueAs(String.class));
    }

    // ── VARCHAR field-type gate ─────────────────────────────────────────────────

    public void testWildcardThrowsForNonStringField() {
        // MappedFieldType.wildcardQuery:309-317 rejects non-keyword/text fields
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("price", "12*"), ctx)
        );
        assertTrue(ex.getMessage().contains("keyword and text"));
        assertTrue(ex.getMessage().contains("price"));
    }

    // ── Parameter rejection ─────────────────────────────────────────────────────

    public void testWildcardThrowsForNonexistentField() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("nonexistent", "val*"), ctx)
        );
        assertTrue(ex.getMessage().contains("Field 'nonexistent' not found"));
    }

    public void testWildcardThrowsForBoostParameter() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("name", "lap*").boost(2.0f), ctx)
        );
        assertTrue(ex.getMessage().contains("boost"));
        assertTrue(ex.getMessage().contains("not supported"));
    }

    public void testWildcardThrowsForRewriteParameter() throws ConversionException {
        // Rewrite is now passed through (H2 disposition) — no longer rejected.
        // Validation occurs on the data node via QueryParsers.parseRewriteMethod.
        translator.convert(QueryBuilders.wildcardQuery("name", "lap*").rewrite("constant_score"), ctx);
    }

    // ── Field resolution ────────────────────────────────────────────────────────

    public void testFieldResolutionUsesCorrectIndex() throws ConversionException {
        // 'brand' is at index 2 in the test schema
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("brand", "app*"), ctx);
        RexCall call = (RexCall) result;

        RexCall fieldMap = (RexCall) call.getOperands().get(0);
        RexInputRef inputRef = (RexInputRef) fieldMap.getOperands().get(1);
        assertEquals(2, inputRef.getIndex());
    }

    // ── Rewrite pass-through (H2 disposition) ───────────────────────────────────

    /**
     * The rewrite parameter must be passed through as a MAP operand rather than rejected.
     */
    public void testRewriteParameterEmitsMapOperand() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "lap*").rewrite("constant_score"), ctx);
        RexCall call = (RexCall) result;

        // Should have 3 operands: field, query, rewrite param
        assertEquals(3, call.getOperands().size());
        RexCall paramMap = (RexCall) call.getOperands().get(2);
        assertEquals("MAP", paramMap.getOperator().getName());
        RexLiteral paramKey = (RexLiteral) paramMap.getOperands().get(0);
        assertEquals("rewrite", paramKey.getValueAs(String.class));
        RexLiteral paramValue = (RexLiteral) paramMap.getOperands().get(1);
        assertEquals("constant_score", paramValue.getValueAs(String.class));
    }

    // ── Boost rejection regression guard ────────────────────────────────────────

    /**
     * Boost must remain rejected with ConversionException — regression guard for the approved disposition.
     */
    public void testBoostParameterRejectedWithConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("name", "lap*").boost(1.5f), ctx)
        );
        assertTrue("Must mention 'boost', got: " + ex.getMessage(), ex.getMessage().contains("boost"));
        assertTrue("Must mention 'not supported', got: " + ex.getMessage(), ex.getMessage().contains("not supported"));
    }

    // ── _name rejection ────────────────────────────────────────────────────────

    /**
     * _name must be rejected with ConversionException — matched_queries is not surfaced in SQL.
     */
    public void testNameParameterRejectedWithConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("name", "lap*").queryName("my_wildcard"), ctx)
        );
        assertEquals("Wildcard query parameter '_name' is not supported", ex.getMessage());
    }

    // ── Behaviour-pinning: leading wildcard emitted verbatim (M3) ───────────────

    /**
     * Behaviour-pinning test: a leading-wildcard pattern is emitted verbatim — no rewriting or rejection.
     */
    public void testLeadingWildcardPatternEmittedVerbatim() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "*checkout"), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("*checkout", queryValue.getValueAs(String.class));
    }

    // ── Behaviour-pinning: case_insensitive explicitly false ────────────────────

    /**
     * Behaviour-pinning test: explicitly setting case_insensitive to false emits no case_insensitive
     * param operand — the translator only emits the param when true.
     */
    public void testExplicitCaseInsensitiveFalseEmitsNoParam() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l*").caseInsensitive(false), ctx);
        RexCall call = (RexCall) result;

        // Only 2 operands: field and query — no case_insensitive param when value is false
        assertEquals(2, call.getOperands().size());
    }

    // ── Behaviour-pinning: unicode pattern emitted verbatim (L1) ────────────────

    /**
     * Behaviour-pinning test: a unicode pattern containing German sharp s (ß) and accented characters
     * is emitted verbatim — documents that case folding is now Lucene's ASCII-only behaviour
     * rather than our old LOWER() wrapping.
     */
    public void testUnicodePatternWithSharpSEmittedVerbatim() throws ConversionException {
        String unicodePattern = "straße*über?";
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", unicodePattern), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals(unicodePattern, queryValue.getValueAs(String.class));
    }
}
