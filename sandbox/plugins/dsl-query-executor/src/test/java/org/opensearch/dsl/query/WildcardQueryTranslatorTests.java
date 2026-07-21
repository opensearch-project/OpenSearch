/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

public class WildcardQueryTranslatorTests extends OpenSearchTestCase {

    private final WildcardQueryTranslator translator = new WildcardQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testReportsCorrectQueryType() {
        assertEquals(org.opensearch.index.query.WildcardQueryBuilder.class, translator.getQueryType());
    }

    public void testWildcardWithAsterisk() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "lap*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());

        RexNode pattern = call.getOperands().get(1);
        assertEquals("lap%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithQuestionMark() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l?ptop"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("l_ptop", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithBothWildcards() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l?p*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("l_p%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithMultipleAsterisks() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "*lap*top*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("%lap%top%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithMultipleQuestionMarks() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "l??top"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("l__top", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardCaseInsensitive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "LAP*").caseInsensitive(true), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        // First operand should be LOWER(field)
        RexNode fieldExpr = call.getOperands().get(0);
        assertTrue(fieldExpr instanceof RexCall);
        assertEquals("LOWER", ((RexCall) fieldExpr).getOperator().getName());

        // Pattern should be lowercased
        RexNode pattern = call.getOperands().get(1);
        assertEquals("lap%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardEscapesPercent() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "50%*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("50\\%%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardEscapesUnderscore() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "test_*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("test\\_%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardEscapesBackslash() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "path\\*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \* in OpenSearch wildcard = literal '*' character (not a wildcard)
        assertEquals("path*", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithNoWildcards() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "laptop"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("laptop", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithOnlyAsterisk() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithOnlyQuestionMark() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "?"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("_", ((RexLiteral) pattern).getValueAs(String.class));
    }

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

    public void testWildcardThrowsForRewriteParameter() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("name", "lap*").rewrite("constant_score"), ctx)
        );
        assertTrue(ex.getMessage().contains("rewrite"));
        assertTrue(ex.getMessage().contains("not supported"));
    }

    public void testWildcardWithComplexPattern() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a*b?c*d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a%b_c%d", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithMixedEscaping() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a%b_c\\d*e?"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \d in OpenSearch wildcard = literal backslash + 'd'; LIKE must have \\d for the literal backslash
        assertEquals("a\\%b\\_c\\\\d%e_", ((RexLiteral) pattern).getValueAs(String.class));
    }

    // --- Backslash-escape semantics: \* and \? are literal characters in OpenSearch wildcard syntax ---

    public void testEscapedAsteriskProducesLiteralStar() throws ConversionException {
        // In OpenSearch wildcard syntax, \* means a literal asterisk character
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\*b"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \* in input → literal '*' in LIKE pattern (not a wildcard)
        assertEquals("a*b", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testEscapedQuestionMarkProducesLiteralQuestionMark() throws ConversionException {
        // In OpenSearch wildcard syntax, \? means a literal question mark character
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\?b"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \? in input → literal '?' in LIKE pattern (not a wildcard)
        assertEquals("a?b", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testTrailingLoneBackslashIsEscaped() throws ConversionException {
        // A trailing backslash with nothing after it is a literal backslash
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "path\\"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // Trailing \ → escaped backslash in LIKE: \\
        assertEquals("path\\\\", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testMixedEscapedAndUnescapedWildcards() throws ConversionException {
        // a\*b*c?d combines: escaped star, unescaped star, unescaped question
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\*b*c?d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \* → literal '*', * → %, ? → _
        assertEquals("a*b%c_d", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testEscapedBackslashFollowedByWildcard() throws ConversionException {
        // \\\* in Java string = two chars: \\ and *. In OS wildcard: \\ = literal backslash, * = wildcard
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\\\*b"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \\ → escaped backslash (\\\\), then * → % wildcard
        assertEquals("a\\\\%b", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testLiteralPercentAndUnderscoreAreEscaped() throws ConversionException {
        // Verify that literal SQL metacharacters % and _ in the value are properly escaped
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "100%_done"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // % → \%, _ → \_
        assertEquals("100\\%\\_done", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testBackslashBeforeNonSpecialCharEmitsEscapedBackslash() throws ConversionException {
        // \n in OpenSearch wildcard means literal backslash + n; LIKE must contain \\n
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\nb"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a\\\\nb", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testBackslashBeforeT() throws ConversionException {
        // \t in OpenSearch wildcard means literal backslash + t
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\tb"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a\\\\tb", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWindowsPathBackslashes() throws ConversionException {
        // C:\\Users\\test in Java string = C:\Users\test in the pattern
        // : is literal, each \U and \t is backslash + non-special char → \\U and \\t in LIKE
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "C:\\Users\\test"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("C:\\\\Users\\\\test", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testLikeExpressionHasExplicitEscapeClause() throws ConversionException {
        // The LIKE call must have 3 operands: field, pattern, escape char
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "lap*"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());
        assertEquals("LIKE expression must have 3 operands (field, pattern, escape)", 3, call.getOperands().size());

        // Third operand is the escape character literal '\'
        RexNode escapeNode = call.getOperands().get(2);
        assertTrue(escapeNode instanceof RexLiteral);
        assertEquals("\\", ((RexLiteral) escapeNode).getValueAs(String.class));
    }

    public void testWildcardWithEmptyPattern() throws ConversionException {
        // Empty pattern should produce an empty LIKE pattern
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", ""), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());

        RexNode pattern = call.getOperands().get(1);
        assertEquals("", ((RexLiteral) pattern).getValueAs(String.class));
    }
}
