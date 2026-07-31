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
        // \* in OpenSearch wildcard = literal '*' (not a wildcard)
        assertEquals("path*", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithMixedEscaping() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a%b_c\\d*e?"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        // \d → escape consumed, emits 'd' (Lucene WildcardQuery.toAutomaton L99-108)
        assertEquals("a\\%b\\_cd%e_", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testEscapedAsteriskProducesLiteralStar() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\*b"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a*b", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testEscapedQuestionMarkProducesLiteralQuestionMark() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\?b"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a?b", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testTrailingLoneBackslashIsEscaped() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "path\\"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("path\\\\", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testMixedEscapedAndUnescapedWildcards() throws ConversionException {
        // \* → literal '*', * → %, ? → _
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\*b*c?d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a*b%c_d", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testEscapedBackslashFollowedByWildcard() throws ConversionException {
        // In OS wildcard: \\ = literal backslash, then * = wildcard
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\\\*b"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a\\\\%b", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testLiteralPercentAndUnderscoreAreEscaped() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "100%_done"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("100\\%\\_done", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testBackslashBeforeNonSpecialCharConsumesEscape() throws ConversionException {
        // Lucene WildcardQuery.toAutomaton (L99-108): \n → escape consumed, emits just 'n'
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\nb"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("anb", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testBackslashBeforeTConsumesEscape() throws ConversionException {
        // Lucene WildcardQuery.toAutomaton (L99-108): \t → escape consumed, emits just 't'
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\tb"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("atb", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWindowsPathEscapesConsumed() throws ConversionException {
        // Lucene WildcardQuery.toAutomaton: \U → 'U', \t → 't' (escape consumed)
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "C:\\Users\\test"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("C:Userstest", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testBackslashBeforeSqlMetacharEscapesForLike() throws ConversionException {
        // Lucene: \% → escape consumed, emits '%'; SQL LIKE needs \% to keep it literal
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", "a\\%b\\_c"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        RexNode pattern = call.getOperands().get(1);
        assertEquals("a\\%b\\_c", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testWildcardWithEmptyPattern() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.wildcardQuery("name", ""), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());

        RexNode pattern = call.getOperands().get(1);
        assertEquals("", ((RexLiteral) pattern).getValueAs(String.class));
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

    public void testWildcardThrowsForNonStringField() {
        // MappedFieldType.wildcardQuery:309-317 rejects non-keyword/text fields
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.wildcardQuery("price", "12*"), ctx)
        );
        assertTrue(ex.getMessage().contains("keyword and text"));
        assertTrue(ex.getMessage().contains("price"));
    }
}
