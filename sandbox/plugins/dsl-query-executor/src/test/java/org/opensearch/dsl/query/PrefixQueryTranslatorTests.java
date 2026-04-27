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

public class PrefixQueryTranslatorTests extends OpenSearchTestCase {

    private final PrefixQueryTranslator translator = new PrefixQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testReportsCorrectQueryType() {
        assertEquals(org.opensearch.index.query.PrefixQueryBuilder.class, translator.getQueryType());
    }

    public void testBasicPrefixQuery() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "lap"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());
        assertEquals(2, call.getOperands().size());

        // Check pattern is "lap%"
        RexNode pattern = call.getOperands().get(1);
        assertTrue(pattern instanceof RexLiteral);
        assertEquals("lap%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryWithEmptyString() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", ""), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());

        // Empty prefix should match all: "%"
        RexNode pattern = call.getOperands().get(1);
        assertEquals("%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryCaseInsensitive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "LAP").caseInsensitive(true), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());

        // First operand should be LOWER(field)
        RexNode fieldExpr = call.getOperands().get(0);
        assertTrue(fieldExpr instanceof RexCall);
        assertEquals(SqlKind.OTHER_FUNCTION, ((RexCall) fieldExpr).getKind());
        assertEquals("LOWER", ((RexCall) fieldExpr).getOperator().getName());

        // Pattern should be lowercased
        RexNode pattern = call.getOperands().get(1);
        assertEquals("lap%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryCaseSensitive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "LAP").caseInsensitive(false), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LIKE, call.getKind());

        // First operand should be direct field reference (no LOWER)
        RexNode fieldExpr = call.getOperands().get(0);
        assertTrue(fieldExpr instanceof org.apache.calcite.rex.RexInputRef);

        // Pattern should preserve case
        RexNode pattern = call.getOperands().get(1);
        assertEquals("LAP%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryEscapesPercentSign() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "50%"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        // % should be escaped to \%
        RexNode pattern = call.getOperands().get(1);
        assertEquals("50\\%%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryEscapesUnderscore() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "test_"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        // _ should be escaped to \_
        RexNode pattern = call.getOperands().get(1);
        assertEquals("test\\_%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryEscapesBackslash() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "path\\to"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        // \ should be escaped to \\
        RexNode pattern = call.getOperands().get(1);
        assertEquals("path\\\\to%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryWithDifferentFields() throws ConversionException {
        // Test with different schema fields
        assertNotNull(translator.convert(QueryBuilders.prefixQuery("name", "test"), ctx));
        assertNotNull(translator.convert(QueryBuilders.prefixQuery("brand", "test"), ctx));
        assertNotNull(translator.convert(QueryBuilders.prefixQuery("price", "100"), ctx));
    }

    public void testPrefixQueryThrowsForNonexistentField() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.prefixQuery("nonexistent", "value"), ctx)
        );
        assertTrue(ex.getMessage().contains("Field 'nonexistent' not found"));
    }

    public void testPrefixQueryThrowsForBoostParameter() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.prefixQuery("name", "lap").boost(2.0f), ctx)
        );
        assertTrue(ex.getMessage().contains("boost"));
        assertTrue(ex.getMessage().contains("not supported"));
    }

    public void testPrefixQueryThrowsForRewriteParameter() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.prefixQuery("name", "lap").rewrite("constant_score"), ctx)
        );
        assertTrue(ex.getMessage().contains("rewrite"));
        assertTrue(ex.getMessage().contains("not supported"));
    }

    public void testPrefixQueryWithSpecialCharacters() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "test-123.abc"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        // Special chars like - and . should not be escaped
        RexNode pattern = call.getOperands().get(1);
        assertEquals("test-123.abc%", ((RexLiteral) pattern).getValueAs(String.class));
    }

    public void testPrefixQueryWithMultipleEscapes() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "a%b_c\\d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;

        // All special chars should be escaped
        RexNode pattern = call.getOperands().get(1);
        assertEquals("a\\%b\\_c\\\\d%", ((RexLiteral) pattern).getValueAs(String.class));
    }
}
