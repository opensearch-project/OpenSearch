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
        assertEquals("path\\\\%", ((RexLiteral) pattern).getValueAs(String.class));
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
        assertEquals("a\\%b\\_c\\\\d%e_", ((RexLiteral) pattern).getValueAs(String.class));
    }
}
