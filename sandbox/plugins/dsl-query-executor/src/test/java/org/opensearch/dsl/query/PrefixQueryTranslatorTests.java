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
 * Tests for {@link PrefixQueryTranslator} — asserts delegation via PREFIX_QUERY RexCall.
 */
public class PrefixQueryTranslatorTests extends OpenSearchTestCase {

    private final PrefixQueryTranslator translator = new PrefixQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testEmitsPrefixQueryRexCall() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "lap"), ctx);

        assertTrue("Result must be a RexCall", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("PREFIX_QUERY", call.getOperator().getName());
        assertEquals(SqlKind.OTHER_FUNCTION, call.getKind());
    }

    public void testFieldOperandContainsInputRef() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "lap"), ctx);
        RexCall call = (RexCall) result;

        // Operand 0: MAP('field', $inputRef)
        RexCall fieldMap = (RexCall) call.getOperands().get(0);
        assertEquals("MAP", fieldMap.getOperator().getName());
        RexLiteral fieldKey = (RexLiteral) fieldMap.getOperands().get(0);
        assertEquals("field", fieldKey.getValueAs(String.class));
        assertTrue("field value must be an InputRef", fieldMap.getOperands().get(1) instanceof RexInputRef);
        RexInputRef inputRef = (RexInputRef) fieldMap.getOperands().get(1);
        assertEquals(0, inputRef.getIndex()); // 'name' is index 0 in test schema
    }

    public void testQueryOperandContainsLiteralValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "lap"), ctx);
        RexCall call = (RexCall) result;

        // Operand 1: MAP('query', 'lap')
        RexCall queryMap = (RexCall) call.getOperands().get(1);
        assertEquals("MAP", queryMap.getOperator().getName());
        RexLiteral queryKey = (RexLiteral) queryMap.getOperands().get(0);
        assertEquals("query", queryKey.getValueAs(String.class));
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("lap", queryValue.getValueAs(String.class));
    }

    public void testVerbatimPassthroughOfMetacharacters() throws ConversionException {
        // Value contains *, ? and backslash — must be passed through unchanged (no escaping)
        String verbatim = "check*it?out\\done";
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", verbatim), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals(verbatim, queryValue.getValueAs(String.class));
    }

    public void testEmptyStringPassedVerbatim() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", ""), ctx);
        RexCall call = (RexCall) result;

        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral queryValue = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("", queryValue.getValueAs(String.class));
    }

    public void testCaseInsensitiveEmitsParam() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "Lap").caseInsensitive(true), ctx);
        RexCall call = (RexCall) result;

        // Should have 3 operands: field, query, case_insensitive param
        assertEquals(3, call.getOperands().size());
        RexCall paramMap = (RexCall) call.getOperands().get(2);
        assertEquals("MAP", paramMap.getOperator().getName());
        RexLiteral paramKey = (RexLiteral) paramMap.getOperands().get(0);
        assertEquals("case_insensitive", paramKey.getValueAs(String.class));
        RexLiteral paramValue = (RexLiteral) paramMap.getOperands().get(1);
        assertEquals("true", paramValue.getValueAs(String.class));
    }

    public void testCaseSensitiveHasNoExtraOperand() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.prefixQuery("name", "lap"), ctx);
        RexCall call = (RexCall) result;

        // Only 2 operands: field and query (no case_insensitive param)
        assertEquals(2, call.getOperands().size());
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

    public void testPrefixQueryThrowsForNonStringField() {
        // MappedFieldType.prefixQuery:291-297 rejects non-keyword/text fields
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.prefixQuery("price", "12"), ctx)
        );
        assertTrue(ex.getMessage().contains("keyword and text"));
        assertTrue(ex.getMessage().contains("price"));
    }

    public void testFieldResolutionUsesCorrectIndex() throws ConversionException {
        // 'brand' is at index 2 in the test schema
        RexNode result = translator.convert(QueryBuilders.prefixQuery("brand", "app"), ctx);
        RexCall call = (RexCall) result;

        RexCall fieldMap = (RexCall) call.getOperands().get(0);
        RexInputRef inputRef = (RexInputRef) fieldMap.getOperands().get(1);
        assertEquals(2, inputRef.getIndex()); // 'brand' is index 2
    }
}
