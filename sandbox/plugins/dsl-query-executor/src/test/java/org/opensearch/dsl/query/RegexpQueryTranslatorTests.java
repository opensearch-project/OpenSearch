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
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class RegexpQueryTranslatorTests extends OpenSearchTestCase {

    private final RegexpQueryTranslator translator = new RegexpQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testReportsCorrectQueryType() {
        assertEquals(RegexpQueryBuilder.class, translator.getQueryType());
    }

    public void testHappyPathRexCallShape() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "lap.*top");
        RexNode result = translator.convert(builder, ctx);

        RexCall call = (RexCall) result;
        assertEquals("REGEXP_QUERY", call.getOperator().getName());
        assertEquals(SqlKind.OTHER_FUNCTION, call.getKind());
        // 2 operands: MAP('field', $inputRef) and MAP('query', 'lap.*top')
        assertEquals(2, call.getOperands().size());

        // Operand 0: MAP('field', $inputRef for 'name' at index 0)
        RexCall fieldMap = (RexCall) call.getOperands().get(0);
        assertEquals("field", ((RexLiteral) fieldMap.getOperands().get(0)).getValueAs(String.class));
        assertTrue(fieldMap.getOperands().get(1) instanceof RexInputRef);
        assertEquals(0, ((RexInputRef) fieldMap.getOperands().get(1)).getIndex());

        // Operand 1: MAP('query', 'lap.*top')
        RexCall queryMap = (RexCall) call.getOperands().get(1);
        assertEquals("query", ((RexLiteral) queryMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("lap.*top", ((RexLiteral) queryMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testCaseInsensitiveEmittedOnlyWhenTrue() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test").caseInsensitive(true);
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        // Should have 3 operands: field, query, case_insensitive
        assertEquals(3, call.getOperands().size());
        RexCall ciMap = (RexCall) call.getOperands().get(2);
        assertEquals("case_insensitive", ((RexLiteral) ciMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("true", ((RexLiteral) ciMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testCaseInsensitiveOmittedWhenFalse() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test");
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        // Only 2 operands: field and query (no case_insensitive)
        assertEquals(2, call.getOperands().size());
    }

    public void testFlagsEmittedOnlyWhenNonDefault() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test")
            .flags(org.opensearch.index.query.RegexpFlag.COMPLEMENT, org.opensearch.index.query.RegexpFlag.INTERSECTION);
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        // Should have 3 operands: field, query, flags
        assertEquals(3, call.getOperands().size());
        RexCall flagsMap = (RexCall) call.getOperands().get(2);
        assertEquals("flags", ((RexLiteral) flagsMap.getOperands().get(0)).getValueAs(String.class));
        String flagsValue = ((RexLiteral) flagsMap.getOperands().get(1)).getValueAs(String.class);
        // Raw int bitmask: COMPLEMENT(0x10000) | INTERSECTION(0x0001) = 65537
        int expected = org.opensearch.index.query.RegexpFlag.COMPLEMENT.value() | org.opensearch.index.query.RegexpFlag.INTERSECTION
            .value();
        assertEquals(String.valueOf(expected), flagsValue);
    }

    public void testFlagsNoneRoundTripsAsRawInt() throws ConversionException {
        // NONE (0) previously broke the name-based round-trip because flagsToString produced "0"
        // which RegexpFlag.resolveValue could not parse. Raw int is lossless.
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test").flags(org.opensearch.index.query.RegexpFlag.NONE);
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        assertEquals(3, call.getOperands().size());
        RexCall flagsMap = (RexCall) call.getOperands().get(2);
        assertEquals("flags", ((RexLiteral) flagsMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("0", ((RexLiteral) flagsMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testMaxDeterminizedStatesEmittedOnlyWhenNonDefault() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test").maxDeterminizedStates(20000);
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        // Should have 3 operands: field, query, max_determinized_states
        assertEquals(3, call.getOperands().size());
        RexCall mdsMap = (RexCall) call.getOperands().get(2);
        assertEquals("max_determinized_states", ((RexLiteral) mdsMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("20000", ((RexLiteral) mdsMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testMaxDeterminizedStatesOmittedWhenDefault() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test");
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        assertEquals(2, call.getOperands().size());
    }

    public void testRewriteEmittedOnlyWhenNonNull() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test").rewrite("scoring_boolean");
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;
        // Should have 3 operands: field, query, rewrite
        assertEquals(3, call.getOperands().size());
        RexCall rewriteMap = (RexCall) call.getOperands().get(2);
        assertEquals("rewrite", ((RexLiteral) rewriteMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("scoring_boolean", ((RexLiteral) rewriteMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testAllOptionalParamsEmittedInStableOrder() throws ConversionException {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "ab.*cd")
            .caseInsensitive(true)
            .flags(org.opensearch.index.query.RegexpFlag.COMPLEMENT, org.opensearch.index.query.RegexpFlag.INTERSECTION)
            .maxDeterminizedStates(50000)
            .rewrite("constant_score");
        RexNode result = translator.convert(builder, ctx);
        RexCall call = (RexCall) result;

        // 6 operands: field, query, case_insensitive, flags, max_determinized_states, rewrite
        assertEquals(6, call.getOperands().size());

        // Verify ordering is deterministic — AbstractRelevanceSerializer.optionalParamsStartIndex()
        // assumes optional params begin at index 2.
        RexCall op0 = (RexCall) call.getOperands().get(0);
        assertEquals("field", ((RexLiteral) op0.getOperands().get(0)).getValueAs(String.class));

        RexCall op1 = (RexCall) call.getOperands().get(1);
        assertEquals("query", ((RexLiteral) op1.getOperands().get(0)).getValueAs(String.class));
        assertEquals("ab.*cd", ((RexLiteral) op1.getOperands().get(1)).getValueAs(String.class));

        RexCall op2 = (RexCall) call.getOperands().get(2);
        assertEquals("case_insensitive", ((RexLiteral) op2.getOperands().get(0)).getValueAs(String.class));
        assertEquals("true", ((RexLiteral) op2.getOperands().get(1)).getValueAs(String.class));

        RexCall op3 = (RexCall) call.getOperands().get(3);
        assertEquals("flags", ((RexLiteral) op3.getOperands().get(0)).getValueAs(String.class));
        int expectedFlags = org.opensearch.index.query.RegexpFlag.COMPLEMENT.value() | org.opensearch.index.query.RegexpFlag.INTERSECTION
            .value();
        assertEquals(String.valueOf(expectedFlags), ((RexLiteral) op3.getOperands().get(1)).getValueAs(String.class));

        RexCall op4 = (RexCall) call.getOperands().get(4);
        assertEquals("max_determinized_states", ((RexLiteral) op4.getOperands().get(0)).getValueAs(String.class));
        assertEquals("50000", ((RexLiteral) op4.getOperands().get(1)).getValueAs(String.class));

        RexCall op5 = (RexCall) call.getOperands().get(5);
        assertEquals("rewrite", ((RexLiteral) op5.getOperands().get(0)).getValueAs(String.class));
        assertEquals("constant_score", ((RexLiteral) op5.getOperands().get(1)).getValueAs(String.class));
    }

    public void testBoostRejected() {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test").boost(2.0f);
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(builder, ctx));
        assertTrue(ex.getMessage().contains("boost"));
        assertTrue(ex.getMessage().contains("not supported"));
    }

    public void testNameRejected() {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("name", "test").queryName("my_query");
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(builder, ctx));
        assertTrue(ex.getMessage().contains("_name"));
        assertTrue(ex.getMessage().contains("not supported"));
    }

    public void testNonVarcharFieldRejected() {
        // price is INTEGER (index 1 in TestUtils schema)
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("price", "test");
        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(builder, ctx));
        assertTrue(ex.getMessage().contains("keyword and text"));
        assertTrue(ex.getMessage().contains("price"));
    }

    public void testUnknownFieldThrows() {
        RegexpQueryBuilder builder = QueryBuilders.regexpQuery("nonexistent_field", "test");
        expectThrows(ConversionException.class, () -> translator.convert(builder, ctx));
    }
}
