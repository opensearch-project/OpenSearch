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
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FuzzyQueryTranslator}.
 */
public class FuzzyQueryTranslatorTests extends OpenSearchTestCase {

    private final FuzzyQueryTranslator translator = new FuzzyQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testReportsCorrectQueryType() {
        assertEquals(FuzzyQueryBuilder.class, translator.getQueryType());
    }

    public void testRejectsNonDefaultBoost() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", "laptop").boost(2.0f), ctx)
        );
        assertTrue(ex.getMessage().contains("boost"));
    }

    public void testRejectsQueryName() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", "laptop").queryName("my_query"), ctx)
        );
        assertTrue(ex.getMessage().contains("_name"));
    }

    public void testRejectsNonVarcharField() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("price", "100"), ctx)
        );
        assertTrue(ex.getMessage().contains("INTEGER"));
    }

    public void testRejectsDateField() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("created_date", "2024-01-01"), ctx)
        );
        assertTrue(ex.getMessage().contains("DATE"));
    }

    public void testRejectsVarbinaryField() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("binary_data", "deadbeef"), ctx)
        );
        assertTrue(ex.getMessage().contains("VARBINARY"));
    }

    public void testRejectsBooleanField() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("is_active", "true"), ctx)
        );
        assertTrue(ex.getMessage().contains("BOOLEAN"));
    }

    public void testRejectsUnknownField() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.fuzzyQuery("nonexistent", "value"), ctx));
    }

    // Pins rejection of a structurally invalid fuzziness value (non-numeric, non-AUTO)
    public void testRejectsInvalidFuzziness() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", "laptop").fuzziness(Fuzziness.build("INVALID")), ctx)
        );
        assertTrue(ex.getMessage().contains("fuzziness"));
    }

    public void testRejectsNegativePrefixLength() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", "laptop").prefixLength(-1), ctx)
        );
        assertTrue(ex.getMessage().contains("prefix_length"));
    }

    public void testRejectsZeroMaxExpansions() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", "laptop").maxExpansions(0), ctx)
        );
        assertTrue(ex.getMessage().contains("max_expansions"));
    }

    public void testRejectsNullValue() {
        // FuzzyQueryBuilder constructor rejects null with IllegalArgumentException
        expectThrows(IllegalArgumentException.class, () -> {
            FuzzyQueryBuilder fqb = new FuzzyQueryBuilder("name", (String) null);
            translator.convert(fqb, ctx);
        });
    }

    public void testRejectsEmptyStringValue() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", ""), ctx)
        );
        assertTrue(ex.getMessage().contains("must not be empty"));
    }

    public void testEmitsCorrectOperatorName() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.fuzzyQuery("name", "laptop"), ctx);
        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals("FUZZY", call.getOperator().getName());
    }

    public void testEmitsFieldAsFirstMapOperand() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.fuzzyQuery("name", "laptop"), ctx);
        RexCall call = (RexCall) result;
        // operand[0] should be MAP('field', $inputRef)
        RexCall fieldMap = (RexCall) call.getOperands().get(0);
        RexLiteral key = (RexLiteral) fieldMap.getOperands().get(0);
        assertEquals("field", key.getValueAs(String.class));
        assertTrue(fieldMap.getOperands().get(1) instanceof RexInputRef);
    }

    public void testEmitsValueAsSecondMapOperand() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.fuzzyQuery("name", "laptop"), ctx);
        RexCall call = (RexCall) result;
        // operand[1] should be MAP('query', 'laptop')
        RexCall queryMap = (RexCall) call.getOperands().get(1);
        RexLiteral key = (RexLiteral) queryMap.getOperands().get(0);
        assertEquals("query", key.getValueAs(String.class));
        RexLiteral value = (RexLiteral) queryMap.getOperands().get(1);
        assertEquals("laptop", value.getValueAs(String.class));
    }

    public void testOmitsDefaultParams() throws ConversionException {
        // All defaults: only field + query operands should be emitted
        RexNode result = translator.convert(QueryBuilders.fuzzyQuery("name", "laptop"), ctx);
        RexCall call = (RexCall) result;
        assertEquals(2, call.getOperands().size());
    }

    public void testEmitsNonDefaultParams() throws ConversionException {
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "laptop").fuzziness(Fuzziness.ONE).transpositions(false);
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        // field + query + fuzziness + transpositions = 4 operands
        assertEquals(4, call.getOperands().size());

        // Operand 2: MAP('fuzziness', '1')
        RexCall fuzzinessMap = (RexCall) call.getOperands().get(2);
        assertEquals("fuzziness", ((RexLiteral) fuzzinessMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("1", ((RexLiteral) fuzzinessMap.getOperands().get(1)).getValueAs(String.class));

        // Operand 3: MAP('transpositions', 'false')
        RexCall transpositionsMap = (RexCall) call.getOperands().get(3);
        assertEquals("transpositions", ((RexLiteral) transpositionsMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("false", ((RexLiteral) transpositionsMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testEmitsRewriteAsMapOperand() throws ConversionException {
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "laptop").rewrite("constant_score");
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        // field + query + rewrite = 3 operands
        assertEquals(3, call.getOperands().size());

        RexCall rewriteMap = (RexCall) call.getOperands().get(2);
        assertEquals("rewrite", ((RexLiteral) rewriteMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("constant_score", ((RexLiteral) rewriteMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testNumericValueStringified() throws ConversionException {
        // FuzzyQueryBuilder accepts Object values; numeric values are stringified via toString()
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "42");
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        RexCall queryMap = (RexCall) call.getOperands().get(1);
        assertEquals("42", ((RexLiteral) queryMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testBooleanValueStringified() throws ConversionException {
        // Boolean values are stringified via toString() matching BytesRefs.toBytesRef path
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "true");
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        RexCall queryMap = (RexCall) call.getOperands().get(1);
        assertEquals("true", ((RexLiteral) queryMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testAcceptsCustomAutoFuzziness() throws ConversionException {
        // AUTO:4,7 is a valid custom auto form — should translate without error
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "laptop").fuzziness(Fuzziness.build("AUTO:4,7"));
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        // Non-default fuzziness emits a param operand
        assertEquals(3, call.getOperands().size());
        RexCall fuzzinessMap = (RexCall) call.getOperands().get(2);
        assertEquals("fuzziness", ((RexLiteral) fuzzinessMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("AUTO:4,7", ((RexLiteral) fuzzinessMap.getOperands().get(1)).getValueAs(String.class));
    }

    public void testAcceptsFuzzinessThree() throws ConversionException {
        // "3" is accepted by Fuzziness.build() and clamped to edit distance 2 by asDistance()
        // — legacy parity: server silently clamps rather than rejecting
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "laptop").fuzziness(Fuzziness.build("3"));
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        // Non-default fuzziness emits a param operand
        assertEquals(3, call.getOperands().size());
        RexCall fuzzinessMap = (RexCall) call.getOperands().get(2);
        assertEquals("fuzziness", ((RexLiteral) fuzzinessMap.getOperands().get(0)).getValueAs(String.class));
        assertEquals("3", ((RexLiteral) fuzzinessMap.getOperands().get(1)).getValueAs(String.class));
    }

    // Pins rejection of an alphabetic non-keyword fuzziness ("abc" passes Fuzziness.build()
    // but fails asDistance() — distinct from "INVALID" which is a recognisable keyword form)
    public void testRejectsNonNumericFuzziness() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.fuzzyQuery("name", "laptop").fuzziness(Fuzziness.build("abc")), ctx)
        );
        assertTrue(ex.getMessage().contains("fuzziness"));
    }
}
