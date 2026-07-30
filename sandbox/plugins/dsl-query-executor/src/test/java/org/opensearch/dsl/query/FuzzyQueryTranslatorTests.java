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

    public void testRejectsUnknownField() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.fuzzyQuery("nonexistent", "value"), ctx));
    }

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
        expectThrows(ConversionException.class, () -> {
            // FuzzyQueryBuilder constructor rejects null; translator must catch and wrap
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
        FuzzyQueryBuilder fqb = QueryBuilders.fuzzyQuery("name", "laptop")
            .fuzziness(Fuzziness.ONE)
            .transpositions(false);
        RexNode result = translator.convert(fqb, ctx);
        RexCall call = (RexCall) result;
        // field + query + fuzziness + transpositions = 4 operands
        assertTrue(call.getOperands().size() > 2);
    }
}
