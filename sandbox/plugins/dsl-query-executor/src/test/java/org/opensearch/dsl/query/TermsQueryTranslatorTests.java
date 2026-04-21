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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.test.OpenSearchTestCase;

public class TermsQueryTranslatorTests extends OpenSearchTestCase {

    private final TermsQueryTranslator translator = new TermsQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testSingleValueUsesEquals() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("name", "laptop"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(2, call.getOperands().size());
    }

    public void testMultipleStringValuesUsesSearch() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("name", "laptop", "phone"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testResolvesCorrectFieldIndex() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("brand", "brandX", "brandY"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // OR expression has nested structure, get field from first operand
        RexCall firstEquals = (RexCall) call.getOperands().get(0);
        RexInputRef fieldRef = (RexInputRef) firstEquals.getOperands().get(0);
        assertEquals(2, fieldRef.getIndex());
    }

    public void testIntegerValues() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("price", new Object[]{1200, 1500}), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // OR expression has nested structure, get field from first operand
        RexCall firstEquals = (RexCall) call.getOperands().get(0);
        RexInputRef fieldRef = (RexInputRef) firstEquals.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
    }

    public void testDoubleValuesUsesSearch() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.termsQuery("rating", new Object[]{4.5, 4.8, 5.0}), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testThrowsForUnknownField() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("nonexistent", "value"), ctx));
    }

    public void testThrowsForEmptyValues() {
        expectThrows(IllegalArgumentException.class,
            () -> translator.convert(QueryBuilders.termsQuery("name", (Object[]) null), ctx));
    }

    public void testThrowsForBoost() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("name", "laptop").boost(2.0f), ctx));
    }

    public void testThrowsForQueryName() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("name", "laptop").queryName("my_query"), ctx));
    }

    public void testThrowsForTermsLookup() {
        TermsLookup termsLookup = new TermsLookup("lookup_index", "1", "terms");
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.termsLookupQuery("name", termsLookup), ctx));
    }

    public void testThrowsForValueType() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("name", "laptop")
                .valueType(TermsQueryBuilder.ValueType.BITMAP), ctx));
    }

    public void testReportsCorrectQueryType() {
        assertEquals(TermsQueryBuilder.class, translator.getQueryType());
    }
}
