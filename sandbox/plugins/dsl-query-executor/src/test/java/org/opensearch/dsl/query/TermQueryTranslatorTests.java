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
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class TermQueryTranslatorTests extends OpenSearchTestCase {

    private final TermQueryTranslator translator = new TermQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testConvertsTermQueryToEquals() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termQuery("name", "laptop"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertTrue(call.getOperands().get(0) instanceof RexInputRef);
        assertTrue(call.getOperands().get(1) instanceof RexLiteral);
        assertEquals("laptop", RexLiteral.stringValue(call.getOperands().get(1)));
    }

    public void testResolvesCorrectFieldIndex() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termQuery("brand", "brandX"), ctx);

        RexCall call = (RexCall) result;
        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        // brand is the 3rd field (index 2) in TestUtils schema: name, price, brand, rating
        assertEquals(2, fieldRef.getIndex());
    }

    public void testIntegerValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 1200), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        // price is the 2nd field (index 1)
        assertEquals(1, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testThrowsForUnknownField() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("nonexistent", "value"), ctx));
    }

    public void testReportsCorrectQueryType() {
        assertEquals(TermQueryBuilder.class, translator.getQueryType());
    }
}
