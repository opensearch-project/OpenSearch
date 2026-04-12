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
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

public class ExistsQueryTranslatorTests extends OpenSearchTestCase {

    private final ExistsQueryTranslator translator = new ExistsQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testConvertsExistsQueryToIsNotNull() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.existsQuery("name"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
        assertEquals(1, call.getOperands().size());
        assertTrue(call.getOperands().get(0) instanceof RexInputRef);
    }

    public void testResolvesCorrectFieldIndex() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.existsQuery("brand"), ctx);

        RexCall call = (RexCall) result;
        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        // brand is the 3rd field (index 2) in TestUtils schema: name, price, brand, rating
        assertEquals(2, fieldRef.getIndex());
    }

    public void testThrowsForUnknownField() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.existsQuery("nonexistent"), ctx));
    }

    public void testThrowsForBoost() {
        expectThrows(ConversionException.class,
            () -> translator.convert(QueryBuilders.existsQuery("name").boost(2.0f), ctx));
    }

    public void testReportsCorrectQueryType() {
        assertEquals(ExistsQueryBuilder.class, translator.getQueryType());
    }
}
