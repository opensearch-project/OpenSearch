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

public class QueryRegistryTests extends OpenSearchTestCase {

    private final QueryRegistry registry = QueryRegistryFactory.create();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testResolvesTermQuery() throws ConversionException {
        RexNode result = registry.convert(QueryBuilders.termQuery("name", "laptop"), ctx);

        // term query translates to an EQUALS call
        assertTrue(result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, (result).getKind());
    }

    public void testResolvesMatchAllQuery() throws ConversionException {
        RexNode result = registry.convert(QueryBuilders.matchAllQuery(), ctx);

        // match_all translates to boolean TRUE literal
        assertTrue(result instanceof RexLiteral);
        assertTrue(RexLiteral.booleanValue(result));
    }

    public void testUnknownQueryTypeReturnsUnresolved() throws ConversionException {
        RexNode result = registry.convert(QueryBuilders.regexpQuery("name", "lap.*"), ctx);

        assertTrue(result instanceof UnresolvedQueryCall);
        UnresolvedQueryCall unresolved = (UnresolvedQueryCall) result;
        assertTrue(unresolved.getQueryBuilder() instanceof org.opensearch.index.query.RegexpQueryBuilder);
    }

    public void testEmptyRegistryReturnsUnresolvedForAnyQuery() throws ConversionException {
        QueryRegistry empty = new QueryRegistry();

        assertTrue(empty.convert(QueryBuilders.termQuery("name", "laptop"), ctx) instanceof UnresolvedQueryCall);
        assertTrue(empty.convert(QueryBuilders.matchAllQuery(), ctx) instanceof UnresolvedQueryCall);
    }
}
