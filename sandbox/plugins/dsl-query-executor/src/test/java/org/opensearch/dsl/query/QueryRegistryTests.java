/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class QueryRegistryTests extends OpenSearchTestCase {

    private final QueryRegistry registry = QueryRegistryFactory.create();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testResolvesTermQuery() throws ConversionException {
        RexNode result = registry.convert(QueryBuilders.termQuery("name", "laptop"), ctx);

        assertNotNull(result);
        assertFalse(result instanceof UnresolvedQueryCall);
    }

    public void testResolvesMatchAllQuery() throws ConversionException {
        RexNode result = registry.convert(QueryBuilders.matchAllQuery(), ctx);

        assertNotNull(result);
        assertFalse(result instanceof UnresolvedQueryCall);
    }

    public void testUnknownQueryTypeReturnsUnresolved() throws ConversionException {
        RexNode result = registry.convert(QueryBuilders.wildcardQuery("name", "lap*"), ctx);

        assertTrue(result instanceof UnresolvedQueryCall);
        UnresolvedQueryCall unresolved = (UnresolvedQueryCall) result;
        assertTrue(unresolved.getQueryBuilder() instanceof WildcardQueryBuilder);
    }

    public void testEmptyRegistryReturnsUnresolvedForAnyQuery() throws ConversionException {
        QueryRegistry empty = new QueryRegistry();

        assertTrue(empty.convert(QueryBuilders.termQuery("name", "laptop"), ctx) instanceof UnresolvedQueryCall);
        assertTrue(empty.convert(QueryBuilders.matchAllQuery(), ctx) instanceof UnresolvedQueryCall);
    }
}
