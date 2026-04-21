/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for DSL query conversion (filter path).
 * Uses various query types; sort and projection use defaults.
 */
public class DslQueryIT extends DslIntegTestBase {

    public void testNoQuery() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()));
    }

    public void testMatchAll() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));
    }

    public void testTermQuery() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "laptop"))));
    }

    public void testWildcardQueryWithUnresolvedNode() {
        createTestIndex();
        // Wildcard query is not converted to standard Rex — wraps in UnresolvedQueryCall.
        assertOk(search(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "lap*"))));
    }

    public void testFailsForNonexistentIndex() {
        expectThrows(
            Exception.class,
            () -> client().search(new SearchRequest("nonexistent-index").source(new SearchSourceBuilder())).actionGet()
        );
    }

    public void testFailsForMultipleIndices() {
        createTestIndex();
        createIndex("test-index-2");
        ensureGreen();

        expectThrows(
            Exception.class,
            () -> client().search(new SearchRequest(INDEX, "test-index-2").source(new SearchSourceBuilder())).actionGet()
        );
    }
}
