/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

// TODO: once end-to-end execution returns real results, update ITs to verify
//  actual hit count, field values, sort order, and aggregation buckets.
/**
 * Base class for DSL query executor integration tests.
 * Provides shared index setup and search helper.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public abstract class DslIntegTestBase extends OpenSearchIntegTestCase {

    protected static final String INDEX = "test-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class, DslQueryExecutorPlugin.class);
    }

    protected void createTestIndex() {
        createIndex(INDEX);
        ensureGreen();
        client().prepareIndex(INDEX)
            .setId("1")
            .setSource("{\"name\":\"laptop\",\"price\":1200,\"brand\":\"brandX\",\"rating\":4.5}", XContentType.JSON)
            .get();
        refresh(INDEX);
    }

    protected SearchResponse search(SearchSourceBuilder source) {
        return client().search(new SearchRequest(INDEX).source(source)).actionGet();
    }

    protected void assertOk(SearchResponse response) {
        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
    }
}
