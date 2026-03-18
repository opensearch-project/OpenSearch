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

/**
 * Integration test that verifies the full DSL query execution pipeline:
 * SearchActionFilter → TransportDslExecuteAction → SearchSourceConverter → DslQueryPlanExecutor → SearchResponse.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class DslQueryExecutorIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class, DslQueryExecutorPlugin.class);
    }

    public void testSearchReturnsEmptyResponse() {
        createTestIndex();

        SearchResponse response = client().search(
            new SearchRequest(INDEX).source(new SearchSourceBuilder())
        ).actionGet();

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertTrue(response.getTook().millis() >= 0);
        // Currently returns empty hits (stub) , will be populated when conversion is implemented
        assertEquals(0L, response.getHits().getTotalHits().value());
    }

    public void testSearchFailsForNonexistentIndex() {
        expectThrows(Exception.class, () ->
            client().search(
                new SearchRequest("nonexistent-index").source(new SearchSourceBuilder())
            ).actionGet()
        );
    }

    public void testSearchFailsForMultipleIndices() {
        createTestIndex();
        createIndex("test-index-2");
        ensureGreen();

        expectThrows(Exception.class, () ->
            client().search(
                new SearchRequest(INDEX, "test-index-2").source(new SearchSourceBuilder())
            ).actionGet()
        );
    }

    private void createTestIndex() {
        createIndex(INDEX);
        ensureGreen();
        client().prepareIndex(INDEX)
            .setId("1")
            .setSource("{\"name\":\"laptop\",\"price\":1200}", XContentType.JSON)
            .get();
        refresh(INDEX);
    }
}
