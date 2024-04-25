/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.top_queries;

import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.plugin.insights.rules.resthandler.top_queries.RestTopQueriesAction.ALLOWED_METRICS;

public class RestTopQueriesActionTests extends OpenSearchTestCase {

    public void testEmptyNodeIdsValidType() {
        Map<String, String> params = new HashMap<>();
        params.put("type", randomFrom(ALLOWED_METRICS));
        RestRequest restRequest = buildRestRequest(params);
        TopQueriesRequest actual = RestTopQueriesAction.prepareRequest(restRequest);
        assertEquals(0, actual.nodesIds().length);
    }

    public void testNodeIdsValid() {
        Map<String, String> params = new HashMap<>();
        params.put("type", randomFrom(ALLOWED_METRICS));
        String[] nodes = randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(5, 10));
        params.put("nodeId", String.join(",", nodes));

        RestRequest restRequest = buildRestRequest(params);
        TopQueriesRequest actual = RestTopQueriesAction.prepareRequest(restRequest);
        assertArrayEquals(nodes, actual.nodesIds());
    }

    public void testInValidType() {
        Map<String, String> params = new HashMap<>();
        params.put("type", randomAlphaOfLengthBetween(5, 10).toUpperCase(Locale.ROOT));

        RestRequest restRequest = buildRestRequest(params);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> { RestTopQueriesAction.prepareRequest(restRequest); });
        assertEquals(
            String.format(Locale.ROOT, "request [/_insights/top_queries] contains invalid metric type [%s]", params.get("type")),
            exception.getMessage()
        );
    }

    public void testGetRoutes() {
        RestTopQueriesAction action = new RestTopQueriesAction();
        List<RestHandler.Route> routes = action.routes();
        assertEquals(2, routes.size());
        assertEquals("query_insights_top_queries_action", action.getName());
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_insights/top_queries")
            .withParams(params)
            .build();
    }
}
