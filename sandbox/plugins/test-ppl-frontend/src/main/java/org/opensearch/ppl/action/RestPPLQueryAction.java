/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for PPL queries: {@code POST /_analytics/ppl}.
 * Parses {@code {"query": "<ppl>"}} from the request body and
 * delegates to the transport action.
 *
 * <p>Also handles {@code POST /_analytics/ppl/_explain} which executes
 * the query and returns profiling information (stage timings) alongside results.
 */
public class RestPPLQueryAction extends BaseRestHandler {

    private static final String QUERY_ENDPOINT = "/_analytics/ppl";
    private static final String EXPLAIN_ENDPOINT = "/_analytics/ppl/_explain";

    @Override
    public String getName() {
        return "analytics_ppl_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, QUERY_ENDPOINT), new Route(POST, EXPLAIN_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String queryText = null;
        boolean profile = false;
        try (XContentParser parser = request.contentParser()) {
            parser.nextToken();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if ("query".equals(fieldName)) {
                    queryText = parser.text();
                } else if ("profile".equals(fieldName)) {
                    profile = parser.booleanValue();
                } else {
                    parser.skipChildren();
                }
            }
        }
        if (queryText == null || queryText.isEmpty()) {
            throw new IllegalArgumentException("Request body must contain a 'query' field");
        }
        boolean enableProfile = profile || request.path().endsWith("/_explain");
        PPLRequest pplRequest = new PPLRequest(queryText, enableProfile);
        return channel -> client.execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest, new RestToXContentListener<>(channel));
    }
}
