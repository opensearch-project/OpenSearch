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
 */
public class RestPPLQueryAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "analytics_ppl_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_analytics/ppl"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String queryText;
        try (XContentParser parser = request.contentParser()) {
            queryText = parseQueryText(parser);
        }
        PPLRequest pplRequest = new PPLRequest(queryText);
        return channel -> client.execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest, new RestToXContentListener<>(channel));
    }

    private String parseQueryText(XContentParser parser) throws IOException {
        String query = null;
        parser.nextToken(); // START_OBJECT
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("query".equals(fieldName)) {
                query = parser.text();
            } else {
                parser.skipChildren();
            }
        }
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Request body must contain a 'query' field");
        }
        return query;
    }
}
