/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dashboards.action.SearchSavedObjectAction;
import org.opensearch.dashboards.action.SearchSavedObjectRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for POST/GET /_opensearch_dashboards/saved_objects/{index}/_search
 */
public class RestSearchSavedObjectAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "opensearch_dashboards_search_saved_objects";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_opensearch_dashboards/saved_objects/{index}/_search"),
            new Route(GET, "/_opensearch_dashboards/saved_objects/{index}/_search")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                sourceBuilder.parseXContent(parser);
            }
        }

        SearchSavedObjectRequest searchRequest = new SearchSavedObjectRequest(index, sourceBuilder);

        return channel -> client.execute(SearchSavedObjectAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
    }
}
