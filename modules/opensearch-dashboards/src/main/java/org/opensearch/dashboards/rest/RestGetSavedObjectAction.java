/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.dashboards.action.GetSavedObjectAction;
import org.opensearch.dashboards.action.GetSavedObjectRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for GET /_opensearch_dashboards/saved_objects/{index}/{id}
 */
public class RestGetSavedObjectAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "opensearch_dashboards_get_saved_object";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_opensearch_dashboards/saved_objects/{index}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String documentId = request.param("id");
        GetSavedObjectRequest getRequest = new GetSavedObjectRequest(index, documentId);

        return channel -> client.execute(GetSavedObjectAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }
}
