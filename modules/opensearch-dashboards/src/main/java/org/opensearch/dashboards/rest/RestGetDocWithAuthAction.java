/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.dashboards.action.GetSavedObjectAction;
import org.opensearch.dashboards.action.GetSavedObjectRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Replaces the wrapped RestGetAction for /_opensearch_dashboards/{index}/_doc/{id}.
 * Protected resource types route through GetSavedObjectAction for resource authorization.
 * The transport filter chain evaluates GetSavedObjectRequest (DocRequest) as the current user.
 */
public class RestGetDocWithAuthAction extends BaseRestHandler {

    private static final Set<String> PROTECTED_TYPES = Set.of("dashboard", "visualization");

    @Override
    public String getName() {
        return "opensearch_dashboards_get_doc_with_auth";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_opensearch_dashboards/{index}/_doc/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String id = request.param("id");

        String docType = id.contains(":") ? id.substring(0, id.indexOf(':')) : null;

        if (docType != null && PROTECTED_TYPES.contains(docType)) {
            // Route through GetSavedObjectAction — the transport filter chain will
            // evaluate GetSavedObjectRequest (DocRequest) as the current user for resource authz.
            // If authz passes, TransportGetSavedObjectAction uses DashboardsPluginClient for the inner GET.
            GetSavedObjectRequest soRequest = new GetSavedObjectRequest(index, id);
            return channel -> client.execute(GetSavedObjectAction.INSTANCE, soRequest, new RestToXContentListener<>(channel));
        }

        GetRequest getRequest = new GetRequest(index, id);
        return channel -> client.execute(GetAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }
}
