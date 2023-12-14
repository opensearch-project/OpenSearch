/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestViewAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new NamedRoute.Builder().path("/views").method(GET).uniqueName("cluster.views.list").handler(this::handleGet).build(),
            new NamedRoute.Builder().path("/views").method(POST).uniqueName("cluster.views.create").handler(this::handlePost).build(),
            new NamedRoute.Builder().path("/views/{view_id}")
                .method(GET)
                .uniqueName("cluster.views.get")
                .handler(this::handleSingleGet)
                .build(),
            new NamedRoute.Builder().path("/views/{view_id}")
                .method(DELETE)
                .uniqueName("cluster.views.delete")
                .handler(this::handleSingleDelete)
                .build(),
            new NamedRoute.Builder().path("/views/{view_id}")
                .method(PUT)
                .uniqueName("cluster.views.update")
                .handler(this::handleSinglePut)
                .build()
        );
    }

    @Override
    public String getName() {
        return "refresh_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> {
            // Consume the rest channel
        };
    }

    public RestResponse handleGet(final RestRequest r) {
        return null;
    }

    public RestResponse handlePost(final RestRequest r) {
        return null;
    }

    public RestResponse handleSingleGet(final RestRequest r) {
        return null;
    }

    public RestResponse handleSinglePut(final RestRequest r) {
        return null;
    }

    public RestResponse handleSingleDelete(final RestRequest r) {
        return null;
    }

}
