/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.dashboards.action.GetAdvancedSettingsAction;
import org.opensearch.dashboards.action.GetAdvancedSettingsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetAdvancedSettingsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "opensearch_dashboards_get_advanced_settings";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_opensearch_dashboards/advanced_settings/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        GetAdvancedSettingsRequest getRequest = new GetAdvancedSettingsRequest(index);

        return channel -> client.execute(GetAdvancedSettingsAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
    }
}
