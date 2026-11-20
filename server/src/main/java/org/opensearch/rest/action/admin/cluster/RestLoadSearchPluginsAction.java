/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.loadsearchplugins.LoadSearchPluginsAction;
import org.opensearch.action.admin.cluster.loadsearchplugins.LoadSearchPluginsRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action to dynamically load search plugins
 *
 * @opensearch.api
 */
public class RestLoadSearchPluginsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_search_plugins/load")));
    }

    @Override
    public String getName() {
        return "load_search_plugins_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        LoadSearchPluginsRequest loadSearchPluginsRequest = new LoadSearchPluginsRequest();
        
        // Parse request parameters
        if (request.hasParam("plugin_path")) {
            loadSearchPluginsRequest.setPluginPath(request.param("plugin_path"));
        }
        
        if (request.hasParam("plugin_name")) {
            loadSearchPluginsRequest.setPluginName(request.param("plugin_name"));
        }
        
        // Parse refresh_search parameter (default: false)
        loadSearchPluginsRequest.setRefreshSearch(request.paramAsBoolean("refresh_search", false));
        
        return channel -> client.admin().cluster().execute(
            LoadSearchPluginsAction.INSTANCE, 
            loadSearchPluginsRequest, 
            new RestToXContentListener<>(channel)
        );
    }
}
