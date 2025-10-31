/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.loadplugins.LoadPluginsAction;
import org.opensearch.action.admin.cluster.loadplugins.LoadPluginsRequest;
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
 * REST action to dynamically load plugins
 *
 * @opensearch.api
 */
public class RestLoadPluginsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_plugins/load")));
    }

    @Override
    public String getName() {
        return "load_plugins_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        LoadPluginsRequest loadPluginsRequest = new LoadPluginsRequest();
        
        // Parse request parameters
        if (request.hasParam("plugin_path")) {
            loadPluginsRequest.setPluginPath(request.param("plugin_path"));
        }
        
        if (request.hasParam("plugin_name")) {
            loadPluginsRequest.setPluginName(request.param("plugin_name"));
        }
        
        // Parse refresh_analysis parameter (default: false)
        loadPluginsRequest.setRefreshAnalysis(request.paramAsBoolean("refresh_analysis", false));
        
        return channel -> client.admin().cluster().execute(LoadPluginsAction.INSTANCE, loadPluginsRequest, new RestToXContentListener<>(channel));
    }
}
