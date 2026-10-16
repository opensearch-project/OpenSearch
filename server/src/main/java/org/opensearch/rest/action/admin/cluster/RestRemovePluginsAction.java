/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.removeplugins.RemovePluginsAction;
import org.opensearch.action.admin.cluster.removeplugins.RemovePluginsRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * REST action for removing plugins
 *
 * @opensearch.internal
 */
public class RestRemovePluginsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_plugins/remove"));
    }

    @Override
    public String getName() {
        return "remove_plugins_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RemovePluginsRequest removePluginsRequest = new RemovePluginsRequest();

        // Parse plugin_name parameter
        String pluginName = request.param("plugin_name");
        if (pluginName != null) {
            removePluginsRequest.setPluginName(pluginName);
        }

        // Parse plugin_names parameter (comma-separated list)
        String pluginNamesParam = request.param("plugin_names");
        if (pluginNamesParam != null) {
            String[] pluginNamesArray = Strings.splitStringByCommaToArray(pluginNamesParam);
            removePluginsRequest.setPluginNames(Arrays.asList(pluginNamesArray));
        }

        // Parse refresh_analysis parameter
        boolean refreshAnalysis = request.paramAsBoolean("refresh_analysis", false);
        removePluginsRequest.setRefreshAnalysis(refreshAnalysis);

        // Parse force parameter
        boolean force = request.paramAsBoolean("force", false);
        removePluginsRequest.setForceRemove(force);

        return channel -> client.admin().cluster().execute(RemovePluginsAction.INSTANCE, removePluginsRequest, new RestToXContentListener<>(channel));
    }
}
