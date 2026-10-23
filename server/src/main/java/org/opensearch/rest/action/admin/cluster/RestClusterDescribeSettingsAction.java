/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsAction;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/** REST endpoint for cluster setting definitions, separate from configured values.
 * @opensearch.internal
 */
public class RestClusterDescribeSettingsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/settings/_describe"));
    }

    @Override
    public String getName() {
        return "cluster_describe_settings_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        var describeRequest = new ClusterDescribeSettingsRequest(request.paramAsStringArray("settings", new String[0]));
        describeRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", describeRequest.clusterManagerNodeTimeout())
        );
        return channel -> client.execute(ClusterDescribeSettingsAction.INSTANCE, describeRequest, new RestToXContentListener<>(channel));
    }
}
