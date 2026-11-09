/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * Abstract REST handler for per-format node-level stats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class BaseFormatNodeStatsRestAction extends BaseRestHandler {

    protected abstract String formatName();

    protected abstract ActionType<? extends FormatNodeStatsResponse<?>> actionType();

    @Override
    public String getName() {
        return formatName() + "_node_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_plugins/" + formatName() + "/_nodes/_stats"),
            new Route(RestRequest.Method.GET, "/_plugins/" + formatName() + "/_nodes/{nodeId}/_stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String nodeId = request.param("nodeId");
        String[] nodeIds = nodeId != null ? Strings.splitStringByCommaToArray(nodeId) : new String[0];
        FormatNodeStatsRequest statsRequest = new FormatNodeStatsRequest(formatName(), nodeIds);
        return channel -> client.execute(actionType(), statsRequest, new RestToXContentListener<>(channel));
    }
}
