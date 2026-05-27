/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code GET /_plugins/dataformat_stats/{index}}.
 * Delegates to {@link TransportDataFormatStatsAction} via broadcast-by-node routing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "dataformat_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/dataformat_stats/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String index = request.param("index");
        String level = request.param("level");
        if (level != null && !"index".equals(level) && !"shards".equals(level)) {
            throw new IllegalArgumentException("level must be 'index' or 'shards' but was [" + level + "]");
        }
        boolean shardLevel = "shards".equals(level);
        String shardParam = request.param("shard");
        Integer shardFilter = shardParam != null ? Integer.parseInt(shardParam) : null;
        String nodeFilter = request.param("node");

        DataFormatStatsRequest statsRequest = new DataFormatStatsRequest(index, shardLevel, shardFilter, nodeFilter);
        return channel -> client.execute(DataFormatStatsActionType.INSTANCE, statsRequest, new RestToXContentListener<>(channel));
    }
}
