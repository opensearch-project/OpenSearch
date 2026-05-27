/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.parquet;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code GET /_plugins/parquet/_nodes/_stats} and
 * {@code GET /_plugins/parquet/_nodes/{node_id}/_stats}.
 * Delegates to {@link TransportParquetNodeStatsAction} via broadcast-by-node routing.
 *
 * <p><b>SECURITY TODO:</b> This endpoint currently has no authorization checks.
 * Before promoting to GA, add cluster/index permission requirements via
 * {@code ActionPlugin#getRestHandlerWrapper}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetNodeStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "parquet_node_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_plugins/parquet/_nodes/_stats"),
            new Route(RestRequest.Method.GET, "/_plugins/parquet/_nodes/{node_id}/_stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String nodeId = request.param("node_id");
        String level = request.param("level");
        if (level != null && !"shards".equals(level)) {
            throw new IllegalArgumentException("level must be 'shards' but was [" + level + "]");
        }
        boolean shardLevel = "shards".equals(level);

        ParquetNodeStatsRequest statsRequest = new ParquetNodeStatsRequest(shardLevel, nodeId);
        String timeoutParam = request.param("timeout");
        statsRequest.timeout(
            timeoutParam != null
                ? TimeValue.parseTimeValue(timeoutParam, TimeValue.timeValueSeconds(30), "timeout")
                : TimeValue.timeValueSeconds(30)
        );
        return channel -> client.execute(ParquetNodeStatsActionType.INSTANCE, statsRequest, new RestToXContentListener<>(channel));
    }
}
