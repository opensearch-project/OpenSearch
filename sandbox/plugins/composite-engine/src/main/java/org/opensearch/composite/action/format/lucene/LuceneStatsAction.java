/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code GET /_plugins/lucene/{index}/_stats}.
 * Delegates to {@link TransportLuceneStatsAction} via broadcast-by-node routing.
 *
 * <p><b>SECURITY TODO:</b> This endpoint currently has no authorization checks.
 * Before promoting to GA, add cluster/index permission requirements via
 * {@code ActionPlugin#getRestHandlerWrapper}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lucene_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/lucene/{index}/_stats"));
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

        LuceneStatsRequest statsRequest = new LuceneStatsRequest(index, shardLevel, shardFilter, nodeFilter);
        String timeoutParam = request.param("timeout");
        statsRequest.timeout(
            timeoutParam != null
                ? TimeValue.parseTimeValue(timeoutParam, TimeValue.timeValueSeconds(30), "timeout")
                : TimeValue.timeValueSeconds(30)
        );
        return channel -> client.execute(LuceneStatsActionType.INSTANCE, statsRequest, new RestToXContentListener<>(channel));
    }
}
