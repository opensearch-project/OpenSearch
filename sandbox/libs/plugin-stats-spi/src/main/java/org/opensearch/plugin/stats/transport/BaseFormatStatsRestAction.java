/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * Abstract REST handler for per-format index-level stats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class BaseFormatStatsRestAction extends BaseRestHandler {

    protected abstract String formatName();

    protected abstract ActionType<? extends FormatStatsResponse<?>> actionType();

    @Override
    public String getName() {
        return formatName() + "_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/" + formatName() + "/{index}/_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String level = request.param("level");
        if (level != null && !"index".equals(level) && !"shards".equals(level)) {
            throw new IllegalArgumentException("level must be 'index' or 'shards' but was [" + level + "]");
        }
        boolean shardLevel = "shards".equals(level);

        String shardsParam = request.param("shards");
        int[] shardFilter = null;
        if (shardsParam != null) {
            String[] parts = Strings.splitStringByCommaToArray(shardsParam);
            shardFilter = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                try {
                    shardFilter[i] = Integer.parseInt(parts[i].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("shards parameter must be a comma-separated list of integers, got: " + shardsParam);
                }
            }
        }
        String nodesParam = request.param("nodes");
        String[] nodeFilter = nodesParam != null ? Strings.splitStringByCommaToArray(nodesParam) : null;

        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.strictExpandOpen());

        FormatStatsRequest statsRequest = new FormatStatsRequest(formatName(), indices, shardLevel, shardFilter, nodeFilter);
        statsRequest.indicesOptions(indicesOptions);
        return channel -> client.execute(actionType(), statsRequest, new RestToXContentListener<>(channel));
    }
}
