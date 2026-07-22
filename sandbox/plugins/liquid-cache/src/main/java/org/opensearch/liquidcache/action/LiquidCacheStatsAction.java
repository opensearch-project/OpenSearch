/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.liquidcache.action;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.liquidcache.LiquidCachePlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/** {@code GET _plugins/liquid_cache/stats} — returns the node-global cache counters. */
public class LiquidCacheStatsAction extends BaseRestHandler {

    private final LiquidCachePlugin plugin;

    public LiquidCacheStatsAction(LiquidCachePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getName() {
        return "liquid_cache_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_plugins/liquid_cache/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                long[] s = plugin.stats();
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("cache_hit", s[0]);
                builder.field("cache_miss", s[1]);
                builder.field("predicate_evals", s[2]);
                builder.field("memory_evictions", s[3]);
                builder.field("transcodes", s[4]);
                builder.field("total_entries", s[5]);
                builder.field("memory_usage_bytes", s[6]);
                builder.field("max_memory_bytes", s[7]);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }
}
