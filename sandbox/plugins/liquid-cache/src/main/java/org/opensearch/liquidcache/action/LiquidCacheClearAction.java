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

/** {@code POST _plugins/liquid_cache/clear} — clears all in-memory cache entries. */
public class LiquidCacheClearAction extends BaseRestHandler {

    private final LiquidCachePlugin plugin;

    public LiquidCacheClearAction(LiquidCachePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getName() {
        return "liquid_cache_clear_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "_plugins/liquid_cache/clear"),
            // legacy route
            new Route(RestRequest.Method.POST, "_plugins/analytics_backend_datafusion/liquid_cache/clear")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                plugin.clearCache();
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("acknowledged", true);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }
}
