/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code POST _plugins/analytics_backend_datafusion/liquid_cache/clear}.
 * <p>
 * Clears all Liquid Cache entries (in-memory) and DataFusion internal caches.
 */
public class LiquidCacheClearAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(LiquidCacheClearAction.class);
    private final DataFusionService dataFusionService;

    public LiquidCacheClearAction(DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
    }

    @Override
    public String getName() {
        return "liquid_cache_clear_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "_plugins/analytics_backend_datafusion/liquid_cache/clear"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                logger.info("Clearing Liquid Cache via REST endpoint");
                dataFusionService.clearLiquidCache();
                logger.info("Liquid Cache cleared successfully");
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
