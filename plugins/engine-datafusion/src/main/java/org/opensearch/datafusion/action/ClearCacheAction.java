/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.datafusion.DataFusionService;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler to clear all DataFusion caches (Liquid Cache + file metadata/statistics).
 * POST /_plugins/datafusion/clear_liquid_cache
 */
public class ClearCacheAction extends BaseRestHandler {

    private final DataFusionService dataFusionService;

    public ClearCacheAction(DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
    }

    @Override
    public String getName() {
        return "clear_liquid_cache_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_plugins/datafusion/clear_liquid_cache"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            NativeBridge.clearLiquidCache(dataFusionService.getRuntimePointer());
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "{\"status\":\"ok\",\"message\":\"Cache cleared\"}"));
        };
    }
}
