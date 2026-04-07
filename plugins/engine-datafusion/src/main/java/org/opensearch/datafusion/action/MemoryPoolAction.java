/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for reading memory pool stats.
 *
 * GET /_plugins/datafusion/memory — read current pool stats (limit, usage, peak)
 *
 * Limit changes should be done via the cluster settings API:
 *   PUT /_cluster/settings { "transient": { "datafusion.search.memory_pool": "15gb" } }
 */
public class MemoryPoolAction extends BaseRestHandler {

    private final long runtimePtr;

    public MemoryPoolAction(long runtimePtr) {
        this.runtimePtr = runtimePtr;
    }

    @Override
    public String getName() {
        return "datafusion_memory_pool_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_plugins/datafusion/memory"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            long currentUsage = NativeBridge.getMemoryPoolCurrentUsage(runtimePtr);
            long peakUsage = NativeBridge.getMemoryPoolPeakUsage(runtimePtr);
            long limit = NativeBridge.getMemoryPoolLimit(runtimePtr);

            String json = String.format(
                Locale.ROOT,
                "{\"memory_pool\":{\"limit_bytes\":%d,\"limit_mb\":%d,"
                + "\"current_usage_bytes\":%d,\"current_usage_mb\":%d,"
                + "\"peak_usage_bytes\":%d,\"peak_usage_mb\":%d,"
                + "\"utilization_percent\":%.1f}}",
                limit, limit / (1024 * 1024),
                currentUsage, currentUsage / (1024 * 1024),
                peakUsage, peakUsage / (1024 * 1024),
                limit > 0 ? (currentUsage * 100.0 / limit) : 0
            );
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", json));
        };
    }
}
