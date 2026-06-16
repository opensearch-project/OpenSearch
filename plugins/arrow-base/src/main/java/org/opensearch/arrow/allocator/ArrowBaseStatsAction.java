/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.function.Supplier;

/**
 * REST handler exposing per-pool native memory stats at {@code _plugins/arrow_base/stats}.
 */
public class ArrowBaseStatsAction extends BaseRestHandler {
    private final Supplier<NativeAllocatorPoolStats> statsSupplier;

    /**
     * Creates a new stats action.
     * @param statsSupplier supplier of pool stats
     */
    public ArrowBaseStatsAction(Supplier<NativeAllocatorPoolStats> statsSupplier) {
        this.statsSupplier = statsSupplier;
    }

    @Override
    public String getName() {
        return "arrow_base_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_plugins/arrow_base/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            NativeAllocatorPoolStats stats = statsSupplier.get();
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.startObject("memory_pools");
            if (stats != null) {
                builder.startObject("runtime");
                builder.field("allocated_bytes", stats.getNativeAllocatedBytes());
                builder.field("resident_bytes", stats.getNativeResidentBytes());
                builder.endObject();
                builder.startObject("pools");
                for (NativeAllocatorPoolStats.PoolStats pool : stats.getPools()) {
                    builder.startObject(pool.getName());
                    builder.field("allocated_bytes", pool.getAllocatedBytes());
                    builder.field("peak_bytes", pool.getPeakBytes());
                    builder.field("limit_bytes", pool.getLimitBytes());
                    builder.field("min_bytes", pool.getMinBytes());
                    builder.field("group", pool.getGroup());
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
