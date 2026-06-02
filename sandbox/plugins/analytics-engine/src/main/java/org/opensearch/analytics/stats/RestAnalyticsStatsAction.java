/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code GET _plugins/_analytics/stats}. Returns a snapshot of
 * node-local query / stage / fragment counters from {@link AnalyticsStatsCollector}.
 *
 * <p>Per-node only for v1 — broadcast via {@code TransportNodesAction} is a
 * follow-up. Mirrors the simple {@code BaseRestHandler} pattern used by
 * {@code DataFusionStatsAction}.
 *
 * <p>Marked {@link ExperimentalApi} — route, response shape, and aggregation
 * scope (per-node vs. cluster-wide) may evolve.
 */
@ExperimentalApi
public class RestAnalyticsStatsAction extends BaseRestHandler {

    private final AnalyticsStatsCollector collector;

    public RestAnalyticsStatsAction(AnalyticsStatsCollector collector) {
        this.collector = collector;
    }

    @Override
    public String getName() {
        return "analytics_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_plugins/_analytics/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                AnalyticsStats stats = collector.snapshot();
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                stats.toXContent(builder, request);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }
}
