/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.rest;

import org.opensearch.analytics.exec.join.MppStrategy;
import org.opensearch.analytics.exec.join.MppStrategyMetrics;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_analytics/_strategies} — returns per-strategy dispatch counts as JSON.
 *
 * <p>Mainly a test-observability surface so end-to-end IT tests can prove that the intended
 * MPP strategy fired (vs. silently degrading to coordinator-centric) by asserting the counter
 * delta around a query.
 *
 * <p>Response shape (counts are cumulative since node start):
 * <pre>
 * { "strategies": { "BROADCAST": 3, "HASH_SHUFFLE": 0, "HASH_SHUFFLE_AGG": 1, "COORDINATOR_CENTRIC": 12 } }
 * </pre>
 *
 * <p>Counters reflect <b>join- and aggregate-shaped queries only</b> — scans without an MPP-
 * eligible operator above them are not recorded. The dispatcher records the routed strategy on
 * the coordinator node that handled the query; this handler returns only the local node's
 * counters. For cluster-wide totals, callers must fan out to every node and sum (see
 * {@code HashShuffleAggregateIT.readStrategyCounter} for an example). A future revision could
 * make this a node-stats-style action that aggregates internally.
 *
 * @opensearch.internal
 */
public class RestMppStrategyStatsAction extends BaseRestHandler {

    private final MppStrategyMetrics metrics;

    public RestMppStrategyStatsAction(MppStrategyMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public String getName() {
        return "analytics_mpp_strategy_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_analytics/_strategies"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<MppStrategy, Long> snapshot = metrics.snapshot();
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.startObject("strategies");
                for (Map.Entry<MppStrategy, Long> entry : snapshot.entrySet()) {
                    builder.field(entry.getKey().name(), entry.getValue());
                }
                builder.endObject();
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
