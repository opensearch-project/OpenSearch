/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.rest;

import org.opensearch.analytics.exec.join.JoinStrategy;
import org.opensearch.analytics.exec.join.JoinStrategyMetrics;
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
 * <p>Mainly a test-observability surface so end-to-end IT tests can prove that BROADCAST
 * actually fired (vs. silently degrading to coordinator-centric) by asserting the counter
 * delta around a query.
 *
 * <p>Response shape (counts are cumulative since node start):
 * <pre>
 * { "strategies": { "BROADCAST": 3, "HASH_SHUFFLE": 0, "COORDINATOR_CENTRIC": 12 } }
 * </pre>
 *
 * @opensearch.internal
 */
public class RestJoinStrategyStatsAction extends BaseRestHandler {

    private final JoinStrategyMetrics metrics;

    public RestJoinStrategyStatsAction(JoinStrategyMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public String getName() {
        return "analytics_join_strategy_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_analytics/_strategies"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<JoinStrategy, Long> snapshot = metrics.snapshot();
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.startObject("strategies");
                for (Map.Entry<JoinStrategy, Long> entry : snapshot.entrySet()) {
                    builder.field(entry.getKey().name(), entry.getValue());
                }
                builder.endObject();
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
