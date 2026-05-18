/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.composite.stats.CompositeStatsRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST handler for {@code GET /_plugins/dataformat_stats} and
 * {@code GET /_plugins/dataformat_stats/{index}}.
 * <p>
 * Collects composite engine stats from all active shard engines and returns
 * them grouped by index. Supports optional {@code level=shards} query param
 * for shard-level detail.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "dataformat_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_plugins/dataformat_stats"),
            new Route(RestRequest.Method.GET, "/_plugins/dataformat_stats/{index}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String indexFilter = request.param("index");
        String level = request.param("level");
        boolean shardLevel = "shards".equals(level);

        return channel -> {
            try {
                Map<ShardId, CompositeIndexingExecutionEngine> engines = CompositeStatsRegistry.getInstance().getEngines();

                // Group stats by index name
                Map<String, Map<ShardId, CompositeShardStats>> byIndex = new HashMap<>();
                for (Map.Entry<ShardId, CompositeIndexingExecutionEngine> entry : engines.entrySet()) {
                    String indexName = entry.getKey().getIndexName();
                    if (indexFilter != null && !indexFilter.equals(indexName)) {
                        continue;
                    }
                    byIndex.computeIfAbsent(indexName, k -> new HashMap<>()).put(entry.getKey(), entry.getValue().getStats());
                }

                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.startObject("indices");
                for (Map.Entry<String, Map<ShardId, CompositeShardStats>> indexEntry : byIndex.entrySet()) {
                    builder.startObject(indexEntry.getKey());

                    // Aggregated stats across shards
                    builder.startObject("composite");
                    CompositeShardStats aggregated = CompositeShardStats.aggregate(indexEntry.getValue().values());
                    aggregated.toXContent(builder, request);
                    builder.endObject();

                    // Shard-level detail
                    if (shardLevel) {
                        builder.startObject("shards");
                        for (Map.Entry<ShardId, CompositeShardStats> shardEntry : indexEntry.getValue().entrySet()) {
                            builder.startObject(String.valueOf(shardEntry.getKey().id()));
                            shardEntry.getValue().toXContent(builder, request);
                            builder.endObject();
                        }
                        builder.endObject();
                    }

                    builder.endObject();
                }
                builder.endObject();
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }
}
