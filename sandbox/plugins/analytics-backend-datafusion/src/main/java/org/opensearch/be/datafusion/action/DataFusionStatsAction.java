/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action;

import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

/**
 * REST handler for {@code GET _plugins/datafusion/stats}.
 * <p>
 * Collects native executor metrics (Tokio runtime + task monitors) from
 * {@link DataFusionService} and returns them as a JSON response. Follows
 * the same {@code BaseRestHandler} → collect → {@code BytesRestResponse}
 * pattern used by the SQL/PPL stats endpoints.
 */
public class DataFusionStatsAction extends BaseRestHandler {

    private final DataFusionService dataFusionService;

    /**
     * Constructs the stats REST action.
     *
     * @param dataFusionService the node-level DataFusion service providing stats
     */
    public DataFusionStatsAction(DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
    }

    @Override
    public String getName() {
        return "datafusion_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_plugins/datafusion/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                DataFusionStats stats = dataFusionService.getStats();
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
