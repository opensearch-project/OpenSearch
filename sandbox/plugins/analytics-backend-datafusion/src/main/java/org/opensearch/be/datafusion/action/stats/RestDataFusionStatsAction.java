/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for DataFusion cluster stats.
 *
 * <p>Parses path parameters ({@code nodeId}, {@code stat}) and delegates to
 * {@link TransportDataFusionStatsAction} via the transport layer.
 *
 * <p>Routes:
 * <ul>
 *   <li>{@code GET /_plugins/_analytics_backend_datafusion/{nodeId}/stats/{stat}}</li>
 *   <li>{@code GET /_plugins/_analytics_backend_datafusion/{nodeId}/stats}</li>
 *   <li>{@code GET /_plugins/_analytics_backend_datafusion/stats/{stat}}</li>
 *   <li>{@code GET /_plugins/_analytics_backend_datafusion/stats}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class RestDataFusionStatsAction extends BaseRestHandler {

    private static final String CANONICAL_PREFIX = "/_plugins/_analytics_backend_datafusion";

    private static final Set<String> VALID_STAT_NAMES = Set.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "datanode_gate",
        "coordinator_gate",
        "spill"
    );

    /**
     * Comma-separated list of valid stat section names for display in error messages.
     * Derived from {@link #VALID_STAT_NAMES} so there is a single place to edit when adding stats.
     */
    public static final String VALID_STATS = VALID_STAT_NAMES.stream().sorted().collect(Collectors.joining(", "));

    @Override
    public String getName() {
        return "datafusion_stats_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, CANONICAL_PREFIX + "/{nodeId}/stats/{stat}"),
                new Route(GET, CANONICAL_PREFIX + "/{nodeId}/stats"),
                new Route(GET, CANONICAL_PREFIX + "/stats/{stat}"),
                new Route(GET, CANONICAL_PREFIX + "/stats")
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse nodeId: default to all nodes if not provided
        String[] nodeIds;
        String nodeIdParam = request.param("nodeId");
        if (nodeIdParam != null && !nodeIdParam.isEmpty()) {
            nodeIds = Strings.splitStringByCommaToArray(nodeIdParam);
        } else {
            nodeIds = new String[0];
        }

        // Parse stat filter: comma-separated stat section names
        Set<String> statsToRetrieve = new HashSet<>();
        String statParam = request.param("stat");
        if (statParam != null && !statParam.isEmpty()) {
            String[] requestedStats = Strings.splitStringByCommaToArray(statParam);

            // Validate stat names — collect all invalid names and report together
            List<String> invalidStats = Arrays.stream(requestedStats).filter(s -> !VALID_STAT_NAMES.contains(s)).toList();
            if (!invalidStats.isEmpty()) {
                return channel -> {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("error", "Invalid stat sections: " + invalidStats + ". Valid values are: " + VALID_STATS);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
                };
            }
            statsToRetrieve.addAll(Arrays.asList(requestedStats));
        }

        DataFusionStatsNodesRequest nodesRequest = new DataFusionStatsNodesRequest(nodeIds, statsToRetrieve);

        return channel -> client.execute(
            DataFusionStatsActionType.INSTANCE,
            nodesRequest,
            new RestBuilderListener<DataFusionStatsNodesResponse>(channel) {
                @Override
                public org.opensearch.rest.RestResponse buildResponse(DataFusionStatsNodesResponse response, XContentBuilder builder)
                    throws Exception {
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            }
        );
    }
}
