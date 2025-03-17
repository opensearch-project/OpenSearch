/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActions;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.wlm.ResourceType;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.common.Table;
import org.opensearch.wlm.stats.QueryGroupStats;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.RestTable;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get Workload Management stats
 *
 * @opensearch.experimental
 */
public class RestWlmStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "_wlm/stats"),
                new Route(GET, "_wlm/{nodeId}/stats"),
                new Route(GET, "_wlm/stats/{workloadGroupId}"),
                new Route(GET, "_wlm/{nodeId}/stats/{workloadGroupId}"),
                new Route(GET, "_list/wlm_stats")
            )
        );
    }

    @Override
    public String getName() {
        return "wlm_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> workloadGroupIds = Strings.tokenizeByCommaToSet(request.param("workloadGroupId", "_all"));
        Boolean breach = request.hasParam("breach") ? Boolean.parseBoolean(request.param("boolean")) : null;
        WlmStatsRequest wlmStatsRequest = new WlmStatsRequest(nodesIds, workloadGroupIds, breach);

        boolean isTabular = request.rawPath().contains("_list/wlm_stats");

        if (isTabular) {
            return channel -> client.admin().cluster().wlmStats(wlmStatsRequest,
                new RestResponseListener<WlmStatsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(WlmStatsResponse response) throws Exception {
                        Table table = buildTable(response);
                        request.params().put("v", "true");
                        return RestTable.buildResponse(table, channel);
                    }
                }
            );
        }

        return channel -> client.admin().cluster().wlmStats(wlmStatsRequest, new RestActions.NodesResponseRestListener<>(channel));
    }

    /**
     * Builds a tabular response with '|' column separators.
     */
    private Table buildTable(WlmStatsResponse response) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("NODE_ID", "desc:Node ID");
        table.addCell("|");
        table.addCell("QUERY_GROUP_ID", "desc:Query Group");
        table.addCell("|");
        table.addCell("TOTAL_COMPLETIONS", "desc:Total Completed Queries");
        table.addCell("|");
        table.addCell("TOTAL_REJECTIONS", "desc:Total Rejected Queries");
        table.addCell("|");
        table.addCell("TOTAL_CANCELLATIONS", "desc:Total Canceled Queries");
        table.addCell("|");
        table.addCell("CPU_USAGE", "desc:CPU Usage");
        table.addCell("|");
        table.addCell("CPU_CANCELLATIONS", "desc:CPU Canceled");
        table.addCell("|");
        table.addCell("CPU_REJECTIONS", "desc:CPU Rejected");
        table.addCell("|");
        table.addCell("MEMORY_USAGE", "desc:Memory Usage");
        table.addCell("|");
        table.addCell("MEMORY_CANCELLATIONS", "desc:Memory Canceled");
        table.addCell("|");
        table.addCell("MEMORY_REJECTIONS", "desc:Memory Rejected");
        table.endHeaders();

        for (WlmStats wlmStats : response.getNodes()) {
            String nodeId = wlmStats.getNode().getId();
            QueryGroupStats queryGroupStats = wlmStats.getWorkloadGroupStats();

            for (Map.Entry<String, QueryGroupStats.QueryGroupStatsHolder> entry : queryGroupStats.getStats().entrySet()) {
                String queryGroupId = entry.getKey();
                QueryGroupStats.QueryGroupStatsHolder statsHolder = entry.getValue();

                table.startRow();
                table.addCell(nodeId);
                table.addCell("|");
                table.addCell(queryGroupId);
                table.addCell("|");
                table.addCell(statsHolder.getCompletions());
                table.addCell("|");
                table.addCell(statsHolder.getRejections());
                table.addCell("|");
                table.addCell(statsHolder.getCancellations());
                table.addCell("|");

                QueryGroupStats.ResourceStats cpuStats = statsHolder.getResourceStats().get(ResourceType.CPU);
                QueryGroupStats.ResourceStats memoryStats = statsHolder.getResourceStats().get(ResourceType.MEMORY);

                table.addCell(cpuStats != null ? cpuStats.getCurrentUsage() : 0);
                table.addCell("|");
                table.addCell(cpuStats != null ? cpuStats.getCancellations() : 0);
                table.addCell("|");
                table.addCell(cpuStats != null ? cpuStats.getRejections() : 0);
                table.addCell("|");
                table.addCell(memoryStats != null ? memoryStats.getCurrentUsage() : 0);
                table.addCell("|");
                table.addCell(memoryStats != null ? memoryStats.getCancellations() : 0);
                table.addCell("|");
                table.addCell(memoryStats != null ? memoryStats.getRejections() : 0);
                table.endRow();
            }
        }

        return table;
    }
}
