/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
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
import org.opensearch.action.pagination.WlmPaginationStrategy;
import org.opensearch.action.pagination.SortBy;
import org.opensearch.action.pagination.SortOrder;
import org.opensearch.action.pagination.PageToken;

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

    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int MAX_PAGE_SIZE = 100;
    private static final Logger logger = LogManager.getLogger(RestWlmStatsAction.class);


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

        int pageSize = parsePageSize(request);
        String nextToken = request.param("next_token");
        SortBy sortBy = parseSortBy(request.param("sort", "node_id"));
        SortOrder sortOrder = parseSortOrder(request.param("order", "asc"));

        if (request.rawPath().contains("_list/wlm_stats")) {
            return handleTabularRequest(request, client, wlmStatsRequest, pageSize, nextToken, sortBy, sortOrder);
        }

        return channel -> client.admin().cluster().wlmStats(wlmStatsRequest, new RestActions.NodesResponseRestListener<>(channel));
    }

    private RestChannelConsumer handleTabularRequest(
        RestRequest request,
        NodeClient client,
        WlmStatsRequest wlmStatsRequest,
        int pageSize,
        String nextToken,
        SortBy sortBy,
        SortOrder sortOrder
    ) {
        return channel -> client.admin().cluster().wlmStats(wlmStatsRequest,
            new RestResponseListener<WlmStatsResponse>(channel) {
                @Override
                public RestResponse buildResponse(WlmStatsResponse response) throws Exception {
                    try {
                        WlmPaginationStrategy paginationStrategy =
                            new WlmPaginationStrategy(pageSize, nextToken, sortBy, sortOrder, response);

                        List<WlmStats> paginatedStats = paginationStrategy.getPaginatedStats();
                        PageToken nextPageToken = paginationStrategy.getResponseToken();

                        Table paginatedTable = createTableWithHeaders(nextPageToken);
                        buildTable(paginatedTable, paginatedStats, paginationStrategy);

                        request.params().put("v", "true");
                        return RestTable.buildResponse(paginatedTable, channel);
                    } catch (OpenSearchParseException e) {
                        handlePaginationError(channel, nextToken, pageSize, sortBy, sortOrder, e);
                        return null;
                    }
                }
            }
        );
    }

    private SortBy parseSortBy(String sortByParam) throws OpenSearchParseException {
        try {
            return SortBy.fromString(sortByParam);
        } catch (IllegalArgumentException e) {
            throw new OpenSearchParseException("Invalid value for 'sort'. Allowed: 'node_id', 'query_group'", e);
        }
    }

    private SortOrder parseSortOrder(String sortOrderParam) throws OpenSearchParseException {
        try {
            return SortOrder.fromString(sortOrderParam);
        } catch (IllegalArgumentException e) {
            throw new OpenSearchParseException("Invalid value for 'order'. Allowed: 'asc', 'desc'", e);
        }
    }

    private int parsePageSize(RestRequest request) {
        int pageSize = request.paramAsInt("size", DEFAULT_PAGE_SIZE);
        if (pageSize <= 0 || pageSize > MAX_PAGE_SIZE) {
            throw new OpenSearchParseException("Invalid value for 'size'. Allowed range: 1 to " + MAX_PAGE_SIZE);
        }
        return pageSize;
    }

    private void handlePaginationError(RestChannel channel, String nextToken, int pageSize, SortBy sortBy, SortOrder sortOrder, OpenSearchParseException e) throws IOException {
        String userMessage = "Pagination state has changed (e.g., new query groups added or removed). "
            + "Please restart pagination from the beginning by omitting the 'next_token' parameter.";

        logger.error("Failed to paginate WLM stats. next_token={}, pageSize={}, sortBy={}, sortOrder={}. Reason: {}",
            nextToken, pageSize, sortBy, sortOrder, e.getMessage(), e);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("error", userMessage);
        builder.field("details", e.getMessage());
        builder.endObject();

        channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
    }

    private Table createTableWithHeaders(PageToken pageToken) {
        Table table = new Table(pageToken);
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
        table.addCell("MEMORY_USAGE", "desc:Memory Usage");
        table.endHeaders();
        return table;
    }

    private void addRow(Table table, String nodeId, String queryGroupId, QueryGroupStats.QueryGroupStatsHolder statsHolder) {
        final String PLACEHOLDER = "NA";

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

        table.addCell(cpuStats != null ? cpuStats.getCurrentUsage() : PLACEHOLDER);
        table.addCell("|");
        table.addCell(memoryStats != null ? memoryStats.getCurrentUsage() : PLACEHOLDER);
        table.endRow();
    }

    private void addFooterRow(Table table, int COLUMN_COUNT) {
        table.startRow();
        table.addCell("No more pages available");
        for (int i = 1; i < COLUMN_COUNT; i++) {
            table.addCell("-");
        }
        table.endRow();
    }

    /**
     * Builds a tabular response with '|' column separators.
     */
    private void buildTable(Table table, List<WlmStats> paginatedStats, WlmPaginationStrategy paginationStrategy) {
        final int COLUMN_COUNT = 13;

        for (WlmStats wlmStats : paginatedStats) {
            String nodeId = wlmStats.getNode().getId();
            QueryGroupStats queryGroupStats = wlmStats.getWorkloadGroupStats();

            for (Map.Entry<String, QueryGroupStats.QueryGroupStatsHolder> entry : queryGroupStats.getStats().entrySet()) {
                String queryGroupId = entry.getKey();
                QueryGroupStats.QueryGroupStatsHolder statsHolder = entry.getValue();
                addRow(table, nodeId, queryGroupId, statsHolder);
            }
        }

        if (paginationStrategy.getResponseToken() == null) {
            addFooterRow(table, COLUMN_COUNT);
        }
    }
}
