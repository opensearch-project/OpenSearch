/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Table;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.action.cat.RestTable;
import org.opensearch.rest.pagination.IndexBasedPaginationStrategy;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _list API action to output indices in pages.
 *
 * @opensearch.api
 */
public class RestIndicesListAction extends AbstractListAction {
    private static final String DEFAULT_LIST_INDICES_PAGE_SIZE_STRING = "1000";
    private static final String PAGINATED_ELEMENT_KEY = "indices";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_list/indices"), new Route(GET, "/_list/indices/{index}")));
    }

    @Override
    public String getName() {
        return "list_indices_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_list/indices\n");
        sb.append("/_list/indices/{index}\n");
    }

    @Override
    public RestChannelConsumer doListRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final boolean local = request.paramAsBoolean("local", false);
        TimeValue clusterManagerTimeout = request.paramAsTime("cluster_manager_timeout", DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
        final boolean includeUnloadedSegments = request.paramAsBoolean("include_unloaded_segments", false);
        final String requestedToken = request.param("next_token");
        final int pageSize = Integer.parseInt(request.param("size", DEFAULT_LIST_INDICES_PAGE_SIZE_STRING));
        final String requestedSortOrder = request.param("sort", "ascending");

        return channel -> {
            final ActionListener<Table> listener = ActionListener.notifyOnce(new RestResponseListener<Table>(channel) {
                @Override
                public RestResponse buildResponse(final Table table) throws Exception {
                    return RestTable.buildResponse(table, channel);
                }
            });

            // Fetch all the indices from clusterStateRequest for a paginated query.
            RestIndicesAction.RestIndicesActionCommonUtils.sendClusterStateRequest(
                indices,
                IndicesOptions.lenientExpandHidden(),
                local,
                clusterManagerTimeout,
                client,
                new ActionListener<ClusterStateResponse>() {
                    @Override
                    public void onResponse(final ClusterStateResponse clusterStateResponse) {
                        try {
                            IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(
                                requestedToken == null ? null : new IndexBasedPaginationStrategy.IndexStrategyPageToken(requestedToken),
                                pageSize,
                                requestedSortOrder,
                                clusterStateResponse.getState()
                            );

                            final GroupedActionListener<ActionResponse> groupedListener = RestIndicesAction.RestIndicesActionCommonUtils
                                .createGroupedListener(
                                    request,
                                    4,
                                    listener,
                                    new Table.PaginationMetadata(
                                        true,
                                        PAGINATED_ELEMENT_KEY,
                                        paginationStrategy.getNextToken() == null
                                            ? null
                                            : paginationStrategy.getNextToken().generateEncryptedToken()
                                    )
                                );
                            groupedListener.onResponse(clusterStateResponse);

                            final String[] indicesToBeQueried = paginationStrategy.getElementsFromRequestedToken().toArray(new String[0]);
                            RestIndicesAction.RestIndicesActionCommonUtils.sendGetSettingsRequest(
                                indicesToBeQueried,
                                IndicesOptions.fromRequest(request, IndicesOptions.strictExpand()),
                                local,
                                clusterManagerTimeout,
                                client,
                                ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure)
                            );
                            RestIndicesAction.RestIndicesActionCommonUtils.sendIndicesStatsRequest(
                                indicesToBeQueried,
                                IndicesOptions.lenientExpandHidden(),
                                includeUnloadedSegments,
                                client,
                                ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure)
                            );
                            RestIndicesAction.RestIndicesActionCommonUtils.sendClusterHealthRequest(
                                indicesToBeQueried,
                                IndicesOptions.lenientExpandHidden(),
                                local,
                                clusterManagerTimeout,
                                client,
                                ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure)
                            );
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        listener.onFailure(e);
                    }
                }
            );
        };

    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(asList("local", "health"));
        responseParams.addAll(AbstractListAction.RESPONSE_PARAMS);
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        return RestIndicesAction.RestIndicesActionCommonUtils.getTableWithHeader(request, null);
    }

}
