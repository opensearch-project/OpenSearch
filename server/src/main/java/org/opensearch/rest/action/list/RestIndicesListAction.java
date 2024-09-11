/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Table;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.pagination.IndexBasedPaginationStrategy;
import org.opensearch.rest.pagination.PaginatedQueryRequest;
import org.opensearch.rest.pagination.PaginatedQueryResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _list API action to output indices in pages.
 *
 * @opensearch.api
 */
public class RestIndicesListAction extends RestIndicesAction {

    protected static final int MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING = 1000;
    protected static final int DEFAULT_LIST_INDICES_PAGE_SIZE_STRING = 1000;
    protected static final String PAGINATED_LIST_INDICES_ELEMENT_KEY = "indices";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_list/indices"), new Route(GET, "/_list/indices/{index}")));
    }

    @Override
    public String getName() {
        return "list_indices_action";
    }

    @Override
    public void documentation(StringBuilder sb) {
        sb.append("/_list/indices\n");
        sb.append("/_list/indices/{index}\n");
    }

    @Override
    public boolean isActionPaginated() {
        return true;
    }

    @Override
    protected PaginatedQueryRequest validateAndGetPaginationMetadata(RestRequest restRequest) {
        PaginatedQueryRequest paginatedQueryRequest = restRequest.parsePaginatedQueryParams(
            "ascending",
            DEFAULT_LIST_INDICES_PAGE_SIZE_STRING
        );
        // validating pageSize
        if (paginatedQueryRequest.getSize() <= 0) {
            throw new IllegalArgumentException("size must be greater than zero");
        } else if (paginatedQueryRequest.getSize() > MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING) {
            throw new IllegalArgumentException("size should be less than [" + MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING + "]");
        }
        // Validating sort order
        if (!Objects.equals(paginatedQueryRequest.getSort(), "ascending")
            && !Objects.equals(paginatedQueryRequest.getSort(), "descending")) {
            throw new IllegalArgumentException("value of sort can either be ascending or descending");
        }
        // Next Token in the request will be validated by the IndexStrategyTokenParser itself.
        if (Objects.nonNull(paginatedQueryRequest.getRequestedTokenStr())) {
            IndexBasedPaginationStrategy.IndexStrategyToken.validateIndexStrategyToken(paginatedQueryRequest.getRequestedTokenStr());
        }

        return paginatedQueryRequest;
    }

    @Override
    protected Table buildTable(
        final RestRequest request,
        final Map<String, Settings> indicesSettings,
        final Map<String, ClusterIndexHealth> indicesHealths,
        final Map<String, IndexStats> indicesStats,
        final Map<String, IndexMetadata> indicesMetadatas,
        final String[] indicesToBeQueried,
        final PaginatedQueryResponse paginatedQueryResponse
    ) {
        final String healthParam = request.param("health");
        final Table table = getTableWithHeader(request, paginatedQueryResponse);
        for (String indexName : indicesToBeQueried) {
            if (indicesSettings.containsKey(indexName) == false) {
                continue;
            }
            buildRow(indicesSettings, indicesHealths, indicesStats, indicesMetadatas, healthParam, indexName, table);
        }
        return table;
    }

    @Override
    protected IndexBasedPaginationStrategy getPaginationStrategy(ClusterStateResponse clusterStateResponse) {
        return new IndexBasedPaginationStrategy(paginatedQueryRequest, PAGINATED_LIST_INDICES_ELEMENT_KEY, clusterStateResponse.getState());
    }

    @Override
    protected PaginatedQueryResponse getPaginatedQueryResponse(IndexBasedPaginationStrategy paginationStrategy) {
        return paginationStrategy.getPaginatedQueryResponse();
    }

    protected String[] getIndicesToBeQueried(String[] indices, IndexBasedPaginationStrategy paginationStrategy) {
        return paginationStrategy.getElementsFromRequestedToken().toArray(new String[0]);
    }
}
