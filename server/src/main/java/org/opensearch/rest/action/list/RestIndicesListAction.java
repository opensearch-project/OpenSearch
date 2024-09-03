/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.common.Table;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.pagination.IndexBasedPaginationStrategy;

import java.util.HashMap;
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
    protected PaginationQueryMetadata validateAndGetPaginationMetadata(RestRequest restRequest) {
        Map<String, Object> paginatedQueryParams = new HashMap<>();
        final String requestedTokenStr = restRequest.param("next_token");
        final String sortOrder = restRequest.param("sort", "ascending");
        final int pageSize = restRequest.paramAsInt("size", DEFAULT_LIST_INDICES_PAGE_SIZE_STRING);
        // validating pageSize
        if (pageSize <= 0) {
            throw new IllegalArgumentException("size must be greater than zero");
        } else if (pageSize > MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING) {
            throw new IllegalArgumentException("size should be less than [" + MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING + "]");
        }
        // Validating sort order
        if (!Objects.equals(sortOrder, "ascending") && !Objects.equals(sortOrder, "descending")) {
            throw new IllegalArgumentException("value of sort can either be ascending or descending");
        }
        // Next Token in the request will be validated by the IndexStrategyPageToken itself.
        IndexBasedPaginationStrategy.IndexStrategyPageToken requestedPageToken = requestedTokenStr == null
            ? null
            : new IndexBasedPaginationStrategy.IndexStrategyPageToken(requestedTokenStr);
        paginatedQueryParams.put("next_token", requestedTokenStr);
        paginatedQueryParams.put("size", pageSize);
        paginatedQueryParams.put("sort", sortOrder);
        return new PaginationQueryMetadata(paginatedQueryParams, requestedPageToken);
    }

    @Override
    protected IndexBasedPaginationStrategy getPaginationStrategy(ClusterStateResponse clusterStateResponse) {
        assert !Objects.nonNull(paginationQueryMetadata.getRequestedPageToken())
            || paginationQueryMetadata.getRequestedPageToken() instanceof IndexBasedPaginationStrategy.IndexStrategyPageToken;
        return new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationQueryMetadata.getRequestedPageToken(),
            (int) paginationQueryMetadata.getPaginationQueryParams().get("size"),
            (String) paginationQueryMetadata.getPaginationQueryParams().get("sort"),
            clusterStateResponse.getState()
        );
    }

    @Override
    protected Table.PaginationMetadata getTablePaginationMetadata(IndexBasedPaginationStrategy paginationStrategy) {
        return new Table.PaginationMetadata(
            true,
            PAGINATED_LIST_INDICES_ELEMENT_KEY,
            paginationStrategy.getNextToken() == null ? null : paginationStrategy.getNextToken().generateEncryptedToken()
        );
    }
}
