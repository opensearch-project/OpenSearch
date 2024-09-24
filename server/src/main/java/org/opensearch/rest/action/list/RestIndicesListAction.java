/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.pagination.IndexPaginationStrategy;
import org.opensearch.rest.pagination.PageParams;

import java.util.Iterator;
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

    protected static final int MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING = 5000;
    protected static final int DEFAULT_LIST_INDICES_PAGE_SIZE_STRING = 500;

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
    protected PageParams validateAndGetPageParams(RestRequest restRequest) {
        PageParams pageParams = super.validateAndGetPageParams(restRequest);
        // validate max supported pageSize
        if (pageParams.getSize() > MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING) {
            throw new IllegalArgumentException("size should be less than [" + MAX_SUPPORTED_LIST_INDICES_PAGE_SIZE_STRING + "]");
        }
        // Next Token in the request will be validated by the IndexStrategyTokenParser itself.
        if (Objects.nonNull(pageParams.getRequestedToken())) {
            IndexPaginationStrategy.IndexStrategyToken.validateIndexStrategyToken(pageParams.getRequestedToken());
        }
        return pageParams;
    }

    protected int defaultPageSize() {
        return DEFAULT_LIST_INDICES_PAGE_SIZE_STRING;
    }

    @Override
    protected IndexPaginationStrategy getPaginationStrategy(ClusterStateResponse clusterStateResponse) {
        return new IndexPaginationStrategy(pageParams, clusterStateResponse.getState());
    }

    // Public for testing
    @Override
    public Iterator<Tuple<String, Settings>> getTableIterator(String[] indices, Map<String, Settings> indexSettingsMap) {
        return new Iterator<>() {
            int indexPos = 0;

            @Override
            public boolean hasNext() {
                while (indexPos < indices.length && indexSettingsMap.containsKey(indices[indexPos]) == false) {
                    indexPos++;
                }
                return indexPos < indices.length;
            }

            @Override
            public Tuple<String, Settings> next() {
                Tuple<String, Settings> tuple = new Tuple<>(indices[indexPos], indexSettingsMap.get(indices[indexPos]));
                indexPos++;
                return tuple;
            }
        };
    }
}
