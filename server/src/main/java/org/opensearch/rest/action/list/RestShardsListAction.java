/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.pagination.PageParams;
import org.opensearch.rest.pagination.ShardPaginationStrategy;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _list API action to output shards in pages.
 *
 * @opensearch.api
 */
public class RestShardsListAction extends RestShardsAction {

    private static final int MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING = 20000;
    private static final int MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING = 2000;

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_list/shards"), new Route(GET, "/_list/shards/{index}")));
    }

    @Override
    public String getName() {
        return "list_shards_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_list/shards\n");
        sb.append("/_list/shards/{index}\n");
    }

    @Override
    public boolean isActionPaginated() {
        return true;
    }

    @Override
    protected PageParams validateAndGetPageParams(RestRequest restRequest) {
        PageParams pageParams = super.validateAndGetPageParams(restRequest);
        // validate max supported pageSize
        if (pageParams.getSize() < MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING) {
            throw new IllegalArgumentException("size should at least be [" + MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING + "]");
        } else if (pageParams.getSize() > MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING) {
            throw new IllegalArgumentException("size should be less than [" + MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING + "]");
        }
        // Next Token in the request will be validated by the ShardStrategyToken itself.
        if (Objects.nonNull(pageParams.getRequestedToken())) {
            ShardPaginationStrategy.ShardStrategyToken.validateShardStrategyToken(pageParams.getRequestedToken());
        }
        return pageParams;
    }

    protected int defaultPageSize() {
        return MIN_SUPPORTED_LIST_SHARDS_PAGE_SIZE_STRING;
    }
}
