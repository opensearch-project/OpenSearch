/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.common.annotation.PublicApi;

/**
 *
 * Class specific to paginated queries, which will contain common query params required by a paginated API.
 */
@PublicApi(since = "3.0.0")
public class PaginatedQueryRequest {

    public static final String PAGINATED_QUERY_PARAM_SORT_KEY = "sort";
    public static final String PAGINATED_QUERY_PARAM_NEXT_TOKEN_KEY = "next_token";
    public static final String PAGINATED_QUERY_PARAM_SIZE_KEY = "size";
    public static final String PAGINATED_QUERY_ASCENDING_SORT = "ascending";

    private final String requestedTokenStr;
    private final String sort;
    private final int size;

    public PaginatedQueryRequest(String requestedToken, String sort, int size) {
        this.requestedTokenStr = requestedToken;
        this.sort = sort;
        this.size = size;
    }

    public String getSort() {
        return sort;
    }

    public String getRequestedTokenStr() {
        return requestedTokenStr;
    }

    public int getSize() {
        return size;
    }

}
