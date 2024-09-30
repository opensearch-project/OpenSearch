/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

/**
 * Pagination response metadata for a paginated query.
 * @opensearch.internal
 */
public class PageToken {

    public static final String PAGINATED_RESPONSE_NEXT_TOKEN_KEY = "next_token";

    /**
     * String denoting the next_token of paginated response, which will be used to fetch next page (if any).
     */
    private final String nextToken;

    /**
     * String denoting the element which is being paginated (for e.g. shards, indices..).
     */
    private final String paginatedEntity;

    public PageToken(String nextToken, String paginatedElement) {
        assert paginatedElement != null : "paginatedElement must be specified for a paginated response";
        this.nextToken = nextToken;
        this.paginatedEntity = paginatedElement;
    }

    public String getNextToken() {
        return nextToken;
    }

    public String getPaginatedEntity() {
        return paginatedEntity;
    }
}
