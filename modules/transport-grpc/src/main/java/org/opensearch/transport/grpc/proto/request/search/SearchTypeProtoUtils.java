/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.search.SearchType;
import org.opensearch.protobufs.SearchRequest;

/**
 * Utility class for converting SearchType enums between OpenSearch and Protocol Buffers formats.
 * This class provides methods to transform search type values to ensure proper execution
 * of search operations with the correct search strategy.
 */
public class SearchTypeProtoUtils {

    private SearchTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer SearchRequest to a SearchType.
     *
     * Similar to {@link SearchType#fromString(String)}
     * Please ensure to keep both implementations consistent.
     *
     * @param request the Protocol Buffer SearchRequest to convert
     * @return the corresponding SearchType
     */
    protected static SearchType fromProto(SearchRequest request) {
        if (!request.hasSearchType()) {
            return SearchType.DEFAULT;
        }
        org.opensearch.protobufs.SearchType searchType = request.getSearchType();
        switch (searchType) {
            case SEARCH_TYPE_DFS_QUERY_THEN_FETCH:
                return SearchType.DFS_QUERY_THEN_FETCH;
            case SEARCH_TYPE_QUERY_THEN_FETCH:
                return SearchType.QUERY_THEN_FETCH;
            default:
                throw new IllegalArgumentException("No search type for [" + searchType + "]");
        }
    }
}
