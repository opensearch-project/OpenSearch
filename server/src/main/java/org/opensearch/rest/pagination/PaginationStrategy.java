/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Interface to be implemented by any strategy getting used for paginating rest responses.
 *
 * @opensearch.internal
 */
public interface PaginationStrategy<T> {

    String DESCENDING_SORT_PARAM_VALUE = "descending";
    String INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE =
        "Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token].";

    /**
     *
     * @return Base64 encoded string, which can be used to fetch next page of response.
     */
    PaginatedQueryResponse getPaginatedQueryResponse();

    /**
     *
     * @return List of elements fetched corresponding to the store and token received by the strategy.
     */
    List<T> getElementsFromRequestedToken();

    /**
     *
     * Utility method to get list of indices sorted by their creation time with {@param latestValidIndexCreateTime}
     * being used to filter out the indices created after it.
     */
    static List<String> getListOfIndicesSortedByCreateTime(
        final ClusterState clusterState,
        String sortOrder,
        final long latestValidIndexCreateTime
    ) {
        // Filter out the indices which have been created after the latest index which was present when
        // paginated query started. Also, sort the indices list based on their creation timestamps
        return clusterState.getRoutingTable()
            .getIndicesRouting()
            .keySet()
            .stream()
            .filter(index -> (latestValidIndexCreateTime >= clusterState.metadata().indices().get(index).getCreationDate()))
            .sorted((index1, index2) -> {
                Long index1CreationTimeStamp = clusterState.metadata().indices().get(index1).getCreationDate();
                Long index2CreationTimeStamp = clusterState.metadata().indices().get(index2).getCreationDate();
                if (index1CreationTimeStamp.equals(index2CreationTimeStamp)) {
                    return DESCENDING_SORT_PARAM_VALUE.equals(sortOrder) ? index2.compareTo(index1) : index1.compareTo(index2);
                }
                return DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)
                    ? Long.compare(index2CreationTimeStamp, index1CreationTimeStamp)
                    : Long.compare(index1CreationTimeStamp, index2CreationTimeStamp);
            })
            .collect(Collectors.toList());
    }

    static String encryptStringToken(String tokenString) {
        if (Objects.isNull(tokenString)) {
            return null;
        }
        return Base64.getEncoder().encodeToString(tokenString.getBytes(UTF_8));
    }

    static String decryptStringToken(String encTokenString) {
        if (Objects.isNull(encTokenString)) {
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(encTokenString), UTF_8);
        } catch (IllegalArgumentException exception) {
            throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
        }
    }
}
