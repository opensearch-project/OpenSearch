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
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Interface to be implemented by any strategy getting used for paginating rest responses.
 *
 * @opensearch.internal
 */
public interface PaginationStrategy<T> {

    String INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE =
        "Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token].";

    /**
     *
     * @return Base64 encoded string, which can be used to fetch next page of response.
     */
    PageToken getResponseToken();

    /**
     *
     * @return List of elements fetched corresponding to the store and token received by the strategy.
     */
    List<T> getRequestedEntities();

    /**
     *
     * Utility method to get list of indices filtered as per {@param filterPredicate} and the sorted according to {@param comparator}.
     */
    static List<IndexMetadata> getSortedIndexMetadata(
        final ClusterState clusterState,
        Predicate<IndexMetadata> filterPredicate,
        Comparator<IndexMetadata> comparator
    ) {
        return clusterState.metadata().indices().values().stream().filter(filterPredicate).sorted(comparator).collect(Collectors.toList());
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
