/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import java.util.List;

/**
 * Interface to be implemented by any strategy getting used for paginating rest responses.
 *
 * @opensearch.internal
 */
public interface PaginationStrategy<T> {

    /**
     *
     * @return Base64 encoded string, which can be used to fetch next page of response.
     */
    PageToken getNextToken();

    /**
     *
     * @return List of elements fetched corresponding to the store and token received by the strategy.
     */
    List<T> getElementsFromRequestedToken();
}
