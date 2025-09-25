/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

/**
 * Experimental
 * @opensearch.internal
 */
public interface SearchResultsCollector<T> {

    /**
     * collect
     */
    void collect(T value);
}
