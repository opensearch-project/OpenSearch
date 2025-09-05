/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Experimental
 * @opensearch.internal
 */
// TODO : account for sub collectors
@ExperimentalApi
public interface SearchResultsCollector<T> {

    /**
     * collect
     */
    void collect(T value);
}
