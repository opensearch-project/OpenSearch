/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.SearchContext;

/**
 * Listener interface for search query phase operations.
 * Allows plugins to hook into the query phase before and after document collection.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface QueryPhaseListener {
    /**
     * Called before document collection begins.
     * This can be used to set up any state needed during collection.
     *
     * @param searchContext the search context for the current search request
     */
    void beforeCollection(SearchContext searchContext);

    /**
     * Called after document collection completes.
     * This can be used to perform cleanup or post-processing.
     *
     * @param searchContext the search context for the current search request
     */
    void afterCollection(SearchContext searchContext);
}
