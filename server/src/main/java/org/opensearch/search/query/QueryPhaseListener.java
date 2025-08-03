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
 * Listener interface that allows plugins to hook into the query phase
 * before and after collection. This enables custom CollectorManager
 * implementations and data processing for advanced search features like
 * hybrid queries and neural search.
 *
 * <p>This API is experimental and may change in future versions based on
 * feedback from plugin implementations.</p>
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface QueryPhaseListener {

    /**
     * Called before collection begins in the query phase.
     * This allows extensions to set up custom state or modify the search context
     * before the main query execution.
     *
     * @param searchContext the current search context
     */
    void beforeCollection(SearchContext searchContext);

    /**
     * Called after collection completes in the query phase.
     * This allows extensions to process collected data or perform
     * post-collection operations.
     *
     * @param searchContext the current search context
     */
    void afterCollection(SearchContext searchContext);

}
