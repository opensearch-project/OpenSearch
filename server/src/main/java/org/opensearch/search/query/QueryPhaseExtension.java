/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.internal.SearchContext;

/**
 * Extension point interface that allows plugins to hook into the query phase
 * before and after score collection. This enables custom CollectorManager
 * implementations and score processing for advanced search features like
 * hybrid queries and neural search.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.2.0")
public interface QueryPhaseExtension {

    /**
     * Called before score collection begins in the query phase.
     * This allows extensions to set up custom state or modify the search context
     * before the main query execution.
     *
     * @param searchContext the current search context
     */
    void beforeScoreCollection(SearchContext searchContext);

    /**
     * Called after score collection completes in the query phase.
     * This allows extensions to process collected scores or perform
     * post-collection operations.
     *
     * @param searchContext the current search context
     */
    void afterScoreCollection(SearchContext searchContext);

    /**
     * Determines whether failures in this extension should fail the entire query.
     * When true, exceptions thrown by this extension will propagate and fail the search.
     * When false (default), exceptions are logged and the search continues.
     *
     * @return true if extension failures should fail the query, false otherwise
     */
    default boolean failOnError() {
        return false;
    }
}
