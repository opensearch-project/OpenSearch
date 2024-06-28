/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.internal.SearchContext;

/**
 * Interface to define different stages of aggregation processing before and after document collection
 *
 * @opensearch.api
 */
@PublicApi(since = "2.9.0")
public interface AggregationProcessor {

    /**
     * Callback invoked before collection of documents are done
     * @param context {@link SearchContext} for the request
     */
    void preProcess(SearchContext context);

    /**
     * Callback invoked after collection of documents are done
     * @param context {@link SearchContext} for the request
     */
    void postProcess(SearchContext context);
}
