/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.PublicApi;

/**
 * Abstraction that allows indication of whether results should be rescored or not based on
 * custom logic of exact {@link QueryCollectorContext} implementation.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.15.0")
public interface RescoringQueryCollectorContext {

    /**
     * Indicates if results from the query context should be rescored
     * @return true if results must be rescored, false otherwise
     */
    boolean shouldRescore();
}
