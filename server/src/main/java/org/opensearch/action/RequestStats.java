/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.action.search.SearchRequestStats;

/**
 * Request level stats
 *
 * @opensearch.internal
 */
public final class RequestStats {

    public SearchRequestStats searchRequestStats;

    public RequestStats() {
        searchRequestStats = new SearchRequestStats();
    }

    public SearchRequestStats getSearchRequestStats() {
        return searchRequestStats;
    }

}
