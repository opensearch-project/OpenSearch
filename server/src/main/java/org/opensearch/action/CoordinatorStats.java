/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.action.search.SearchCoordinatorStats;

/**
 * Cordinator level stats
 *
 * @opensearch.internal
 */
public final class CoordinatorStats {

    public SearchCoordinatorStats searchCoordinatorStats;

    public CoordinatorStats() {
        searchCoordinatorStats = new SearchCoordinatorStats();
    }

    public SearchCoordinatorStats getSearchCoordinatorStats() {
        return searchCoordinatorStats;
    }

}
