/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common.helpers;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.search.SearchHits;
import org.opensearch.search.profile.SearchProfileShardResults;

/**
 * Helper methods for manipulating {@link SearchResponse}.
 */
public final class SearchResponseUtil {
    private SearchResponseUtil() {

    }

    /**
     * Construct a new {@link SearchResponse} based on an existing one, replacing just the {@link SearchHits}.
     * @param newHits new search hits
     * @param response the existing search response
     * @return a new search response where the search hits have been replaced
     */
    public static SearchResponse replaceHits(SearchHits newHits, SearchResponse response) {
        return new SearchResponse(
            new SearchResponseSections(
                newHits,
                response.getAggregations(),
                response.getSuggest(),
                response.isTimedOut(),
                response.isTerminatedEarly(),
                new SearchProfileShardResults(response.getProfileResults()),
                response.getNumReducePhases()
            ),
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }
}
