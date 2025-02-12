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
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;

/**
 * Helper methods for manipulating {@link SearchResponse}.
 */
public final class SearchResponseUtil {
    private SearchResponseUtil() {

    }

    /**
     * Construct a new {@link SearchResponse} based on an existing one, replacing just the {@link SearchHits}.
     * @param newHits new {@link SearchHits}
     * @param response the existing search response
     * @return a new search response where the {@link SearchHits} has been replaced
     */
    public static SearchResponse replaceHits(SearchHits newHits, SearchResponse response) {
        SearchResponseSections searchResponseSections;
        if (response.getAggregations() == null || response.getAggregations() instanceof InternalAggregations) {
            // We either have no aggregations, or we have Writeable InternalAggregations.
            // Either way, we can produce a Writeable InternalSearchResponse.
            searchResponseSections = new InternalSearchResponse(
                newHits,
                (InternalAggregations) response.getAggregations(),
                response.getSuggest(),
                new SearchProfileShardResults(response.getProfileResults()),
                response.isTimedOut(),
                response.isTerminatedEarly(),
                response.getNumReducePhases()
            );
        } else {
            // We have non-Writeable Aggregations, so the whole SearchResponseSections is non-Writeable.
            searchResponseSections = new SearchResponseSections(
                newHits,
                response.getAggregations(),
                response.getSuggest(),
                response.isTimedOut(),
                response.isTerminatedEarly(),
                new SearchProfileShardResults(response.getProfileResults()),
                response.getNumReducePhases()
            );
        }

        return new SearchResponse(
            searchResponseSections,
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

    /**
     * Convenience method when only replacing the {@link SearchHit} array within the {@link SearchHits} in a {@link SearchResponse}.
     * @param newHits the new array of {@link SearchHit} elements.
     * @param response the search response to update
     * @return a {@link SearchResponse} where the underlying array of {@link SearchHit} within the {@link SearchHits} has been replaced.
     */
    public static SearchResponse replaceHits(SearchHit[] newHits, SearchResponse response) {
        if (response.getHits() == null) {
            throw new IllegalStateException("Response must have hits");
        }
        SearchHits searchHits = new SearchHits(
            newHits,
            response.getHits().getTotalHits(),
            response.getHits().getMaxScore(),
            response.getHits().getSortFields(),
            response.getHits().getCollapseField(),
            response.getHits().getCollapseValues()
        );
        return replaceHits(searchHits, response);
    }
}
