/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.deciders;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;

import java.util.Optional;

/**
 * Default implementation that evaluates query support for intra-segment search
 * by delegating to {@link QueryBuilder#supportsIntraSegmentSearch()}.
 */
@ExperimentalApi
public class DefaultIntraSegmentSearchDecider extends IntraSegmentSearchRequestDecider {

    private boolean allSupport = true;
    private String reason = "no query evaluated";

    @Override
    public void evaluateForQuery(QueryBuilder queryBuilder, IndexSettings indexSettings) {
        boolean supports = queryBuilder.supportsIntraSegmentSearch();

        if (!supports) {
            allSupport = false;
            reason = queryBuilder.getName() + " does not support intra-segment search";
        }
    }

    @Override
    public IntraSegmentSearchDecision getIntraSegmentSearchDecision() {
        if (allSupport) {
            return new IntraSegmentSearchDecision(
                IntraSegmentSearchDecision.DecisionStatus.YES,
                "all queries support intra-segment search"
            );
        }
        return new IntraSegmentSearchDecision(IntraSegmentSearchDecision.DecisionStatus.NO, reason);
    }

    /**
     * Factory for creating DefaultIntraSegmentSearchDecider instances.
     */
    @ExperimentalApi
    public static class Factory implements IntraSegmentSearchRequestDecider.Factory {
        @Override
        public Optional<IntraSegmentSearchRequestDecider> create(IndexSettings indexSettings) {
            return Optional.of(new DefaultIntraSegmentSearchDecider());
        }
    }
}
