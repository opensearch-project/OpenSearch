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
import org.opensearch.search.aggregations.AggregatorFactories;

import java.util.Optional;

/**
 * Default implementation that evaluates query and aggregation support for intra-segment search.
 */
@ExperimentalApi
public class DefaultIntraSegmentSearchDecider extends IntraSegmentSearchRequestDecider {

    private boolean querySupport = true;
    private boolean aggSupport = true;
    private String queryReason = "no query evaluated";
    private String aggReason = "no aggregations evaluated";
    private boolean hasQuery = false;
    private boolean hasAggregations = false;

    @Override
    public void evaluateForQuery(QueryBuilder queryBuilder, IndexSettings indexSettings) {
        hasQuery = true;
        boolean supports = queryBuilder.supportsIntraSegmentSearch();
        if (!supports) {
            querySupport = false;
            queryReason = queryBuilder.getName() + " does not support intra-segment search";
        }
    }

    @Override
    public void evaluateForAggregations(AggregatorFactories aggregations, IndexSettings indexSettings) {
        if (aggregations == null) {
            return;
        }
        hasAggregations = true;
        boolean supports = aggregations.allFactoriesSupportIntraSegmentSearch();
        if (supports) {
            aggReason = "all aggregations support intra-segment search";
        } else {
            aggSupport = false;
            aggReason = "some aggregations do not support intra-segment search";
        }
    }

    @Override
    public IntraSegmentSearchDecision getIntraSegmentSearchDecision() {
        if (hasQuery && !querySupport) {
            return new IntraSegmentSearchDecision(IntraSegmentSearchDecision.DecisionStatus.NO, queryReason);
        }
        if (hasAggregations && !aggSupport) {
            return new IntraSegmentSearchDecision(IntraSegmentSearchDecision.DecisionStatus.NO, aggReason);
        }
        if (hasAggregations && aggSupport) {
            return new IntraSegmentSearchDecision(IntraSegmentSearchDecision.DecisionStatus.YES, aggReason);
        }
        if (hasQuery && querySupport) {
            return new IntraSegmentSearchDecision(
                IntraSegmentSearchDecision.DecisionStatus.YES,
                "all queries support intra-segment search"
            );
        }
        return new IntraSegmentSearchDecision(IntraSegmentSearchDecision.DecisionStatus.NO_OP, "no preference");
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
