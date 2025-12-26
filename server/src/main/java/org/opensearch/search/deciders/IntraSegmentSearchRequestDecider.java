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
 * Decider for intra-segment search. Evaluates whether queries benefit from
 * intra-segment parallelization (partitioning large segments into doc ID ranges).
 */
@ExperimentalApi
public abstract class IntraSegmentSearchRequestDecider {

    /**
     * Evaluate if the query benefits from intra-segment search.
     * Called for each query node in the query tree.
     */
    public abstract void evaluateForQuery(QueryBuilder queryBuilder, IndexSettings indexSettings);

    /**
     * Returns the final decision after evaluating all query nodes.
     */
    public abstract IntraSegmentSearchDecision getIntraSegmentSearchDecision();

    /**
     * Factory to create IntraSegmentSearchRequestDecider instances per request.
     */
    @ExperimentalApi
    public interface Factory {
        default Optional<IntraSegmentSearchRequestDecider> create(IndexSettings indexSettings) {
            return Optional.empty();
        }
    }
}
