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
 * {@link ConcurrentSearchRequestDecider} allows pluggable way to evaluate if a query in the search request
 * can use concurrent segment search using the passed in queryBuilders from query tree and index settings
 * on a per shard request basis.
 * Implementations will need to implement the Factory interface that can be used to create the ConcurrentSearchRequestDecider
 * This factory will be called on each shard search request to create the ConcurrentSearchRequestDecider and get the
 * concurrent search decision from the created decider on a per-request basis.
 * For all the deciders the evaluateForQuery method will be called for each node in the query tree.
 * After traversing of the query tree is completed, the final decision from the deciders will be
 * obtained using {@link ConcurrentSearchRequestDecider#getConcurrentSearchDecision}
 */
@ExperimentalApi
public abstract class ConcurrentSearchRequestDecider {

    /**
     * Evaluate for the passed in queryBuilder node in the query tree of the search request
     * if concurrent segment search can be used.
     * This method will be called for each of the query builder node in the query tree of the request.
     */
    public abstract void evaluateForQuery(QueryBuilder queryBuilder, IndexSettings indexSettings);

    /**
     * Provide the final decision for concurrent search based on all evaluations
     * Plugins may need to maintain internal state of evaluations to provide a final decision
     * If decision is null, then it is ignored
     * @return ConcurrentSearchDecision
     */
    public abstract ConcurrentSearchDecision getConcurrentSearchDecision();

    /**
     * Factory interface that can be implemented to create the ConcurrentSearchRequestDecider object.
     * Implementations can use the passed in indexSettings to decide whether to create the decider object or
     * return {@link Optional#empty()}.
     */
    @ExperimentalApi
    public interface Factory {
        default Optional<ConcurrentSearchRequestDecider> create(IndexSettings indexSettings) {
            return Optional.empty();
        }
    }

}
