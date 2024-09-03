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

/**
 * {@link ConcurrentSearchDecider} allows pluggable way to evaluate if a query in the search request
 * can use concurrent segment search using the passed in queryBuilders from query tree and index settings
 * on a per shard request basis.
 * Implementations can also opt out of the evaluation process for certain indices based on the index settings.
 * For all the deciders which can evaluate query tree for an index, its evaluateForQuery method
 * will be called for each node in the query tree. After traversing of the query tree is completed, the final
 * decision from the deciders will be obtained using {@link ConcurrentSearchDecider#getConcurrentSearchDecision}
 */
@ExperimentalApi
public abstract class ConcurrentSearchDecider {

    /**
     * Evaluate for the passed in queryBuilder node in the query tree of the search request
     * if concurrent segment search can be used.
     * This method will be called for each of the query builder node in the query tree of the request.
     */
    public abstract void evaluateForQuery(QueryBuilder queryBuilder, IndexSettings indexSettings);

    /**
     * Provides a way for deciders to opt out of decision-making process for certain requests based on
     * index settings.
     * Return true if interested in decision making for index,
     * false, otherwise
     */
    public abstract boolean canEvaluateForIndex(IndexSettings indexSettings);

    /**
     * Provide the final decision for concurrent search based on all evaluations
     * Plugins may need to maintain internal state of evaluations to provide a final decision
     * If decision is null, then it is ignored
     * @return ConcurrentSearchDecision
     */
    public abstract ConcurrentSearchDecision getConcurrentSearchDecision();

}
