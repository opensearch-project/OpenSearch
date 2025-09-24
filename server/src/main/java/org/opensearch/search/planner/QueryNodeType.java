/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Types of query plan nodes, used for optimization and execution strategies.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum QueryNodeType {
    /**
     * Boolean query with must/should/filter/must_not clauses
     */
    BOOLEAN,

    /**
     * Term query - exact match on a field
     */
    TERM,

    /**
     * Terms query - matches any of multiple values
     */
    TERMS,

    /**
     * Range query - numeric or date range
     */
    RANGE,

    /**
     * Match query - analyzed text search
     */
    MATCH,

    /**
     * Match all query
     */
    MATCH_ALL,

    /**
     * Wildcard query
     */
    WILDCARD,

    /**
     * Prefix query
     */
    PREFIX,

    /**
     * Fuzzy query
     */
    FUZZY,

    /**
     * Regexp query
     */
    REGEXP,

    /**
     * Script query
     */
    SCRIPT,

    /**
     * Nested query
     */
    NESTED,

    /**
     * Function score query
     */
    FUNCTION_SCORE,

    /**
     * Constant score query
     */
    CONSTANT_SCORE,

    /**
     * Disjunction max query
     */
    DIS_MAX,

    /**
     * Multi-match query
     */
    MULTI_MATCH,

    /**
     * Query string query
     */
    QUERY_STRING,

    /**
     * Simple query string
     */
    SIMPLE_QUERY_STRING,

    /**
     * Exists query
     */
    EXISTS,

    /**
     * Geo queries
     */
    GEO,

    /**
     * Vector/KNN queries
     */
    VECTOR,

    /**
     * Unknown or custom query type
     */
    OTHER;

    /**
     * Returns true if this query type typically has high computational cost
     */
    public boolean isComputationallyExpensive() {
        switch (this) {
            case SCRIPT:
            case WILDCARD:
            case REGEXP:
            case FUZZY:
            case FUNCTION_SCORE:
            case VECTOR:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns true if this query type benefits from being executed early
     */
    public boolean preferEarlyExecution() {
        switch (this) {
            case TERM:
            case TERMS:
            case EXISTS:
                return true;
            default:
                return false;
        }
    }
}
