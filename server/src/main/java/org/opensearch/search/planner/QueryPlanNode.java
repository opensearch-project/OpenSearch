/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * Represents a node in a query execution plan tree.
 * Wraps a Lucene Query with additional metadata and cost information.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface QueryPlanNode {

    /**
     * Returns the Lucene query wrapped by this plan node.
     *
     * @return The Lucene query
     */
    Query getLuceneQuery();

    /**
     * Returns the estimated cost of executing this query node.
     * This combines Lucene's cost estimate with OpenSearch-specific heuristics.
     *
     * @return The estimated cost
     */
    QueryCost getEstimatedCost();

    /**
     * Returns the child nodes of this query plan node.
     * Leaf nodes will return an empty list.
     *
     * @return List of child query plan nodes
     */
    List<QueryPlanNode> getChildren();

    /**
     * Returns the type of this query plan node.
     * Used for optimization and execution strategy selection.
     *
     * @return The node type
     */
    QueryNodeType getType();

    /**
     * Returns a human-readable string representation of this node
     * for debugging and logging purposes.
     *
     * @return String representation of the node
     */
    String toString();

    /**
     * Returns a detailed breakdown of this node for profiling.
     * Includes cost breakdown and query structure.
     *
     * @return Profiling information
     */
    QueryPlanProfile getProfile();
}
