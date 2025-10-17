/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Concrete implementations of query plan nodes for different query types.
 *
 * <p>This package contains specific node types for various Lucene/OpenSearch
 * queries, each with specialized cost calculation logic:
 *
 * <ul>
 *   <li>{@link org.opensearch.search.planner.nodes.TermPlanNode} - For term queries</li>
 *   <li>{@link org.opensearch.search.planner.nodes.BooleanPlanNode} - For boolean queries</li>
 *   <li>{@link org.opensearch.search.planner.nodes.RangePlanNode} - For range queries</li>
 *   <li>{@link org.opensearch.search.planner.nodes.MatchPlanNode} - For match queries</li>
 *   <li>{@link org.opensearch.search.planner.nodes.GenericPlanNode} - Fallback for other query types</li>
 * </ul>
 *
 * @opensearch.internal
 */
package org.opensearch.search.planner.nodes;
