/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Query planning and cost estimation framework for OpenSearch.
 *
 * <p>This package provides infrastructure for building logical query plans
 * from OpenSearch queries and estimating their execution costs. The main
 * components include:
 *
 * <ul>
 *   <li>{@link org.opensearch.search.planner.QueryPlanNode} - Base interface for query plan nodes</li>
 *   <li>{@link org.opensearch.search.planner.QueryCost} - Multi-dimensional cost representation</li>
 *   <li>{@link org.opensearch.search.planner.LogicalPlanBuilder} - Builds plans from QueryBuilder</li>
 *   <li>{@link org.opensearch.search.planner.CostEstimator} - Estimates query execution costs</li>
 *   <li>{@link org.opensearch.search.planner.QueryPlanVisualizer} - Visualizes plans for debugging</li>
 * </ul>
 *
 * @opensearch.experimental
 */
package org.opensearch.search.planner;
