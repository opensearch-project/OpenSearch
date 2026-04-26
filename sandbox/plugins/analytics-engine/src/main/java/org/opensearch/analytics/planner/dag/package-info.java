/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * DAG construction, plan forking, and fragment conversion for the Analytics Plugin planner.
 * Converts a marked RelNode tree into a {@link org.opensearch.analytics.planner.dag.QueryDAG}
 * with per-stage plan alternatives, then strips annotations and dispatches to backend
 * {@link org.opensearch.analytics.spi.FragmentConvertor} implementations.
 */
package org.opensearch.analytics.planner.dag;
