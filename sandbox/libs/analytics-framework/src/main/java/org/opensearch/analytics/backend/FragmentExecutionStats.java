/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

/**
 * Execution stats for a completed fragment, carried alongside the slow log
 * callback to provide insight into the work performed.
 *
 * @param rowsProduced             rows returned by this fragment
 * @param usedSecondaryIndex       whether a secondary/inverted index was consulted for filtering
 * @param delegatedPredicateCount  number of predicates delegated to the secondary index (0 if none)
 * @param filterTreeShape          boolean tree shape for delegation (null if no delegation)
 * @param hasPartialAggregate      whether this fragment performs partial aggregation
 * @param taskId                   the task ID for correlation with _tasks API
 * @param opaqueId                 the X-Opaque-Id header for correlation with the client request (nullable)
 *
 * @opensearch.internal
 */
public record FragmentExecutionStats(long rowsProduced, boolean usedSecondaryIndex, int delegatedPredicateCount, String filterTreeShape,
    boolean hasPartialAggregate, long taskId, String opaqueId) {

    public static final FragmentExecutionStats EMPTY = new FragmentExecutionStats(0, false, 0, null, false, -1, null);
}
