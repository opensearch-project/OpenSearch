/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

/**
 * Describes where a stage's fragment executes.
 *
 * @opensearch.internal
 */
public enum StageExecutionType {
    /** Fragment runs on data nodes (today's default). */
    DATA_NODE,
    /** Fragment runs in-process on the node executing the walker, fed by child stage outputs. */
    LOCAL
}
