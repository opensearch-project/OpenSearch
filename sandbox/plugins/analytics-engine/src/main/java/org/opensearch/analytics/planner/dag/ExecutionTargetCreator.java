/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;

/**
 * Creates an {@link ExecutionTarget} for a specific node. Used by {@link ComposableTargetResolver}.
 *
 * <p>TODO: add ShuffleStreamExecutionTargetCreator, InMemoryExecutionTargetCreator
 * when shuffle aggregates, joins, and broadcast joins are implemented.
 *
 * @opensearch.internal
 */
public interface ExecutionTargetCreator {

    /**
     * @param node          the node selected by {@link NodeSelector}
     * @param childManifest manifest from child stage (null for leaf stages).
     *                      TODO: replace Object with typed manifest once shuffle is implemented.
     */
    ExecutionTarget create(DiscoveryNode node, @Nullable Object childManifest);
}
