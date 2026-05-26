/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * Selects the nodes a stage should execute on. Used by {@link ComposableTargetResolver}.
 *
 * <p>TODO: add ResourceAwareNodeSelector when cluster resource metrics are available.
 *
 * @opensearch.internal
 */
public interface NodeSelector {

    /**
     * @param clusterState  current cluster state
     * @param childManifest manifest from child stage (null for leaf stages).
     *                      TODO: replace Object with typed manifest once shuffle is implemented.
     */
    List<DiscoveryNode> select(ClusterState clusterState, @Nullable Object childManifest);
}
