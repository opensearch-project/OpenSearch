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

import java.util.ArrayList;
import java.util.List;

/**
 * Composes a {@link NodeSelector} with an {@link ExecutionTargetCreator} to resolve
 * execution targets for dynamic allocation stages (shuffle, broadcast).
 *
 * <p>Node selection and target creation are independent concerns — any
 * {@link NodeSelector} can be paired with any {@link ExecutionTargetCreator}.
 *
 * @opensearch.internal
 */
public class ComposableTargetResolver extends TargetResolver {

    private final NodeSelector nodeSelector;
    private final ExecutionTargetCreator targetCreator;

    public ComposableTargetResolver(NodeSelector nodeSelector, ExecutionTargetCreator targetCreator) {
        this.nodeSelector = nodeSelector;
        this.targetCreator = targetCreator;
    }

    @Override
    public List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest) {
        List<DiscoveryNode> nodes = nodeSelector.select(clusterState, childManifest);
        List<ExecutionTarget> targets = new ArrayList<>(nodes.size());
        for (DiscoveryNode node : nodes) {
            targets.add(targetCreator.create(node, childManifest));
        }
        return targets;
    }
}
