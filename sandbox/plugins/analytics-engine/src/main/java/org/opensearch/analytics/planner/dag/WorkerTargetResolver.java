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
 * Hash-shuffle worker {@link TargetResolver}. Returns one {@link WorkerExecutionTarget} per
 * partition: target {@code p} routes to {@code targetWorkerNodeIds.get(p)} and carries
 * {@code partitionIndex == p}. The list of node ids is supplied at construction time by
 * {@code UnifiedDispatch}/{@code ShuffleEnrichment} so producers and consumer agree on which partition
 * lives where (the same list is also stamped on every {@code ShuffleProducerInstructionNode}).
 *
 * @opensearch.internal
 */
public final class WorkerTargetResolver extends TargetResolver {

    private final List<String> targetWorkerNodeIds;

    public WorkerTargetResolver(List<String> targetWorkerNodeIds) {
        this.targetWorkerNodeIds = List.copyOf(targetWorkerNodeIds);
    }

    @Override
    public List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest) {
        List<ExecutionTarget> targets = new ArrayList<>(targetWorkerNodeIds.size());
        for (int p = 0; p < targetWorkerNodeIds.size(); p++) {
            String nodeId = targetWorkerNodeIds.get(p);
            DiscoveryNode node = clusterState.nodes().get(nodeId);
            if (node == null) {
                throw new IllegalStateException(
                    "WorkerTargetResolver: worker node id '" + nodeId + "' (partition " + p + ") not present in cluster state"
                );
            }
            targets.add(new WorkerExecutionTarget(node, p));
        }
        return targets;
    }
}
