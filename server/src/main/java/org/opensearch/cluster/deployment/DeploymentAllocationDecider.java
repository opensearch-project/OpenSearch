/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.Decision;

/**
 * Prevents primary shards from being allocated to or remaining on nodes that are in a deployment state.
 *
 * @opensearch.internal
 */
public class DeploymentAllocationDecider extends AllocationDecider {

    public static final String NAME = "deployment";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary() == false) {
            return Decision.ALWAYS;
        }
        DeploymentState deploymentState = DeploymentStateService.getNodeDeploymentState(allocation.metadata(), node.node());
        if (deploymentState == null) {
            return Decision.ALWAYS;
        }
        return allocation.decision(Decision.NO, NAME, "cannot allocate primary shard to node with deployment state [%s]", deploymentState);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }
}
