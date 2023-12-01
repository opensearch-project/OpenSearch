/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision.Type;
import org.opensearch.common.annotation.PublicApi;

/**
 * {@link AllocationDecider} is an abstract base class that allows to make
 * dynamic cluster- or index-wide shard allocation decisions on a per-node
 * basis.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class AllocationDecider {
    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced to the given allocation. The default is
     * {@link Decision#ALWAYS}.
     */
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be remain
     * on the given node. The default is {@link Decision#ALWAYS}.
     */
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be allocated at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be allocated at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether shards of the given index should be auto-expanded to this node at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the cluster can execute
     * re-balanced operations at all.
     * {@link Decision#ALWAYS}.
     */
    public Decision canRebalance(RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given primary shard can be
     * forcibly allocated on the given node. This method should only be called
     * for unassigned primary shards where the node has a shard copy on disk.
     * <p>
     * Note: all implementations that override this behavior should take into account
     * the results of {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)}
     * before making a decision on force allocation, because force allocation should only
     * be considered if all deciders return {@link Decision#NO}.
     */
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        assert shardRouting.unassigned() : "must not call canForceAllocatePrimary on an assigned shard " + shardRouting;
        Decision decision = canAllocate(shardRouting, node, allocation);
        if (decision.type() == Type.NO) {
            // On a NO decision, by default, we allow force allocating the primary.
            return allocation.decision(
                Decision.YES,
                decision.label(),
                "primary shard [%s] allowed to force allocate on node [%s]",
                shardRouting.shardId(),
                node.nodeId()
            );
        } else {
            // On a THROTTLE/YES decision, we use the same decision instead of forcing allocation
            return decision;
        }
    }

    /**
     * Returns a {@link Decision} whether the given shard can be moved away from the current node
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canMoveAway(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether any shard in the cluster can be moved away from the current node
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canMoveAnyShard(RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether any shard on the given
     * {@link RoutingNode}} can be allocated The default is {@link Decision#ALWAYS}.
     * All implementations that override this behaviour must take a
     * {@link Decision}} whether or not to skip iterating over the remaining
     * deciders for this node.
     */
    public Decision canAllocateAnyShardToNode(RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

}
