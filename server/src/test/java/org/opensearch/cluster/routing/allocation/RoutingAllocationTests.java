/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Set;

public class RoutingAllocationTests extends OpenSearchTestCase {

    public void testCopyIgnoreNodesIsIndependent() {
        RoutingAllocation source = newAllocation();
        RoutingAllocation firstSimulation = newAllocation();
        RoutingAllocation secondSimulation = newAllocation();
        ShardId shard = new ShardId("index", "uuid", 0);
        ShardId otherShard = new ShardId("index", "uuid", 1);
        source.addIgnoreShardForNode(shard, "failed-node");
        source.addIgnoreShardForNode(shard, "other-failed-node");
        firstSimulation.copyIgnoreNodesFrom(source);
        secondSimulation.copyIgnoreNodesFrom(source);
        assertEquals(Set.of("failed-node", "other-failed-node"), firstSimulation.getIgnoreNodes(shard));
        assertEquals(source.getIgnoreNodes(shard), secondSimulation.getIgnoreNodes(shard));

        source.addIgnoreShardForNode(shard, "source-only");
        source.addIgnoreShardForNode(otherShard, "source-only");
        firstSimulation.addIgnoreShardForNode(shard, "simulation-only");
        firstSimulation.addIgnoreShardForNode(otherShard, "simulation-only");
        assertFalse(source.shouldIgnoreShardForNode(shard, "simulation-only"));
        assertFalse(source.shouldIgnoreShardForNode(otherShard, "simulation-only"));
        assertFalse(firstSimulation.shouldIgnoreShardForNode(shard, "source-only"));
        assertFalse(firstSimulation.shouldIgnoreShardForNode(otherShard, "source-only"));
        assertEquals(Set.of("failed-node", "other-failed-node"), secondSimulation.getIgnoreNodes(shard));
        assertTrue(secondSimulation.getIgnoreNodes(otherShard).isEmpty());
    }

    public void testCopyIgnoreNodesReplacesPreviousEntries() {
        RoutingAllocation source = newAllocation();
        RoutingAllocation target = newAllocation();
        ShardId shard = new ShardId("index", "uuid", 0);
        target.addIgnoreShardForNode(shard, "old-node");
        source.addIgnoreShardForNode(shard, "new-node");
        target.copyIgnoreNodesFrom(source);
        assertEquals(Set.of("new-node"), target.getIgnoreNodes(shard));

        target.copyIgnoreNodesFrom(newAllocation());
        assertTrue(target.getIgnoreNodes(shard).isEmpty());
        target.addIgnoreShardForNode(shard, "target-only");
        assertFalse(source.shouldIgnoreShardForNode(shard, "target-only"));
    }

    private RoutingAllocation newAllocation() {
        ClusterState state = ClusterState.EMPTY_STATE;
        return new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            null,
            System.nanoTime()
        );
    }
}
