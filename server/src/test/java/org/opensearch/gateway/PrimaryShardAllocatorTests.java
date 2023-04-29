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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.gateway;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotShardSizeInfo;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.routing.UnassignedInfo.Reason.CLUSTER_RECOVERED;
import static org.opensearch.cluster.routing.UnassignedInfo.Reason.INDEX_CREATED;
import static org.opensearch.cluster.routing.UnassignedInfo.Reason.INDEX_REOPENED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PrimaryShardAllocatorTests extends OpenSearchAllocationTestCase {

    private final ShardId shardId = new ShardId("test", "_na_", 0);
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");
    private TestAllocator testAllocator;

    @Before
    public void buildTestAllocator() {
        this.testAllocator = new TestAllocator();
    }

    private void allocateAllUnassigned(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            testAllocator.allocateUnassigned(iterator.next(), allocation, iterator);
        }
    }

    public void testNoProcessPrimaryNotAllocatedBefore() {
        final RoutingAllocation allocation;
        // with old version, we can't know if a shard was allocated before or not
        allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            randomFrom(INDEX_CREATED, CLUSTER_RECOVERED, INDEX_REOPENED)
        );
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().iterator().next().shardId(), equalTo(shardId));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when async fetch returns that there is no data, the shard will not be allocated.
     */
    public void testNoAsyncFetchData() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            "allocId"
        );
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests when the node returns that no data was found for it (null for allocation id),
     * it will be moved to ignore unassigned.
     */
    public void testNoAllocationFound() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            "allocId"
        );
        testAllocator.addData(node1, null, randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests when the node returns data with a shard allocation id that does not match active allocation ids, it will be moved to ignore
     * unassigned.
     */
    public void testNoMatchingAllocationIdFound() {
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(yesAllocationDeciders(), CLUSTER_RECOVERED, "id2");
        testAllocator.addData(node1, "id1", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests when the node returns that no data was found for it, it will be moved to ignore unassigned.
     */
    public void testStoreException() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            "allocId1"
        );
        testAllocator.addData(node1, "allocId1", randomBoolean(), new CorruptIndexException("test", "test"));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when the node returns a ShardLockObtainFailedException, it will be considered as a valid shard copy
     */
    public void testShardLockObtainFailedException() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            "allocId1"
        );
        testAllocator.addData(node1, "allocId1", randomBoolean(), new ShardLockObtainFailedException(shardId, "test"));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node1.getId())
        );
        // check that allocation id is reused
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo("allocId1")
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that replica with the highest primary term version will be selected as target
     */
    public void testPreferReplicaWithHighestPrimaryTerm() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        String allocId3 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId2,
            allocId3
        );
        testAllocator.addData(node1, allocId1, false, new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()));
        testAllocator.addData(node2, allocId2, false, new ReplicationCheckpoint(shardId, 22, 120, 2, Codec.getDefault().getName()));
        testAllocator.addData(node3, allocId3, false, new ReplicationCheckpoint(shardId, 20, 120, 2, Codec.getDefault().getName()));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node2.getId())
        );
        // Assert node2's allocation id is used
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId2)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that replica with highest primary ter version will be selected as target
     */
    public void testPreferReplicaWithNullReplicationCheckpoint() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        String allocId3 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId2,
            allocId3
        );
        testAllocator.addData(node1, allocId1, false, new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()));
        testAllocator.addData(node2, allocId2, false);
        testAllocator.addData(node3, allocId3, false, new ReplicationCheckpoint(shardId, 40, 120, 2, Codec.getDefault().getName()));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
        // Assert node3's allocation id should be used as it has highest replication checkpoint
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId3)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that null ReplicationCheckpoint are ignored
     */
    public void testPreferReplicaWithAllNullReplicationCheckpoint() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        String allocId3 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId2,
            allocId3
        );
        testAllocator.addData(node1, allocId1, false, null, null);
        testAllocator.addData(node2, allocId2, false, null, null);
        testAllocator.addData(node3, allocId3, true, null, null);
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
        // Assert node3's allocation id should be used as it was previous primary
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId3)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that replica with highest segment info version will be selected as target on equal primary terms
     */
    public void testPreferReplicaWithHighestSegmentInfoVersion() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        String allocId3 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId2,
            allocId3
        );
        testAllocator.addData(node1, allocId1, false, new ReplicationCheckpoint(shardId, 10, 101, 1, Codec.getDefault().getName()));
        testAllocator.addData(node2, allocId2, false, new ReplicationCheckpoint(shardId, 20, 120, 3, Codec.getDefault().getName()));
        testAllocator.addData(node3, allocId3, false, new ReplicationCheckpoint(shardId, 20, 120, 2, Codec.getDefault().getName()));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node2.getId())
        );
        // Assert node2's allocation id is used
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId2)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that prefer allocation of replica at lower checkpoint but in sync set
     */
    public void testOutOfSyncHighestRepCheckpointIsIgnored() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        String allocId3 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId3
        );
        testAllocator.addData(node1, allocId1, false, new ReplicationCheckpoint(shardId, 10, 101, 1, Codec.getDefault().getName()));
        testAllocator.addData(node2, allocId2, false, new ReplicationCheckpoint(shardId, 20, 120, 2, Codec.getDefault().getName()));
        testAllocator.addData(node3, allocId3, false, new ReplicationCheckpoint(shardId, 15, 120, 2, Codec.getDefault().getName()));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
        // Assert node3's allocation id is used
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId3)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that prefer allocation of older primary over replica with higher replication checkpoint
     */
    public void testPreferAllocatingPreviousPrimaryWithLowerRepCheckpoint() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        String allocId3 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId2,
            allocId3
        );
        testAllocator.addData(node1, allocId1, true, new ReplicationCheckpoint(shardId, 10, 101, 1, Codec.getDefault().getName()));
        testAllocator.addData(node2, allocId2, false, new ReplicationCheckpoint(shardId, 20, 120, 2, Codec.getDefault().getName()));
        testAllocator.addData(node3, allocId3, false, new ReplicationCheckpoint(shardId, 15, 120, 2, Codec.getDefault().getName()));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node1.getId())
        );
        // Assert node1's allocation id is used with highest replication checkpoint
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId1)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when one node returns a ShardLockObtainFailedException and another properly loads the store, it will
     * select the second node as target
     */
    public void testShardLockObtainFailedExceptionPreferOtherValidCopies() {
        String allocId1 = randomAlphaOfLength(10);
        String allocId2 = randomAlphaOfLength(10);
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            CLUSTER_RECOVERED,
            allocId1,
            allocId2
        );
        testAllocator.addData(node1, allocId1, randomBoolean(), new ShardLockObtainFailedException(shardId, "test"));
        testAllocator.addData(node2, allocId2, randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node2.getId())
        );
        // check that allocation id is reused
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo(allocId2)
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when there is a node to allocate the shard to, it will be allocated to it.
     */
    public void testFoundAllocationAndAllocating() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            randomFrom(CLUSTER_RECOVERED, INDEX_REOPENED),
            "allocId1"
        );
        testAllocator.addData(node1, "allocId1", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node1.getId())
        );
        // check that allocation id is reused
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).allocationId().getId(),
            equalTo("allocId1")
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when the nodes with prior copies of the given shard all return a decision of NO, but
     * {@link AllocationDecider#canForceAllocatePrimary(ShardRouting, RoutingNode, RoutingAllocation)}
     * returns a YES decision for at least one of those NO nodes, then we force allocate to one of them
     */
    public void testForceAllocatePrimary() {
        testAllocator.addData(node1, "allocId1", randomBoolean());
        AllocationDeciders deciders = new AllocationDeciders(
            Arrays.asList(
                // since the deciders return a NO decision for allocating a shard (due to the guaranteed NO decision from the second
                // decider),
                // the allocator will see if it can force assign the primary, where the decision will be YES
                new TestAllocateDecision(randomBoolean() ? Decision.YES : Decision.NO),
                getNoDeciderThatAllowsForceAllocate()
            )
        );
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(deciders, CLUSTER_RECOVERED, "allocId1");
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertTrue(allocation.routingNodes().unassigned().ignored().isEmpty());
        assertEquals(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), 1);
        assertEquals(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), node1.getId());
    }

    /**
     * Tests that when the nodes with prior copies of the given shard all return a decision of NO, and
     * {@link AllocationDecider#canForceAllocatePrimary(ShardRouting, RoutingNode, RoutingAllocation)}
     * returns a NO or THROTTLE decision for a node, then we do not force allocate to that node.
     */
    public void testDontAllocateOnNoOrThrottleForceAllocationDecision() {
        testAllocator.addData(node1, "allocId1", randomBoolean());
        boolean forceDecisionNo = randomBoolean();
        AllocationDeciders deciders = new AllocationDeciders(
            Arrays.asList(
                // since both deciders here return a NO decision for allocating a shard,
                // the allocator will see if it can force assign the primary, where the decision will be either NO or THROTTLE,
                // so the shard will remain un-initialized
                new TestAllocateDecision(Decision.NO),
                forceDecisionNo ? getNoDeciderThatDeniesForceAllocate() : getNoDeciderThatThrottlesForceAllocate()
            )
        );
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(deciders, CLUSTER_RECOVERED, "allocId1");
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        List<ShardRouting> ignored = allocation.routingNodes().unassigned().ignored();
        assertEquals(ignored.size(), 1);
        assertEquals(
            ignored.get(0).unassignedInfo().getLastAllocationStatus(),
            forceDecisionNo ? AllocationStatus.DECIDERS_NO : AllocationStatus.DECIDERS_THROTTLED
        );
        assertTrue(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty());
    }

    /**
     * Tests that when the nodes with prior copies of the given shard return a THROTTLE decision,
     * then we do not force allocate to that node but instead throttle.
     */
    public void testDontForceAllocateOnThrottleDecision() {
        testAllocator.addData(node1, "allocId1", randomBoolean());
        AllocationDeciders deciders = new AllocationDeciders(
            Arrays.asList(
                // since we have a NO decision for allocating a shard (because the second decider returns a NO decision),
                // the allocator will see if it can force assign the primary, and in this case,
                // the TestAllocateDecision's decision for force allocating is to THROTTLE (using
                // the default behavior) so despite the other decider's decision to return YES for
                // force allocating the shard, we still THROTTLE due to the decision from TestAllocateDecision
                new TestAllocateDecision(Decision.THROTTLE),
                getNoDeciderThatAllowsForceAllocate()
            )
        );
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(deciders, CLUSTER_RECOVERED, "allocId1");
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        List<ShardRouting> ignored = allocation.routingNodes().unassigned().ignored();
        assertEquals(ignored.size(), 1);
        assertEquals(ignored.get(0).unassignedInfo().getLastAllocationStatus(), AllocationStatus.DECIDERS_THROTTLED);
        assertTrue(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty());
    }

    /**
     * Tests that when there was a node that previously had the primary, it will be allocated to that same node again.
     */
    public void testPreferAllocatingPreviousPrimary() {
        String primaryAllocId = UUIDs.randomBase64UUID();
        String replicaAllocId = UUIDs.randomBase64UUID();
        RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            yesAllocationDeciders(),
            randomFrom(CLUSTER_RECOVERED, INDEX_REOPENED),
            primaryAllocId,
            replicaAllocId
        );
        boolean node1HasPrimaryShard = randomBoolean();
        testAllocator.addData(node1, node1HasPrimaryShard ? primaryAllocId : replicaAllocId, node1HasPrimaryShard);
        testAllocator.addData(node2, node1HasPrimaryShard ? replicaAllocId : primaryAllocId, !node1HasPrimaryShard);
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        DiscoveryNode allocatedNode = node1HasPrimaryShard ? node1 : node2;
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(allocatedNode.getId())
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when there is a node to allocate to, but it is throttling (and it is the only one),
     * it will be moved to ignore unassigned until it can be allocated to.
     */
    public void testFoundAllocationButThrottlingDecider() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            throttleAllocationDeciders(),
            CLUSTER_RECOVERED,
            "allocId1"
        );
        testAllocator.addData(node1, "allocId1", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when there is a node to be allocated to, but it the decider said "no", we still
     * force the allocation to it.
     */
    public void testFoundAllocationButNoDecider() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimaryNoReplicas(
            noAllocationDeciders(),
            CLUSTER_RECOVERED,
            "allocId1"
        );
        testAllocator.addData(node1, "allocId1", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node1.getId())
        );
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy and allocation
     * deciders say yes, we allocate to that node.
     */
    public void testRestore() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders(), randomLong(), "allocId");
        testAllocator.addData(node1, "some allocId", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy and allocation
     * deciders say throttle, we add it to ignored shards.
     */
    public void testRestoreThrottle() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(throttleAllocationDeciders(), randomLong(), "allocId");
        testAllocator.addData(node1, "some allocId", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(false));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when restoring from a snapshot and we find a node with a shard copy but allocation
     * deciders say no, we still allocate to that node.
     */
    public void testRestoreForcesAllocateIfShardAvailable() {
        final long shardSize = randomNonNegativeLong();
        RoutingAllocation allocation = getRestoreRoutingAllocation(noAllocationDeciders(), shardSize, "allocId");
        testAllocator.addData(node1, "some allocId", randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        final List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertThat(initializingShards.size(), equalTo(1));
        assertThat(initializingShards.get(0).getExpectedShardSize(), equalTo(shardSize));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when restoring from a snapshot and we don't find a node with a shard copy, the shard will remain in
     * the unassigned list to be allocated later.
     */
    public void testRestoreDoesNotAssignIfNoShardAvailable() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders(), randomNonNegativeLong(), "allocId");
        testAllocator.addData(node1, null, randomBoolean());
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().size(), equalTo(1));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    /**
     * Tests that when restoring from a snapshot and we don't know the shard size yet, the shard will remain in
     * the unassigned list to be allocated later.
     */
    public void testRestoreDoesNotAssignIfShardSizeNotAvailable() {
        RoutingAllocation allocation = getRestoreRoutingAllocation(yesAllocationDeciders(), null, "allocId");
        testAllocator.addData(node1, null, false);
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().unassigned().ignored().isEmpty(), equalTo(false));
        ShardRouting ignoredRouting = allocation.routingNodes().unassigned().ignored().get(0);
        assertThat(ignoredRouting.unassignedInfo().getLastAllocationStatus(), equalTo(AllocationStatus.FETCHING_SHARD_DATA));
        assertClusterHealthStatus(allocation, ClusterHealthStatus.YELLOW);
    }

    private RoutingAllocation getRestoreRoutingAllocation(AllocationDeciders allocationDeciders, Long shardSize, String... allocIds) {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putInSyncAllocationIds(0, Sets.newHashSet(allocIds))
            )
            .build();

        final Snapshot snapshot = new Snapshot("test", new SnapshotId("test", UUIDs.randomBase64UUID()));
        RoutingTable routingTable = RoutingTable.builder()
            .addAsRestore(
                metadata.index(shardId.getIndex()),
                new SnapshotRecoverySource(
                    UUIDs.randomBase64UUID(),
                    snapshot,
                    Version.CURRENT,
                    new IndexId(shardId.getIndexName(), UUIDs.randomBase64UUID(random()))
                )
            )
            .build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(allocationDeciders, new RoutingNodes(state, false), state, null, new SnapshotShardSizeInfo(Map.of()) {
            @Override
            public Long getShardSize(ShardRouting shardRouting) {
                return shardSize;
            }
        }, System.nanoTime());
    }

    private RoutingAllocation routingAllocationWithOnePrimaryNoReplicas(
        AllocationDeciders deciders,
        UnassignedInfo.Reason reason,
        String... activeAllocationIds
    ) {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putInSyncAllocationIds(shardId.id(), Sets.newHashSet(activeAllocationIds))
            )
            .build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        switch (reason) {

            case INDEX_CREATED:
                routingTableBuilder.addAsNew(metadata.index(shardId.getIndex()));
                break;
            case CLUSTER_RECOVERED:
                routingTableBuilder.addAsRecovery(metadata.index(shardId.getIndex()));
                break;
            case INDEX_REOPENED:
                routingTableBuilder.addAsFromCloseToOpen(metadata.index(shardId.getIndex()));
                break;
            default:
                throw new IllegalArgumentException("can't do " + reason + " for you. teach me");
        }
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, null, null, System.nanoTime());
    }

    private void assertClusterHealthStatus(RoutingAllocation allocation, ClusterHealthStatus expectedStatus) {
        RoutingTable oldRoutingTable = allocation.routingTable();
        RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster")).routingTable(newRoutingTable).build();
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState);
        assertThat(clusterStateHealth.getStatus().ordinal(), lessThanOrEqualTo(expectedStatus.ordinal()));
    }

    private AllocationDecider getNoDeciderThatAllowsForceAllocate() {
        return getNoDeciderWithForceAllocate(Decision.YES);
    }

    private AllocationDecider getNoDeciderThatThrottlesForceAllocate() {
        return getNoDeciderWithForceAllocate(Decision.THROTTLE);
    }

    private AllocationDecider getNoDeciderThatDeniesForceAllocate() {
        return getNoDeciderWithForceAllocate(Decision.NO);
    }

    private AllocationDecider getNoDeciderWithForceAllocate(final Decision forceAllocateDecision) {
        return new TestAllocateDecision(Decision.NO) {
            @Override
            public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                assert shardRouting.primary() : "cannot force allocate a non-primary shard " + shardRouting;
                return forceAllocateDecision;
            }
        };
    }

    class TestAllocator extends PrimaryShardAllocator {

        private Map<DiscoveryNode, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> data;

        public TestAllocator clear() {
            data = null;
            return this;
        }

        public TestAllocator addData(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint
        ) {
            return addData(node, allocationId, primary, replicationCheckpoint, null);
        }

        public TestAllocator addData(DiscoveryNode node, String allocationId, boolean primary) {
            return addData(
                node,
                allocationId,
                primary,
                ReplicationCheckpoint.empty(shardId, new CodecService(null, null).codec("default").getName()),
                null
            );
        }

        public TestAllocator addData(DiscoveryNode node, String allocationId, boolean primary, @Nullable Exception storeException) {
            return addData(
                node,
                allocationId,
                primary,
                ReplicationCheckpoint.empty(shardId, new CodecService(null, null).codec("default").getName()),
                storeException
            );
        }

        public TestAllocator addData(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            @Nullable Exception storeException
        ) {
            if (data == null) {
                data = new HashMap<>();
            }
            data.put(
                node,
                new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(
                    node,
                    allocationId,
                    primary,
                    replicationCheckpoint,
                    storeException
                )
            );
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            return new AsyncShardFetch.FetchResult<>(shardId, data, Collections.<String>emptySet());
        }
    }
}
