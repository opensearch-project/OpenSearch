/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper;
import org.opensearch.snapshots.SnapshotShardSizeInfo;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ReplicaShardBatchAllocatorTests extends OpenSearchAllocationTestCase {
    private static final org.apache.lucene.util.Version MIN_SUPPORTED_LUCENE_VERSION = org.opensearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;
    private final ShardId shardId = new ShardId("test", "_na_", 0);
    private static Set<ShardId> shardsInBatch;
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");

    private TestBatchAllocator testBatchAllocator;

    @Before
    public void buildTestAllocator() {
        this.testBatchAllocator = new TestBatchAllocator();
    }

    public static void setUpShards(int numberOfShards) {
        shardsInBatch = new HashSet<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            ShardId shardId = new ShardId("test", "_na_", shardNumber);
            shardsInBatch.add(shardId);
        }
    }

    private void allocateAllUnassignedBatch(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        List<ShardRouting> shardToBatch = new ArrayList<>();
        while (iterator.hasNext()) {
            shardToBatch.add(iterator.next());
        }
        testBatchAllocator.allocateUnassignedBatch(shardToBatch, allocation);
    }

    /**
     * Verifies that when we are still fetching data in an async manner, the replica shard moves to ignore unassigned.
     */
    public void testNoAsyncFetchData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testBatchAllocator.clean();
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that on index creation, we don't fetch data for any shards, but keep the replica shard unassigned to let
     * the shard allocator to allocate it. There isn't a copy around to find anyhow.
     */
    public void testAsyncFetchWithNoShardOnIndexCreation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.EMPTY,
            UnassignedInfo.Reason.INDEX_CREATED
        );
        testBatchAllocator.clean();
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that for anything but index creation, fetch data ends up being called, since we need to go and try
     * and find a better copy for the shard.
     */
    public void testAsyncFetchOnAnythingButIndexCreation() {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(
            random(),
            EnumSet.complementOf(EnumSet.of(UnassignedInfo.Reason.INDEX_CREATED))
        );
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(), Settings.EMPTY, reason);
        testBatchAllocator.clean();
        allocateAllUnassignedBatch(allocation);
        assertThat("failed with reason " + reason, testBatchAllocator.getFetchDataCalledAndClean(), equalTo(true));
        assertThat("failed with reason" + reason, testBatchAllocator.getShardEligibleFetchDataCountAndClean(), equalTo(1));
    }

    /**
     * Verifies that when there is a full match (syncId and files) we allocate it to matching node.
     */
    public void testSimpleFullMatchAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    /**
     * Verifies that when there is a sync id match but no files match, we allocate it to matching node.
     */
    public void testSyncIdMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "MATCH", null, new StoreFileMetadata("file1", 10, "NO_MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    /**
     * Verifies that when there is no sync id match but files match, we allocate it to matching node.
     */
    public void testFileChecksumMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "NO_MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    public void testPreferCopyWithHighestMatchingOperations() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        long retainingSeqNoOnPrimary = randomLongBetween(1, Integer.MAX_VALUE);
        long retainingSeqNoForNode2 = randomLongBetween(0, retainingSeqNoOnPrimary - 1);
        // Rarely use a seqNo above retainingSeqNoOnPrimary, which could in theory happen when primary fails and comes back quickly.
        long retainingSeqNoForNode3 = randomLongBetween(retainingSeqNoForNode2 + 1, retainingSeqNoOnPrimary + 100);
        List<RetentionLease> retentionLeases = Arrays.asList(
            newRetentionLease(node1, retainingSeqNoOnPrimary),
            newRetentionLease(node2, retainingSeqNoForNode2),
            newRetentionLease(node3, retainingSeqNoForNode3)
        );
        testBatchAllocator.addData(
            node1,
            retentionLeases,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node2,
            "NOT_MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node3,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
    }

    public void testCancelRecoveryIfFoundCopyWithNoopRetentionLease() {
        final UnassignedInfo unassignedInfo;
        final Set<String> failedNodes;
        if (randomBoolean()) {
            failedNodes = Collections.emptySet();
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null);
        } else {
            failedNodes = new HashSet<>(randomSubsetOf(Arrays.asList("node-4", "node-5", "node-6")));
            unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                null,
                null,
                randomIntBetween(1, 10),
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                failedNodes
            );
        }
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        testBatchAllocator.addData(
            node1,
            Arrays.asList(newRetentionLease(node1, retainingSeqNo), newRetentionLease(node3, retainingSeqNo)),
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node2,
            "NO_MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node3,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        Collection<ShardRouting> replicaShards = allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED);
        List<ShardRouting> shardRoutingBatch = new ArrayList<>(replicaShards);
        List<List<ShardRouting>> shardBatchList = Collections.singletonList(
            new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING))
        );

        testBatchAllocator.processExistingRecoveries(allocation, shardBatchList);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        List<ShardRouting> unassignedShards = allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED);
        assertThat(unassignedShards, hasSize(1));
        assertThat(unassignedShards.get(0).shardId(), equalTo(shardId));
        assertThat(unassignedShards.get(0).unassignedInfo().getNumFailedAllocations(), equalTo(0));
        assertThat(unassignedShards.get(0).unassignedInfo().getFailedNodeIds(), equalTo(failedNodes));
    }

    public void testNotCancellingRecoveryIfCurrentRecoveryHasRetentionLease() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        List<RetentionLease> peerRecoveryRetentionLeasesOnPrimary = new ArrayList<>();
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node1, retainingSeqNo));
        peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node2, randomLongBetween(1, retainingSeqNo)));
        if (randomBoolean()) {
            peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNo)));
        }
        testBatchAllocator.addData(
            node1,
            peerRecoveryRetentionLeasesOnPrimary,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node2,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node3,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testNotCancelIfPrimaryDoesNotHaveValidRetentionLease() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testBatchAllocator.addData(
            node1,
            Collections.singletonList(newRetentionLease(node3, randomNonNegativeLong())),
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node2,
            "NOT_MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node3,
            "NOT_MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testIgnoreRetentionLeaseIfCopyIsEmpty() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        List<RetentionLease> retentionLeases = new ArrayList<>();
        retentionLeases.add(newRetentionLease(node1, retainingSeqNo));
        retentionLeases.add(newRetentionLease(node2, randomLongBetween(0, retainingSeqNo)));
        if (randomBoolean()) {
            retentionLeases.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNo)));
        }
        testBatchAllocator.addData(
            node1,
            retentionLeases,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(node2, null, null); // has retention lease but store is empty
        testBatchAllocator.addData(
            node3,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
    }

    /**
     * When we can't find primary data, but still find replica data, we go ahead and keep it unassigned
     * to be allocated. This is today behavior, which relies on a primary corruption identified with
     * adding a replica and having that replica actually recover and cause the corruption to be identified
     * See CorruptFileTest#
     */
    public void testNoPrimaryData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testBatchAllocator.addData(
            node2,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is primary data, but no data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    public void testNoDataForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testBatchAllocator.addData(
            node1,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is primary data, but no matching data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    public void testNoMatchingFilesForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "NO_MATCH", null, new StoreFileMetadata("file1", 10, "NO_MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * When there is no decision or throttle decision across all nodes for the shard, make sure the shard
     * moves to the ignore unassigned list.
     */
    public void testNoOrThrottleDecidersRemainsInUnassigned() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            randomBoolean() ? noAllocationDeciders() : throttleAllocationDeciders()
        );
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node to allocate to due to matching is being throttled, we move the shard to ignored
     * to wait till throttling on it is done.
     */
    public void testThrottleWhenAllocatingToMatchingNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            new AllocationDeciders(
                Arrays.asList(
                    new TestAllocateDecision(Decision.YES),
                    new SameShardAllocationDecider(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ),
                    new AllocationDecider() {
                        @Override
                        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                            if (node.node().equals(node2)) {
                                return Decision.THROTTLE;
                            }
                            return Decision.YES;
                        }
                    }
                )
            )
        );
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    public void testDelayedAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1)).build(),
            UnassignedInfo.Reason.NODE_LEFT
        );
        testBatchAllocator.addData(
            node1,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        if (randomBoolean()) {
            // we sometime return empty list of files, make sure we test this as well
            testBatchAllocator.addData(node2, null, null);
        }
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));

        allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1)).build(),
            UnassignedInfo.Reason.NODE_LEFT
        );
        testBatchAllocator.addData(
            node2,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node2.getId())
        );
    }

    public void testCancelRecoveryBetterSyncId() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "NO_MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node3, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    public void testNotCancellingRecoveryIfSyncedOnExistingRecovery() {
        final UnassignedInfo unassignedInfo;
        if (randomBoolean()) {
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null);
        } else {
            unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                null,
                null,
                randomIntBetween(1, 10),
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                Collections.singleton("node-4")
            );
        }
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        List<RetentionLease> retentionLeases = new ArrayList<>();
        if (randomBoolean()) {
            long retainingSeqNoOnPrimary = randomLongBetween(0, Long.MAX_VALUE);
            retentionLeases.add(newRetentionLease(node1, retainingSeqNoOnPrimary));
            if (randomBoolean()) {
                retentionLeases.add(newRetentionLease(node2, randomLongBetween(0, retainingSeqNoOnPrimary)));
            }
            if (randomBoolean()) {
                retentionLeases.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNoOnPrimary)));
            }
        }
        testBatchAllocator.addData(
            node1,
            retentionLeases,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node2,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.addData(
            node3,
            randomSyncId(),
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testNotCancellingRecovery() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testDoNotCancelForBrokenNode() {
        Set<String> failedNodes = new HashSet<>();
        failedNodes.add(node3.getId());
        if (randomBoolean()) {
            failedNodes.add("node4");
        }
        UnassignedInfo unassignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            null,
            null,
            randomIntBetween(failedNodes.size(), 10),
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            failedNodes
        );
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        long retainingSeqNoOnPrimary = randomLongBetween(0, Long.MAX_VALUE);
        List<RetentionLease> retentionLeases = Arrays.asList(
            newRetentionLease(node1, retainingSeqNoOnPrimary),
            newRetentionLease(node3, retainingSeqNoOnPrimary)
        );
        testBatchAllocator.addData(
            node1,
            retentionLeases,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        )
            .addData(node2, randomSyncId(), null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node3, randomSyncId(), null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED), empty());
    }

    public void testDoNotCancelForInactivePrimaryNode() {
        RoutingAllocation allocation = oneInactivePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), null);
        testBatchAllocator.addData(
            node1,
            null,
            "MATCH",
            null,
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        ).addData(node2, randomSyncId(), null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));

        testBatchAllocator.processExistingRecoveries(
            allocation,
            Collections.singletonList(new ArrayList<>(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING)))
        );

        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED), empty());
    }

    public void testAllocateUnassignedBatchThrottlingAllocationDeciderIsHonoured() throws InterruptedException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(
            Settings.builder()
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
                .build(),
            clusterSettings,
            random()
        );
        setUpShards(2);
        final RoutingAllocation routingAllocation = twoPrimaryAndOneUnAssignedReplica(allocationDeciders);
        for (ShardId shardIdFromBatch : shardsInBatch) {
            testBatchAllocator.addShardData(
                node1,
                shardIdFromBatch,
                "MATCH",
                null,
                new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
            )
                .addShardData(
                    node2,
                    shardIdFromBatch,
                    "NO_MATCH",
                    null,
                    new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
                )
                .addShardData(
                    node3,
                    shardIdFromBatch,
                    "MATCH",
                    null,
                    new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
                );
        }
        allocateAllUnassignedBatch(routingAllocation);
        // Verify the throttling decider was throttled, incoming recoveries on a node should be
        // lesser than or equal to allowed concurrent recoveries
        assertEquals(0, routingAllocation.routingNodes().getIncomingRecoveries(node2.getId()));
        assertEquals(1, routingAllocation.routingNodes().getIncomingRecoveries(node3.getId()));
        List<ShardRouting> initializingShards = routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(1, initializingShards.size());
        List<ShardRouting> ignoredShardRoutings = routingAllocation.routingNodes().unassigned().ignored();
        assertEquals(1, ignoredShardRoutings.size());
        // Allocation status for ignored replicas shards is not updated after running the deciders they just get marked as ignored.
        assertEquals(UnassignedInfo.AllocationStatus.NO_ATTEMPT, ignoredShardRoutings.get(0).unassignedInfo().getLastAllocationStatus());
        AllocateUnassignedDecision allocateUnassignedDecision = testBatchAllocator.makeAllocationDecision(
            ignoredShardRoutings.get(0),
            routingAllocation,
            logger
        );
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, allocateUnassignedDecision.getAllocationStatus());
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1Replica(deciders, Settings.EMPTY, UnassignedInfo.Reason.CLUSTER_RECOVERED);
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders, Settings settings, UnassignedInfo.Reason reason) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putInSyncAllocationIds(0, Sets.newHashSet(primaryShard.allocationId().getId()));
        Metadata metadata = Metadata.builder().put(indexMetadata).build();
        // mark shard as delayed if reason is NODE_LEFT
        boolean delayed = reason == UnassignedInfo.Reason.NODE_LEFT
            && UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings).nanos() > 0;
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                ShardRouting.newUnassigned(
                                    shardId,
                                    false,
                                    RecoverySource.PeerRecoverySource.INSTANCE,
                                    new UnassignedInfo(
                                        reason,
                                        null,
                                        null,
                                        failedAllocations,
                                        System.nanoTime(),
                                        System.currentTimeMillis(),
                                        delayed,
                                        UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                                        Collections.emptySet()
                                    )
                                )
                            )
                            .build()
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation twoPrimaryAndOneUnAssignedReplica(AllocationDeciders deciders) throws InterruptedException {
        Map<ShardId, ShardRouting> shardIdShardRoutingMap = new HashMap<>();
        Index index = shardId.getIndex();

        // Created started ShardRouting for each primary shards
        for (ShardId shardIdFromBatch : shardsInBatch) {
            shardIdShardRoutingMap.put(
                shardIdFromBatch,
                TestShardRouting.newShardRouting(shardIdFromBatch, node1.getId(), true, ShardRoutingState.STARTED)
            );
        }

        // Create Index Metadata
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(index.getName())
            .settings(settings(Version.CURRENT).put(Settings.EMPTY))
            .numberOfShards(2)
            .numberOfReplicas(1);
        for (ShardId shardIdFromBatch : shardsInBatch) {
            indexMetadata.putInSyncAllocationIds(
                shardIdFromBatch.id(),
                Sets.newHashSet(shardIdShardRoutingMap.get(shardIdFromBatch).allocationId().getId())
            );
        }
        Metadata metadata = Metadata.builder().put(indexMetadata).build();

        // Create Index Routing table
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (ShardId shardIdFromBatch : shardsInBatch) {
            IndexShardRoutingTable.Builder indexShardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardIdFromBatch);
            // Add a primary shard in started state
            indexShardRoutingTableBuilder.addShard(shardIdShardRoutingMap.get(shardIdFromBatch));
            // Add replicas of primary shard in un-assigned state.
            for (int i = 0; i < 1; i++) {
                indexShardRoutingTableBuilder.addShard(
                    ShardRouting.newUnassigned(
                        shardIdFromBatch,
                        false,
                        RecoverySource.PeerRecoverySource.INSTANCE,
                        new UnassignedInfo(
                            UnassignedInfo.Reason.CLUSTER_RECOVERED,
                            null,
                            null,
                            0,
                            System.nanoTime(),
                            System.currentTimeMillis(),
                            false,
                            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                            Collections.emptySet()
                        )
                    )
                );
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTableBuilder.build());
        }

        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTableBuilder.build()).build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders, UnassignedInfo unassignedInfo) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Sets.newHashSet(primaryShard.allocationId().getId()))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                TestShardRouting.newShardRouting(
                                    shardId,
                                    node2.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.INITIALIZING,
                                    unassignedInfo
                                )
                            )
                            .build()
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation oneInactivePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders, UnassignedInfo unassignedInfo) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.INITIALIZING);
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                TestShardRouting.newShardRouting(
                                    shardId,
                                    node2.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.INITIALIZING,
                                    unassignedInfo
                                )
                            )
                            .build()
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2))
            .build();
        return new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1ReplicaRecovering(deciders, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
    }

    static RetentionLease newRetentionLease(DiscoveryNode node, long retainingSeqNo) {
        return new RetentionLease(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId()),
            retainingSeqNo,
            randomNonNegativeLong(),
            ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
        );
    }

    static String randomSyncId() {
        return randomFrom("MATCH", "NOT_MATCH", null);
    }

    class TestBatchAllocator extends ReplicaShardBatchAllocator {
        private Map<DiscoveryNode, TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> data = null;
        private AtomicBoolean fetchDataCalled = new AtomicBoolean(false);
        private AtomicInteger eligibleShardFetchDataCount = new AtomicInteger(0);

        public void clean() {
            data = null;
        }

        public boolean getFetchDataCalledAndClean() {
            return fetchDataCalled.getAndSet(false);
        }

        public int getShardEligibleFetchDataCountAndClean() {
            return eligibleShardFetchDataCount.getAndSet(0);
        }

        public TestBatchAllocator addData(
            DiscoveryNode node,
            String syncId,
            @Nullable Exception storeFileFetchException,
            StoreFileMetadata... files
        ) {
            return addData(node, Collections.emptyList(), syncId, storeFileFetchException, files);
        }

        public TestBatchAllocator addData(
            DiscoveryNode node,
            List<RetentionLease> peerRecoveryRetentionLeases,
            String syncId,
            @Nullable Exception storeFileFetchException,
            StoreFileMetadata... files
        ) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<String, StoreFileMetadata> filesAsMap = new HashMap<>();
            for (StoreFileMetadata file : files) {
                filesAsMap.put(file.name(), file);
            }
            Map<String, String> commitData = new HashMap<>();
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            data.put(
                node,
                new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch(
                    node,
                    Map.of(
                        shardId,
                        new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata(
                            new TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata(
                                shardId,
                                new Store.MetadataSnapshot(unmodifiableMap(filesAsMap), unmodifiableMap(commitData), randomInt()),
                                peerRecoveryRetentionLeases
                            ),
                            storeFileFetchException
                        )
                    )
                )
            );
            return this;
        }

        public TestBatchAllocator addShardData(
            DiscoveryNode node,
            ShardId shardId,
            String syncId,
            @Nullable Exception storeFileFetchException,
            StoreFileMetadata... files
        ) {
            return addShardData(node, Collections.emptyList(), shardId, syncId, storeFileFetchException, files);
        }

        public TestBatchAllocator addShardData(
            DiscoveryNode node,
            List<RetentionLease> peerRecoveryRetentionLeases,
            ShardId shardId,
            String syncId,
            @Nullable Exception storeFileFetchException,
            StoreFileMetadata... files
        ) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<String, StoreFileMetadata> filesAsMap = new HashMap<>();
            for (StoreFileMetadata file : files) {
                filesAsMap.put(file.name(), file);
            }
            Map<String, String> commitData = new HashMap<>();
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }

            TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata =
                new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata(
                    new TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata(
                        shardId,
                        new Store.MetadataSnapshot(unmodifiableMap(filesAsMap), unmodifiableMap(commitData), randomInt()),
                        peerRecoveryRetentionLeases
                    ),
                    storeFileFetchException
                );
            Map<ShardId, TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata> shardIdNodeStoreFilesMetadataHashMap =
                new HashMap<>();
            if (data.containsKey(node)) {
                NodeStoreFilesMetadataBatch nodeStoreFilesMetadataBatch = data.get(node);
                shardIdNodeStoreFilesMetadataHashMap.putAll(nodeStoreFilesMetadataBatch.getNodeStoreFilesMetadataBatch());
            }
            shardIdNodeStoreFilesMetadataHashMap.put(shardId, nodeStoreFilesMetadata);
            data.put(
                node,
                new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch(node, shardIdNodeStoreFilesMetadataHashMap)
            );

            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetadataBatch> fetchData(
            List<ShardRouting> eligibleShards,
            List<ShardRouting> ineligibleShards,
            RoutingAllocation allocation
        ) {
            fetchDataCalled.set(true);
            eligibleShardFetchDataCount.set(eligibleShards.size());
            return new AsyncShardFetch.FetchResult<>(data, Collections.<ShardId, Set<String>>emptyMap());
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return fetchDataCalled.get();
        }
    }
}
