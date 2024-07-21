/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gateway;

import org.apache.lucene.codecs.Codec;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.UnassignedInfo.Reason.CLUSTER_RECOVERED;

public class PrimaryShardBatchAllocatorTests extends OpenSearchAllocationTestCase {

    private final ShardId shardId = new ShardId("test", "_na_", 0);
    private static Set<ShardId> shardsInBatch;
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");
    private TestBatchAllocator batchAllocator;

    public static void setUpShards(int numberOfShards) {
        shardsInBatch = new HashSet<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            ShardId shardId = new ShardId("test", "_na_", shardNumber);
            shardsInBatch.add(shardId);
        }
    }

    @Before
    public void buildTestAllocator() {
        this.batchAllocator = new TestBatchAllocator();
    }

    private void allocateAllUnassigned(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            batchAllocator.allocateUnassigned(iterator.next(), allocation, iterator);
        }
    }

    private void allocateAllUnassignedBatch(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        List<ShardRouting> shardsToBatch = new ArrayList<>();
        while (iterator.hasNext()) {
            ShardRouting unassignedShardRouting = iterator.next();
            if (unassignedShardRouting.primary()) {
                shardsToBatch.add(unassignedShardRouting);
            }
        }
        batchAllocator.allocateUnassignedBatch(shardsToBatch, allocation);
    }

    public void testMakeAllocationDecisionDataFetching() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimary(noAllocationDeciders(), CLUSTER_RECOVERED, "allocId1");
        ShardRouting shard = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard();
        AllocateUnassignedDecision allocateUnassignedDecision = batchAllocator.makeAllocationDecision(shard, allocation, logger);
        assertEquals(AllocationDecision.AWAITING_INFO, allocateUnassignedDecision.getAllocationDecision());
    }

    public void testMakeAllocationDecisionForReplicaShard() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimary(noAllocationDeciders(), CLUSTER_RECOVERED, "allocId1");

        List<ShardRouting> replicaShards = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).replicaShards();
        for (ShardRouting shardRouting : replicaShards) {
            AllocateUnassignedDecision allocateUnassignedDecision = batchAllocator.makeAllocationDecision(shardRouting, allocation, logger);
            assertFalse(allocateUnassignedDecision.isDecisionTaken());
        }
    }

    public void testMakeAllocationDecisionDataFetched() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimary(noAllocationDeciders(), CLUSTER_RECOVERED, "allocId1");

        ShardRouting shard = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard();
        batchAllocator.addData(node1, "allocId1", true, new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()));
        AllocateUnassignedDecision allocateUnassignedDecision = batchAllocator.makeAllocationDecision(shard, allocation, logger);
        assertEquals(AllocationDecision.YES, allocateUnassignedDecision.getAllocationDecision());
    }

    public void testMakeAllocationDecisionDataFetchedMultipleShards() {
        setUpShards(2);
        final RoutingAllocation allocation = routingAllocationWithMultiplePrimaries(
            noAllocationDeciders(),
            CLUSTER_RECOVERED,
            2,
            0,
            "allocId-0",
            "allocId-1"
        );
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardId shardId : shardsInBatch) {
            ShardRouting shard = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard();
            allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard().recoverySource();
            shards.add(shard);
            batchAllocator.addShardData(
                node1,
                "allocId-" + shardId.id(),
                shardId,
                true,
                new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()),
                null
            );
        }
        for (ShardRouting shardRouting : shards) {
            AllocateUnassignedDecision allocateUnassignedDecision = batchAllocator.makeAllocationDecision(shardRouting, allocation, logger);
            assertEquals(AllocationDecision.YES, allocateUnassignedDecision.getAllocationDecision());
        }
    }

    public void testInitializePrimaryShards() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(Settings.builder().build(), clusterSettings, random());
        setUpShards(2);
        final RoutingAllocation routingAllocation = routingAllocationWithMultiplePrimaries(
            allocationDeciders,
            CLUSTER_RECOVERED,
            2,
            0,
            "allocId-0",
            "allocId-1"
        );

        for (ShardId shardId : shardsInBatch) {
            batchAllocator.addShardData(
                node1,
                "allocId-" + shardId.id(),
                shardId,
                true,
                new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()),
                null
            );
        }

        allocateAllUnassignedBatch(routingAllocation);

        assertEquals(0, routingAllocation.routingNodes().unassigned().size());
        List<ShardRouting> initializingShards = routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(2, initializingShards.size());
        assertTrue(shardsInBatch.contains(initializingShards.get(0).shardId()));
        assertTrue(shardsInBatch.contains(initializingShards.get(1).shardId()));
        assertEquals(2, routingAllocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId()));
    }

    public void testInitializeOnlyPrimaryUnassignedShardsIgnoreReplicaShards() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(Settings.builder().build(), clusterSettings, random());
        setUpShards(1);
        final RoutingAllocation routingAllocation = routingAllocationWithOnePrimary(allocationDeciders, CLUSTER_RECOVERED, "allocId-0");

        for (ShardId shardId : shardsInBatch) {
            batchAllocator.addShardData(
                node1,
                "allocId-0",
                shardId,
                true,
                new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()),
                null
            );
        }

        allocateAllUnassignedBatch(routingAllocation);

        List<ShardRouting> initializingShards = routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(1, initializingShards.size());
        assertTrue(shardsInBatch.contains(initializingShards.get(0).shardId()));
        assertTrue(initializingShards.get(0).primary());
        assertEquals(1, routingAllocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId()));
        List<ShardRouting> unassignedShards = routingAllocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED);
        assertEquals(1, unassignedShards.size());
        assertTrue(!unassignedShards.get(0).primary());
    }

    public void testAllocateUnassignedBatchThrottlingAllocationDeciderIsHonoured() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(
            Settings.builder()
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
                .build(),
            clusterSettings,
            random()
        );
        setUpShards(2);
        final RoutingAllocation routingAllocation = routingAllocationWithMultiplePrimaries(
            allocationDeciders,
            CLUSTER_RECOVERED,
            2,
            0,
            "allocId-0",
            "allocId-1"
        );

        for (ShardId shardId : shardsInBatch) {
            batchAllocator.addShardData(
                node1,
                "allocId-" + shardId.id(),
                shardId,
                true,
                new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()),
                null
            );
        }

        allocateAllUnassignedBatch(routingAllocation);

        // Verify the throttling decider was not throttled, recovering shards on node greater than initial concurrent recovery setting
        assertEquals(1, routingAllocation.routingNodes().getInitialPrimariesIncomingRecoveries(node1.getId()));
        List<ShardRouting> initializingShards = routingAllocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        assertEquals(1, initializingShards.size());
        Set<String> nodesWithInitialisingShards = initializingShards.stream().map(ShardRouting::currentNodeId).collect(Collectors.toSet());
        assertEquals(1, nodesWithInitialisingShards.size());
        assertEquals(Collections.singleton(node1.getId()), nodesWithInitialisingShards);
        List<ShardRouting> ignoredShards = routingAllocation.routingNodes().unassigned().ignored();
        assertEquals(1, ignoredShards.size());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, ignoredShards.get(0).unassignedInfo().getLastAllocationStatus());
    }

    public void testAllocateUnassignedBatchOnTimeoutWithMatchingPrimaryShards() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(Settings.builder().build(), clusterSettings, random());
        setUpShards(1);
        final RoutingAllocation routingAllocation = routingAllocationWithOnePrimary(allocationDeciders, CLUSTER_RECOVERED, "allocId-0");
        ShardRouting shardRouting = routingAllocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard();

        List<ShardRouting> shardRoutings = Arrays.asList(shardRouting);
        batchAllocator.allocateUnassignedBatchOnTimeout(shardRoutings, routingAllocation, true);

        List<ShardRouting> ignoredShards = routingAllocation.routingNodes().unassigned().ignored();
        assertEquals(1, ignoredShards.size());
        assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, ignoredShards.get(0).unassignedInfo().getLastAllocationStatus());
    }

    public void testAllocateUnassignedBatchOnTimeoutWithNoMatchingPrimaryShards() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(Settings.builder().build(), clusterSettings, random());
        setUpShards(1);
        final RoutingAllocation routingAllocation = routingAllocationWithOnePrimary(allocationDeciders, CLUSTER_RECOVERED, "allocId-0");
        List<ShardRouting> shardRoutings = new ArrayList<>();
        batchAllocator.allocateUnassignedBatchOnTimeout(shardRoutings, routingAllocation, true);

        List<ShardRouting> ignoredShards = routingAllocation.routingNodes().unassigned().ignored();
        assertEquals(0, ignoredShards.size());
    }

    public void testAllocateUnassignedBatchOnTimeoutWithNonPrimaryShards() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = randomAllocationDeciders(Settings.builder().build(), clusterSettings, random());
        setUpShards(1);
        final RoutingAllocation routingAllocation = routingAllocationWithOnePrimary(allocationDeciders, CLUSTER_RECOVERED, "allocId-0");

        ShardRouting shardRouting = routingAllocation.routingTable()
            .getIndicesRouting()
            .get("test")
            .shard(shardId.id())
            .replicaShards()
            .get(0);
        List<ShardRouting> shardRoutings = Arrays.asList(shardRouting);
        batchAllocator.allocateUnassignedBatchOnTimeout(shardRoutings, routingAllocation, true);

        List<ShardRouting> ignoredShards = routingAllocation.routingNodes().unassigned().ignored();
        assertEquals(0, ignoredShards.size());
    }

    private RoutingAllocation routingAllocationWithOnePrimary(
        AllocationDeciders deciders,
        UnassignedInfo.Reason reason,
        String... activeAllocationIds
    ) {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
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
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, null, System.nanoTime());
    }

    private RoutingAllocation routingAllocationWithMultiplePrimaries(
        AllocationDeciders deciders,
        UnassignedInfo.Reason reason,
        int numberOfShards,
        int replicas,
        String... activeAllocationIds
    ) {
        Iterator<ShardId> shardIterator = shardsInBatch.iterator();
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(replicas)
                    .putInSyncAllocationIds(shardIterator.next().id(), Sets.newHashSet(activeAllocationIds[0]))
                    .putInSyncAllocationIds(shardIterator.next().id(), Sets.newHashSet(activeAllocationIds[1]))
            )
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (ShardId shardIdFromBatch : shardsInBatch) {
            switch (reason) {
                case INDEX_CREATED:
                    routingTableBuilder.addAsNew(metadata.index(shardIdFromBatch.getIndex()));
                    break;
                case CLUSTER_RECOVERED:
                    routingTableBuilder.addAsRecovery(metadata.index(shardIdFromBatch.getIndex()));
                    break;
                case INDEX_REOPENED:
                    routingTableBuilder.addAsFromCloseToOpen(metadata.index(shardIdFromBatch.getIndex()));
                    break;
                default:
                    throw new IllegalArgumentException("can't do " + reason + " for you. teach me");
            }
        }
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, ClusterInfo.EMPTY, null, System.nanoTime());
    }

    class TestBatchAllocator extends PrimaryShardBatchAllocator {

        private Map<DiscoveryNode, TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> data;

        public TestBatchAllocator clear() {
            data = null;
            return this;
        }

        public TestBatchAllocator addData(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint
        ) {
            return addData(node, allocationId, primary, replicationCheckpoint, null);
        }

        public TestBatchAllocator addData(DiscoveryNode node, String allocationId, boolean primary) {
            Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", nodeSettings);
            return addData(
                node,
                allocationId,
                primary,
                ReplicationCheckpoint.empty(shardId, new CodecService(null, indexSettings, null).codec("default").getName()),
                null
            );
        }

        public TestBatchAllocator addData(DiscoveryNode node, String allocationId, boolean primary, @Nullable Exception storeException) {
            Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", nodeSettings);
            return addData(
                node,
                allocationId,
                primary,
                ReplicationCheckpoint.empty(shardId, new CodecService(null, indexSettings, null).codec("default").getName()),
                storeException
            );
        }

        public TestBatchAllocator addData(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            @Nullable Exception storeException
        ) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<ShardId, GatewayStartedShard> shardData = Map.of(
                shardId,
                new GatewayStartedShard(allocationId, primary, replicationCheckpoint, storeException)
            );
            data.put(node, new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch(node, shardData));
            return this;
        }

        public TestBatchAllocator addShardData(
            DiscoveryNode node,
            String allocationId,
            ShardId shardId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            @Nullable Exception storeException
        ) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<ShardId, GatewayStartedShard> shardData = new HashMap<>();
            shardData.put(shardId, new GatewayStartedShard(allocationId, primary, replicationCheckpoint, storeException));
            if (data.get(node) != null) shardData.putAll(data.get(node).getNodeGatewayStartedShardsBatch());
            data.put(node, new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch(node, shardData));
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> fetchData(
            List<ShardRouting> shardsEligibleForFetch,
            List<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            return new AsyncShardFetch.FetchResult<>(data, Collections.<ShardId, Set<String>>emptyMap());
        }
    }
}
