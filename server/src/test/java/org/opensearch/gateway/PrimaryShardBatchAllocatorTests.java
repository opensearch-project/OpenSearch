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
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            shardsToBatch.add(iterator.next());
        }
        batchAllocator.allocateUnassignedBatch(shardsToBatch, allocation);
    }

    public void testMakeAllocationDecisionDataFetching() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimary(noAllocationDeciders(), CLUSTER_RECOVERED, "allocId1");

        List<ShardRouting> shards = new ArrayList<>();
        allocateAllUnassignedBatch(allocation);
        ShardRouting shard = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard();
        shards.add(shard);
        HashMap<ShardRouting, AllocateUnassignedDecision> allDecisions = batchAllocator.makeAllocationDecision(shards, allocation, logger);
        // verify we get decisions for all the shards
        assertEquals(shards.size(), allDecisions.size());
        assertEquals(shards, new ArrayList<>(allDecisions.keySet()));
        assertEquals(AllocationDecision.AWAITING_INFO, allDecisions.get(shard).getAllocationDecision());
    }

    public void testMakeAllocationDecisionForReplicaShard() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimary(noAllocationDeciders(), CLUSTER_RECOVERED, "allocId1");

        List<ShardRouting> replicaShards = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).replicaShards();
        List<ShardRouting> shards = new ArrayList<>(replicaShards);
        HashMap<ShardRouting, AllocateUnassignedDecision> allDecisions = batchAllocator.makeAllocationDecision(shards, allocation, logger);
        // verify we get decisions for all the shards
        assertEquals(shards.size(), allDecisions.size());
        assertEquals(shards, new ArrayList<>(allDecisions.keySet()));
        assertFalse(allDecisions.get(replicaShards.get(0)).isDecisionTaken());
    }

    public void testMakeAllocationDecisionDataFetched() {
        final RoutingAllocation allocation = routingAllocationWithOnePrimary(noAllocationDeciders(), CLUSTER_RECOVERED, "allocId1");

        List<ShardRouting> shards = new ArrayList<>();
        ShardRouting shard = allocation.routingTable().getIndicesRouting().get("test").shard(shardId.id()).primaryShard();
        shards.add(shard);
        batchAllocator.addData(node1, "allocId1", true, new ReplicationCheckpoint(shardId, 20, 101, 1, Codec.getDefault().getName()));
        HashMap<ShardRouting, AllocateUnassignedDecision> allDecisions = batchAllocator.makeAllocationDecision(shards, allocation, logger);
        // verify we get decisions for all the shards
        assertEquals(shards.size(), allDecisions.size());
        assertEquals(shards, new ArrayList<>(allDecisions.keySet()));
        assertEquals(AllocationDecision.YES, allDecisions.get(shard).getAllocationDecision());
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
        HashMap<ShardRouting, AllocateUnassignedDecision> allDecisions = batchAllocator.makeAllocationDecision(shards, allocation, logger);
        // verify we get decisions for all the shards
        assertEquals(shards.size(), allDecisions.size());
        assertEquals(new HashSet<>(shards), allDecisions.keySet());
        for (ShardRouting shard : shards) {
            assertEquals(AllocationDecision.YES, allDecisions.get(shard).getAllocationDecision());
        }
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
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, null, null, System.nanoTime());
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
        return new RoutingAllocation(deciders, new RoutingNodes(state, false), state, null, null, System.nanoTime());
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
            Map<ShardId, TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard> shardData = Map.of(
                shardId,
                new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard(
                    allocationId,
                    primary,
                    replicationCheckpoint,
                    storeException
                )
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
            Map<ShardId, TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard> shardData = new HashMap<>();
            shardData.put(
                shardId,
                new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard(
                    allocationId,
                    primary,
                    replicationCheckpoint,
                    storeException
                )
            );
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
