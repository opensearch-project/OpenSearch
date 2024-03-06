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

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.RemoteStoreMigrationAllocationDecider;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.hamcrest.core.Is.is;

public class RemoteStoreMigrationAllocationDeciderTests extends OpenSearchAllocationTestCase {

    private final static String TEST_INDEX = "test_index";
    private final static String TEST_REPO = "test_repo";

    private final static String REMOTE_STORE_DIRECTION = "remote_store";
    private final static String DOCREP_DIRECTION = "docrep";
    private final static String NONE_DIRECTION = "none";

    private final static String STRICT_MODE = "strict";
    private final static String MIXED_MODE = "mixed";

    private final Settings directionEnabledNodeSettings = Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();

    private final Settings strictModeCompatibilitySettings = Settings.builder()
        .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
        .build();
    private final Settings mixedModeCompatibilitySettings = Settings.builder()
        .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
        .build();

    private final Settings remoteStoreDirectionSettings = Settings.builder()
        .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
        .build();
    private final Settings docrepDirectionSettings = Settings.builder()
        .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.DOCREP)
        .build();

    // tests for primary shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreBackedIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, 1, 0);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        DiscoveryNode remoteNode = getRemoteNode();
        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        ClusterState clusterState = getInitialClusterState(customSettings, indexMetadataBuilder, discoveryNodes);

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(primaryShardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        String reason = "[remote_store migration_direction]: primary shard copy can not be allocated to a non-remote node";
        if (isRemoteStoreBackedIndex) {
            reason =
                "[remote_store migration_direction]: primary shard copy can not be allocated to a non-remote node because a remote store backed index's shard copy can only be allocated to a remote node";
        }
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForMixedModeAndRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreBackedIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, 1, 0);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        DiscoveryNode remoteNode = getRemoteNode();
        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        ClusterState clusterState = getInitialClusterState(customSettings, indexMetadataBuilder, discoveryNodes);

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(primaryShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is("[remote_store migration_direction]: primary shard copy can be allocated to a remote node")
        );
    }

    // tests for replica shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreBackedIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());

        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            // primary on non-remote node
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                nonRemoteNode.getId(),
                                true,
                                ShardRoutingState.STARTED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is(
                "[remote_store migration_direction]: replica shard copy can not be allocated to a remote node since primary shard copy is not yet migrated to remote"
            )
        );
    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForMixedModeAndRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreBackedIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode remoteNode1 = getRemoteNode();
        assertTrue(remoteNode1.isRemoteStoreNode());
        DiscoveryNode remoteNode2 = getRemoteNode();
        assertTrue(remoteNode2.isRemoteStoreNode());
        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            // primary on remote node
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                remoteNode1.getId(),
                                true,
                                ShardRoutingState.STARTED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is(
                "[remote_store migration_direction]: replica shard copy can be allocated to a remote node since primary shard copy has been migrated to remote"
            )
        );
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreBackedIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());
        DiscoveryNode nonRemoteNode1 = getNonRemoteNode();
        assertFalse(nonRemoteNode1.isRemoteStoreNode());
        DiscoveryNode nonRemoteNode2 = getNonRemoteNode();
        assertFalse(nonRemoteNode2.isRemoteStoreNode());

        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            // primary shard on non-remote node
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                nonRemoteNode1.getId(),
                                true,
                                ShardRoutingState.STARTED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, nonRemoteRoutingNode, routingAllocation);
        Decision.Type type = Decision.Type.YES;
        String reason = "[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node";
        if (isRemoteStoreBackedIndex) {
            type = Decision.Type.NO;
            reason =
                "[remote_store migration_direction]: replica shard copy can not be allocated to a non-remote node because a remote store backed index's shard copy can only be allocated to a remote node";
        }
        assertThat(decision.type(), is(type));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreBackedIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());
        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());

        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            // primary shard on non-remote node
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                remoteNode.getId(),
                                true,
                                ShardRoutingState.STARTED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, nonRemoteRoutingNode, routingAllocation);
        Decision.Type type = Decision.Type.YES;
        String reason = "[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node";
        if (isRemoteStoreBackedIndex) {
            type = Decision.Type.NO;
            reason =
                "[remote_store migration_direction]: replica shard copy can not be allocated to a non-remote node because a remote store backed index's shard copy can only be allocated to a remote node";
        }
        assertThat(decision.type(), is(type));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    // tests for STRICT mode

    public void testAlwaysAllocateNewPrimaryShardForStrictMode() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(false, 1, 0);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, STRICT_MODE, indexMetadataBuilder);

        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(nonRemoteNode).localNodeId(nonRemoteNode.getId()).build();

        ClusterState clusterState = getInitialClusterState(customSettings, indexMetadataBuilder, discoveryNodes);

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(primaryShardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is("[remote_store migration_direction]: primary shard copy can be allocated to a non-remote node for strict compatibility mode")
        );

        indexMetadataBuilder = getIndexMetadataBuilder(true, 1, 0);
        customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, STRICT_MODE, indexMetadataBuilder);

        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());

        discoveryNodes = DiscoveryNodes.builder().add(remoteNode).localNodeId(remoteNode.getId()).build();

        clusterState = getInitialClusterState(customSettings, indexMetadataBuilder, discoveryNodes);

        primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        decision = remoteStoreMigrationAllocationDecider.canAllocate(primaryShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is("[remote_store migration_direction]: primary shard copy can be allocated to a remote node for strict compatibility mode")
        );
    }

    public void testAlwaysAllocateNewReplicaShardForStrictMode() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(false, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, STRICT_MODE, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode nonRemoteNode1 = getNonRemoteNode();
        assertFalse(nonRemoteNode1.isRemoteStoreNode());
        DiscoveryNode nonRemoteNode2 = getNonRemoteNode();
        assertFalse(nonRemoteNode2.isRemoteStoreNode());

        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                nonRemoteNode1.getId(),
                                true,
                                ShardRoutingState.STARTED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is("[remote_store migration_direction]: replica shard copy can be allocated to a non-remote node for strict compatibility mode")
        );

        indexMetadataBuilder = getIndexMetadataBuilder(true, 1, 1);
        customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, STRICT_MODE, indexMetadataBuilder);

        DiscoveryNode remoteNode1 = getRemoteNode();
        assertTrue(remoteNode1.isRemoteStoreNode());
        DiscoveryNode remoteNode2 = getRemoteNode();
        assertTrue(remoteNode2.isRemoteStoreNode());

        routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                remoteNode1.getId(),
                                true,
                                ShardRoutingState.STARTED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .build();

        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

        remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is("[remote_store migration_direction]: replica shard copy can be allocated to a remote node for strict compatibility mode")
        );
    }

    // edge case

    public void testDontAllocateReplicaIfPrimaryNotFound() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(false, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, MIXED_MODE, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());

        Metadata metadata = Metadata.builder().put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            // unassigned primary shard
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                null,
                                true,
                                ShardRoutingState.UNASSIGNED
                            )
                        )
                            .addShard(
                                // new replica's allocation
                                TestShardRouting.newShardRouting(
                                    shardId.getIndexName(),
                                    shardId.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.UNASSIGNED
                                )
                            )
                            .build()
                    )
            )
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(remoteNode).localNodeId(remoteNode.getId()).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider = new RemoteStoreMigrationAllocationDecider(
            customSettings,
            getClusterSettings(customSettings)
        );

        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(remoteStoreMigrationAllocationDecider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(replicaShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is(
                "[remote_store migration_direction]: replica shard copy can not be allocated to a remote node since primary shard for this replica is not yet active"
            )
        );
    }

    // prepare index metadata for test-index
    private IndexMetadata.Builder getIndexMetadataBuilder(boolean isRemoteStoreBackedIndex, int shardCount, int replicaCount) {
        return IndexMetadata.builder(TEST_INDEX)
            .settings(
                settings(Version.CURRENT).put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, TEST_REPO)
                    .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, TEST_REPO)
                    .put(SETTING_REMOTE_STORE_ENABLED, isRemoteStoreBackedIndex)
                    .build()
            )
            .numberOfShards(shardCount)
            .numberOfReplicas(replicaCount);
    }

    // get node-level settings
    private Settings getCustomSettings(String direction, String compatibilityMode, IndexMetadata.Builder indexMetadataBuilder) {
        Settings.Builder builder = Settings.builder();
        // direction settings
        if (direction.toLowerCase(Locale.ROOT).equals(REMOTE_STORE_DIRECTION)) {
            builder.put(remoteStoreDirectionSettings);
        } else if (direction.toLowerCase(Locale.ROOT).equals(DOCREP_DIRECTION)) {
            builder.put(docrepDirectionSettings);
        }

        // compatibility mode settings
        if (compatibilityMode.toLowerCase(Locale.ROOT).equals(STRICT_MODE)) {
            builder.put(strictModeCompatibilitySettings);
        } else if (compatibilityMode.toLowerCase(Locale.ROOT).equals(MIXED_MODE)) {
            builder.put(mixedModeCompatibilitySettings);
        }

        // index metadata settings
        builder.put(indexMetadataBuilder.build().getSettings());

        builder.put(directionEnabledNodeSettings);

        return builder.build();
    }

    private String getRandomCompatibilityMode() {
        return randomFrom(STRICT_MODE, MIXED_MODE);
    }

    private ClusterSettings getClusterSettings(Settings settings) {
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    private ClusterState getInitialClusterState(
        Settings settings,
        IndexMetadata.Builder indexMetadataBuilder,
        DiscoveryNodes discoveryNodes
    ) {
        Metadata metadata = Metadata.builder().persistentSettings(settings).put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(indexMetadataBuilder.build())
            .addAsNew(metadata.index(TEST_INDEX))
            .build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(discoveryNodes).build();
    }

    // get a dummy non-remote node
    private DiscoveryNode getNonRemoteNode() {
        return new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
    }

    // get a dummy remote node
    public DiscoveryNode getRemoteNode() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_VALUE"
        );
        return new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }
}
