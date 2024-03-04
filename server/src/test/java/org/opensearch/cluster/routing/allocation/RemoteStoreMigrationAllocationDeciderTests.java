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

    // tests for primary shard copy allocation with REMOTE_STORE direction

    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        String compatibilityMode = getRandomCompatibilityMode();
        boolean isRemoteStoreEnabledIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreEnabledIndex, 1, 0);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, compatibilityMode, indexMetadataBuilder);

        ClusterState clusterState = getInitialClusterState(customSettings, indexMetadataBuilder);
        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        assertFalse(nonRemoteNode.isRemoteStoreNode());

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(nonRemoteNode).localNodeId(nonRemoteNode.getId()).build())
            .build();

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

        Decision.Type type = Decision.Type.NO;
        String reason = "[remote_store migration_direction]: primary shard copy can not be allocated to a non_remote_store node";
        if (compatibilityMode.equals(STRICT_MODE)) {
            type = Decision.Type.YES;
            reason =
                "[remote_store migration_direction]: primary shard copy can be allocated to a non_remote_store node for strict compatibility mode";
        } else if (isRemoteStoreEnabledIndex) {
            reason =
                "[remote_store migration_direction]: primary shard copy can not be allocated to a non_remote_store node because a remote_store_enabled index's shard copy can only move towards a remote_store node";
        }
        assertThat(decision.type(), is(type));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    public void testAllocateNewPrimaryShardOnRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        String compatibilityMode = getRandomCompatibilityMode();
        boolean isRemoteStoreEnabledIndex = randomBoolean();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreEnabledIndex, 1, 0);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, compatibilityMode, indexMetadataBuilder);

        ClusterState clusterState = getInitialClusterState(customSettings, indexMetadataBuilder);
        DiscoveryNode remoteNode = getRemoteNode();
        assertTrue(remoteNode.isRemoteStoreNode());

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(remoteNode).localNodeId(remoteNode.getId()).build())
            .build();

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
        String reason = "[remote_store migration_direction]: primary shard copy can be allocated to a remote_store node";
        if (compatibilityMode.equals(STRICT_MODE)) {
            reason =
                "[remote_store migration_direction]: primary shard copy can be allocated to a remote_store node for strict compatibility mode";
        }
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    // tests for replica shard copy allocation with REMOTE_STORE direction

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        String compatibilityMode = getRandomCompatibilityMode();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreEnabledIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, compatibilityMode, indexMetadataBuilder);

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
        Decision.Type type = Decision.Type.NO;
        String reason =
            "[remote_store migration_direction]: replica shard copy can not be allocated to a remote_store node since primary shard copy is not yet migrated to remote";
        if (compatibilityMode.equals(STRICT_MODE)) {
            type = Decision.Type.YES;
            reason =
                "[remote_store migration_direction]: replica shard copy can be allocated to a remote_store node for strict compatibility mode";
        }
        assertThat(decision.type(), is(type));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    public void testAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        String compatibilityMode = getRandomCompatibilityMode();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreEnabledIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, compatibilityMode, indexMetadataBuilder);

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        DiscoveryNode remoteNode1 = getRemoteNode();
        assertTrue(remoteNode1.isRemoteStoreNode());
        DiscoveryNode remoteNode2 = getRemoteNode();
        assertTrue(remoteNode2.isRemoteStoreNode());

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
        String reason =
            "[remote_store migration_direction]: replica shard copy can be allocated to a remote_store node since primary shard copy has been migrated to remote";
        if (compatibilityMode.equals(STRICT_MODE)) {
            reason =
                "[remote_store migration_direction]: replica shard copy can be allocated to a remote_store node for strict compatibility mode";
        }
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnNonRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        String compatibilityMode = getRandomCompatibilityMode();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreEnabledIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, compatibilityMode, indexMetadataBuilder);

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
        String reason = "[remote_store migration_direction]: replica shard copy can be allocated to a non_remote_store node";
        if (compatibilityMode.equals(STRICT_MODE)) {
            reason =
                "[remote_store migration_direction]: replica shard copy can be allocated to a non_remote_store node for strict compatibility mode";
        } else if (isRemoteStoreEnabledIndex) {
            type = Decision.Type.NO;
            reason =
                "[remote_store migration_direction]: replica shard copy can not be allocated to a non_remote_store node because a remote_store_enabled index's shard copy can only move towards a remote_store node";
        }
        assertThat(decision.type(), is(type));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    public void testAllocateNewReplicaShardOnNonRemoteNodeIfPrimaryShardOnRemoteNodeForRemoteStoreDirection() {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);

        boolean isRemoteStoreEnabledIndex = randomBoolean();
        String compatibilityMode = getRandomCompatibilityMode();
        IndexMetadata.Builder indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreEnabledIndex, 1, 1);
        Settings customSettings = getCustomSettings(REMOTE_STORE_DIRECTION, compatibilityMode, indexMetadataBuilder);

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
        String reason = "[remote_store migration_direction]: replica shard copy can be allocated to a non_remote_store node";
        if (compatibilityMode.equals(STRICT_MODE)) {
            reason =
                "[remote_store migration_direction]: replica shard copy can be allocated to a non_remote_store node for strict compatibility mode";
        } else if (isRemoteStoreEnabledIndex) {
            type = Decision.Type.NO;
            reason =
                "[remote_store migration_direction]: replica shard copy can not be allocated to a non_remote_store node because a remote_store_enabled index's shard copy can only move towards a remote_store node";
        }
        assertThat(decision.type(), is(type));
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    // prepare index metadata for test-index
    private IndexMetadata.Builder getIndexMetadataBuilder(boolean isRemoteStoreEnabledIndex, int shardCount, int replicaCount) {
        return IndexMetadata.builder(TEST_INDEX)
            .settings(
                settings(Version.CURRENT).put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, TEST_REPO)
                    .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, TEST_REPO)
                    .put(SETTING_REMOTE_STORE_ENABLED, isRemoteStoreEnabledIndex)
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

    private ClusterState getInitialClusterState(Settings settings, IndexMetadata.Builder indexMetadataBuilder) {
        Metadata metadata = Metadata.builder().persistentSettings(settings).put(indexMetadataBuilder).build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(indexMetadataBuilder.build())
            .addAsNew(metadata.index(TEST_INDEX))
            .build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
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
