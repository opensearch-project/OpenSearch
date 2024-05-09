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
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.NONE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.hamcrest.core.Is.is;

public class RemoteStoreMigrationAllocationDeciderTests extends OpenSearchAllocationTestCase {

    private final static String TEST_INDEX = "test_index";
    private final static String TEST_REPO = "test_repo";

    private final Settings directionEnabledNodeSettings = Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();

    private final Settings strictModeCompatibilitySettings = Settings.builder()
        .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
        .build();
    private final Settings mixedModeCompatibilitySettings = Settings.builder()
        .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
        .build();

    private final Settings remoteStoreDirectionSettings = Settings.builder()
        .put(MIGRATION_DIRECTION_SETTING.getKey(), REMOTE_STORE)
        .build();
    private final Settings docrepDirectionSettings = Settings.builder()
        .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.DOCREP)
        .build();

    private Boolean isRemoteStoreBackedIndex = null, isMixedMode;
    private int shardCount, replicaCount;
    private IndexMetadata.Builder indexMetadataBuilder;
    private Settings customSettings;
    private DiscoveryNodes discoveryNodes;
    private ClusterState clusterState;
    private RemoteStoreMigrationAllocationDecider remoteStoreMigrationAllocationDecider;
    private RoutingAllocation routingAllocation;
    private Metadata metadata;
    private RoutingTable routingTable = null;

    private ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

    private void beforeAllocation(String direction) {
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);
        if (isRemoteStoreBackedIndex == null) {
            isRemoteStoreBackedIndex = randomBoolean();
        }
        indexMetadataBuilder = getIndexMetadataBuilder(isRemoteStoreBackedIndex, shardCount, replicaCount);

        String compatibilityMode = isMixedMode
            ? RemoteStoreNodeService.CompatibilityMode.MIXED.mode
            : RemoteStoreNodeService.CompatibilityMode.STRICT.mode;
        customSettings = getCustomSettings(direction, compatibilityMode, indexMetadataBuilder);

        if (routingTable != null) {
            metadata = Metadata.builder().put(indexMetadataBuilder).build();
            clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(discoveryNodes)
                .build();
        } else {
            clusterState = getInitialClusterState(customSettings, indexMetadataBuilder, discoveryNodes);
        }

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
    }

    private void prepareRoutingTable(boolean isReplicaAllocation, String primaryShardNodeId) {
        routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(
                            TestShardRouting.newShardRouting(
                                shardId.getIndexName(),
                                shardId.getId(),
                                (isReplicaAllocation ? primaryShardNodeId : null),
                                true,
                                (isReplicaAllocation ? ShardRoutingState.STARTED : ShardRoutingState.UNASSIGNED)
                            )
                        )
                            .addShard(
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
    }

    // tests for primary shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testDontAllocateNewPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() {
        shardCount = 1;
        replicaCount = 0;
        isMixedMode = true;

        DiscoveryNode remoteNode = getRemoteNode();
        DiscoveryNode nonRemoteNode = getNonRemoteNode();

        discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode.getId());

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
        shardCount = 1;
        replicaCount = 0;
        isMixedMode = true;

        DiscoveryNode remoteNode = getRemoteNode();
        DiscoveryNode nonRemoteNode = getNonRemoteNode();

        discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        ShardRouting primaryShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(primaryShardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation().toLowerCase(Locale.ROOT),
            is("[remote_store migration_direction]: primary shard copy can be allocated to a remote node")
        );
    }

    // tests for replica shard copy allocation with MIXED mode and REMOTE_STORE direction

    public void testDontAllocateNewReplicaShardOnRemoteNodeIfPrimaryShardOnNonRemoteNodeForMixedModeAndRemoteStoreDirection() {
        shardCount = 1;
        replicaCount = 1;
        isMixedMode = true;

        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        DiscoveryNode remoteNode = getRemoteNode();

        // primary on non-remote node, new replica's allocation
        prepareRoutingTable(true, nonRemoteNode.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode.getId());

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
        shardCount = 1;
        replicaCount = 1;
        isMixedMode = true;

        DiscoveryNode remoteNode1 = getRemoteNode();
        DiscoveryNode remoteNode2 = getRemoteNode();
        DiscoveryNode nonRemoteNode = getNonRemoteNode();

        // primary on remote node, new replica's allocation
        prepareRoutingTable(true, remoteNode1.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

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
        shardCount = 1;
        replicaCount = 1;
        isMixedMode = true;

        DiscoveryNode remoteNode = getRemoteNode();
        DiscoveryNode nonRemoteNode1 = getNonRemoteNode();
        DiscoveryNode nonRemoteNode2 = getNonRemoteNode();

        // primary shard on non-remote node, new replica's allocation
        prepareRoutingTable(true, nonRemoteNode1.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());

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
        shardCount = 1;
        replicaCount = 1;
        isMixedMode = true;

        DiscoveryNode nonRemoteNode = getNonRemoteNode();
        DiscoveryNode remoteNode = getRemoteNode();

        // primary shard on remote node, new replica's allocation
        prepareRoutingTable(true, remoteNode.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode)
            .localNodeId(nonRemoteNode.getId())
            .add(remoteNode)
            .localNodeId(remoteNode.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        assertEquals(2, clusterState.getRoutingTable().allShards().size());
        ShardRouting replicaShardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode.getId());

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

    // test for STRICT mode

    public void testAlwaysAllocateNewShardForStrictMode() {
        shardCount = 1;
        replicaCount = 1;
        isMixedMode = false;
        isRemoteStoreBackedIndex = false;

        DiscoveryNode nonRemoteNode1 = getNonRemoteNode();
        DiscoveryNode nonRemoteNode2 = getNonRemoteNode();

        boolean isReplicaAllocation = randomBoolean();

        prepareRoutingTable(isReplicaAllocation, nonRemoteNode1.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        if (isReplicaAllocation) {
            shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        }
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());

        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(shardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        String reason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can be allocated to a non-remote node for strict compatibility mode",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));

        isRemoteStoreBackedIndex = true;

        DiscoveryNode remoteNode1 = getRemoteNode();
        DiscoveryNode remoteNode2 = getRemoteNode();

        prepareRoutingTable(isReplicaAllocation, remoteNode1.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .build();

        beforeAllocation(REMOTE_STORE.direction);

        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        if (isReplicaAllocation) {
            shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        }
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

        decision = remoteStoreMigrationAllocationDecider.canAllocate(shardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        reason = String.format(
            Locale.ROOT,
            "[remote_store migration_direction]: %s shard copy can be allocated to a remote node for strict compatibility mode",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    // test for NONE direction
    public void testAllocationForNoneDirection() {
        shardCount = 1;
        replicaCount = 1;
        isMixedMode = true;
        isRemoteStoreBackedIndex = false; // non-remote store backed index

        DiscoveryNode remoteNode1 = getRemoteNode();
        DiscoveryNode remoteNode2 = getRemoteNode();
        DiscoveryNode nonRemoteNode1 = getNonRemoteNode();
        DiscoveryNode nonRemoteNode2 = getNonRemoteNode();

        boolean isReplicaAllocation = randomBoolean();

        prepareRoutingTable(isReplicaAllocation, nonRemoteNode1.getId());

        discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();

        beforeAllocation(NONE.direction);
        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        ShardRouting shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        if (isReplicaAllocation) {
            shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        }
        RoutingNode nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());
        RoutingNode remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

        // allocation decision for non-remote node for non-remote store backed index
        Decision decision = remoteStoreMigrationAllocationDecider.canAllocate(shardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        String reason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can be allocated to a non-remote node for non remote store backed index",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));

        // allocation decision for remote node for non-remote store backed index
        decision = remoteStoreMigrationAllocationDecider.canAllocate(shardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        reason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can not be allocated to a remote node for non remote store backed index",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));

        isRemoteStoreBackedIndex = true; // remote store backed index
        prepareRoutingTable(isReplicaAllocation, remoteNode1.getId());

        beforeAllocation(NONE.direction);
        assertEquals(2, clusterState.getRoutingTable().allShards().size());

        shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).primaryShard();
        if (isReplicaAllocation) {
            shardRouting = clusterState.getRoutingTable().shardRoutingTable(TEST_INDEX, 0).replicaShards().get(0);
        }
        nonRemoteRoutingNode = clusterState.getRoutingNodes().node(nonRemoteNode2.getId());
        remoteRoutingNode = clusterState.getRoutingNodes().node(remoteNode2.getId());

        // allocation decision for non-remote node for remote store backed index
        decision = remoteStoreMigrationAllocationDecider.canAllocate(shardRouting, nonRemoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        reason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can not be allocated to a non-remote node for remote store backed index",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));

        // allocation decision for remote node for remote store backed index
        decision = remoteStoreMigrationAllocationDecider.canAllocate(shardRouting, remoteRoutingNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        reason = String.format(
            Locale.ROOT,
            "[none migration_direction]: %s shard copy can be allocated to a remote node for remote store backed index",
            (isReplicaAllocation ? "replica" : "primary")
        );
        assertThat(decision.getExplanation().toLowerCase(Locale.ROOT), is(reason));
    }

    // prepare index metadata for test-index
    private IndexMetadata.Builder getIndexMetadataBuilder(boolean isRemoteStoreBackedIndex, int shardCount, int replicaCount) {
        Settings.Builder builder = settings(Version.CURRENT);
        if (isRemoteStoreBackedIndex) {
            builder.put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, TEST_REPO)
                .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, TEST_REPO)
                .put(SETTING_REMOTE_STORE_ENABLED, true);
        }
        return IndexMetadata.builder(TEST_INDEX).settings(builder).numberOfShards(shardCount).numberOfReplicas(replicaCount);
    }

    // get node-level settings
    private Settings getCustomSettings(String direction, String compatibilityMode, IndexMetadata.Builder indexMetadataBuilder) {
        Settings.Builder builder = Settings.builder();
        // direction settings
        if (direction.toLowerCase(Locale.ROOT).equals(REMOTE_STORE.direction)) {
            builder.put(remoteStoreDirectionSettings);
        } else if (direction.toLowerCase(Locale.ROOT).equals(RemoteStoreNodeService.Direction.DOCREP.direction)) {
            builder.put(docrepDirectionSettings);
        }

        // compatibility mode settings
        if (compatibilityMode.toLowerCase(Locale.ROOT).equals(RemoteStoreNodeService.CompatibilityMode.STRICT.mode)) {
            builder.put(strictModeCompatibilitySettings);
        } else if (compatibilityMode.toLowerCase(Locale.ROOT).equals(RemoteStoreNodeService.CompatibilityMode.MIXED.mode)) {
            builder.put(mixedModeCompatibilitySettings);
        }

        // index metadata settings
        builder.put(indexMetadataBuilder.build().getSettings());

        builder.put(directionEnabledNodeSettings);

        return builder.build();
    }

    private String getRandomCompatibilityMode() {
        return randomFrom(RemoteStoreNodeService.CompatibilityMode.STRICT.mode, RemoteStoreNodeService.CompatibilityMode.MIXED.mode);
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
    private DiscoveryNode getRemoteNode() {
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
