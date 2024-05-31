/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_STORE_CUSTOM_KEY;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.mockito.Mockito.mock;

public class RemoteMigrationIndexMetadataUpdaterTests extends OpenSearchTestCase {
    private final String indexName = "test-index";

    public void testMaybeAddRemoteIndexSettingsAllPrimariesAndReplicasOnRemote() throws IOException {
        Metadata metadata = createIndexMetadataWithDocrepSettings(indexName);
        IndexMetadata existingIndexMetadata = metadata.index(indexName);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(existingIndexMetadata);
        long currentSettingsVersion = indexMetadataBuilder.settingsVersion();
        DiscoveryNode primaryNode = IndexShardTestUtils.getFakeRemoteEnabledNode("1");
        DiscoveryNode replicaNode = IndexShardTestUtils.getFakeRemoteEnabledNode("2");
        DiscoveryNodes allNodes = DiscoveryNodes.builder().add(primaryNode).add(replicaNode).build();
        RoutingTable routingTable = createRoutingTableAllShardsStarted(indexName, 1, 1, primaryNode, replicaNode);
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            allNodes,
            routingTable,
            existingIndexMetadata,
            metadata.settings(),
            logger
        );
        migrationIndexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexName);
        assertTrue(currentSettingsVersion < indexMetadataBuilder.settingsVersion());
        assertRemoteSettingsApplied(indexMetadataBuilder.build());
    }

    public void testMaybeAddRemoteIndexSettingsDoesNotRunWhenSettingsAlreadyPresent() throws IOException {
        Metadata metadata = createIndexMetadataWithRemoteStoreSettings(indexName);
        IndexMetadata existingIndexMetadata = metadata.index(indexName);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(existingIndexMetadata);
        long currentSettingsVersion = indexMetadataBuilder.settingsVersion();
        DiscoveryNode primaryNode = IndexShardTestUtils.getFakeRemoteEnabledNode("1");
        DiscoveryNode replicaNode = IndexShardTestUtils.getFakeRemoteEnabledNode("2");
        DiscoveryNodes allNodes = DiscoveryNodes.builder().add(primaryNode).add(replicaNode).build();
        RoutingTable routingTable = createRoutingTableAllShardsStarted(indexName, 1, 1, primaryNode, replicaNode);
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            allNodes,
            routingTable,
            existingIndexMetadata,
            metadata.settings(),
            logger
        );
        migrationIndexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexName);
        assertEquals(currentSettingsVersion, indexMetadataBuilder.settingsVersion());
    }

    public void testMaybeAddRemoteIndexSettingsDoesNotUpdateSettingsWhenAllShardsInDocrep() throws IOException {
        Metadata metadata = createIndexMetadataWithDocrepSettings(indexName);
        IndexMetadata existingIndexMetadata = metadata.index(indexName);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(existingIndexMetadata);
        long currentSettingsVersion = indexMetadataBuilder.settingsVersion();
        DiscoveryNode primaryNode = IndexShardTestUtils.getFakeDiscoNode("1");
        DiscoveryNode replicaNode = IndexShardTestUtils.getFakeDiscoNode("2");
        DiscoveryNodes allNodes = DiscoveryNodes.builder().add(primaryNode).add(replicaNode).build();
        RoutingTable routingTable = createRoutingTableAllShardsStarted(indexName, 1, 1, primaryNode, replicaNode);
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            allNodes,
            routingTable,
            existingIndexMetadata,
            metadata.settings(),
            logger
        );
        migrationIndexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexName);
        assertEquals(currentSettingsVersion, indexMetadataBuilder.settingsVersion());
        assertDocrepSettingsApplied(indexMetadataBuilder.build());
    }

    public void testMaybeAddRemoteIndexSettingsUpdatesIndexSettingsWithUnassignedReplicas() throws IOException {
        Metadata metadata = createIndexMetadataWithDocrepSettings(indexName);
        IndexMetadata existingIndexMetadata = metadata.index(indexName);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(existingIndexMetadata);
        long currentSettingsVersion = indexMetadataBuilder.settingsVersion();
        DiscoveryNode primaryNode = IndexShardTestUtils.getFakeRemoteEnabledNode("1");
        DiscoveryNode replicaNode = IndexShardTestUtils.getFakeDiscoNode("2");
        DiscoveryNodes allNodes = DiscoveryNodes.builder().add(primaryNode).add(replicaNode).build();
        RoutingTable routingTable = createRoutingTableReplicasUnassigned(indexName, 1, 1, primaryNode);
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            allNodes,
            routingTable,
            existingIndexMetadata,
            metadata.settings(),
            logger
        );
        migrationIndexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexName);
        assertTrue(currentSettingsVersion < indexMetadataBuilder.settingsVersion());
        assertRemoteSettingsApplied(indexMetadataBuilder.build());
    }

    public void testMaybeAddRemoteIndexSettingsDoesNotUpdateIndexSettingsWithRelocatingReplicas() throws IOException {
        Metadata metadata = createIndexMetadataWithDocrepSettings(indexName);
        IndexMetadata existingIndexMetadata = metadata.index(indexName);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(existingIndexMetadata);
        long currentSettingsVersion = indexMetadataBuilder.settingsVersion();
        DiscoveryNode primaryNode = IndexShardTestUtils.getFakeRemoteEnabledNode("1");
        DiscoveryNode replicaNode = IndexShardTestUtils.getFakeDiscoNode("2");
        DiscoveryNode replicaRelocatingNode = IndexShardTestUtils.getFakeDiscoNode("3");
        DiscoveryNodes allNodes = DiscoveryNodes.builder().add(primaryNode).add(replicaNode).build();
        RoutingTable routingTable = createRoutingTableReplicasRelocating(indexName, 1, 1, primaryNode, replicaNode, replicaRelocatingNode);
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            allNodes,
            routingTable,
            existingIndexMetadata,
            metadata.settings(),
            logger
        );
        migrationIndexMetadataUpdater.maybeAddRemoteIndexSettings(indexMetadataBuilder, indexName);
        assertEquals(currentSettingsVersion, indexMetadataBuilder.settingsVersion());
        assertDocrepSettingsApplied(indexMetadataBuilder.build());
    }

    public void testMaybeUpdateRemoteStorePathStrategyExecutes() {
        Metadata currentMetadata = createIndexMetadataWithDocrepSettings(indexName);
        IndexMetadata existingIndexMetadata = currentMetadata.index(indexName);
        IndexMetadata.Builder builder = IndexMetadata.builder(existingIndexMetadata);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeRemoteEnabledNode("1")).build();
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            discoveryNodes,
            mock(RoutingTable.class),
            existingIndexMetadata,
            Settings.builder()
                .put(
                    CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(),
                    RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.name()
                )
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_PREFIX.name())
                .build(),
            logger
        );
        migrationIndexMetadataUpdater.maybeUpdateRemoteStoreCustomMetadata(builder, indexName);
        assertCustomPathMetadataIsPresent(builder.build());
    }

    public void testMaybeUpdateRemoteStorePathStrategyDoesNotExecute() {
        Metadata currentMetadata = createIndexMetadataWithRemoteStoreSettings(indexName);
        IndexMetadata existingIndexMetadata = currentMetadata.index(indexName);
        IndexMetadata.Builder builder = IndexMetadata.builder(currentMetadata.index(indexName));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeRemoteEnabledNode("1")).build();
        RemoteMigrationIndexMetadataUpdater migrationIndexMetadataUpdater = new RemoteMigrationIndexMetadataUpdater(
            discoveryNodes,
            mock(RoutingTable.class),
            existingIndexMetadata,
            Settings.builder()
                .put(
                    CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.getKey(),
                    RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.name()
                )
                .put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), RemoteStoreEnums.PathType.HASHED_PREFIX.name())
                .build(),
            logger
        );

        migrationIndexMetadataUpdater.maybeUpdateRemoteStoreCustomMetadata(builder, indexName);

        assertCustomPathMetadataIsPresent(builder.build());
    }

    private RoutingTable createRoutingTableAllShardsStarted(
        String indexName,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNode primaryHostingNode,
        DiscoveryNode replicaHostingNode
    ) {
        RoutingTable.Builder builder = RoutingTable.builder();
        Index index = new Index(indexName, UUID.randomUUID().toString());

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId(index, i);
            IndexShardRoutingTable.Builder indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingTable.addShard(
                TestShardRouting.newShardRouting(shardId, primaryHostingNode.getId(), true, ShardRoutingState.STARTED)
            );
            for (int j = 0; j < numberOfReplicas; j++) {
                indexShardRoutingTable.addShard(
                    TestShardRouting.newShardRouting(shardId, replicaHostingNode.getId(), false, ShardRoutingState.STARTED)
                );
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable.build());
        }
        return builder.add(indexRoutingTableBuilder.build()).build();
    }

    private RoutingTable createRoutingTableReplicasUnassigned(
        String indexName,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNode primaryHostingNode
    ) {
        RoutingTable.Builder builder = RoutingTable.builder();
        Index index = new Index(indexName, UUID.randomUUID().toString());

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId(index, i);
            IndexShardRoutingTable.Builder indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingTable.addShard(
                TestShardRouting.newShardRouting(shardId, primaryHostingNode.getId(), true, ShardRoutingState.STARTED)
            );
            for (int j = 0; j < numberOfReplicas; j++) {
                indexShardRoutingTable.addShard(
                    ShardRouting.newUnassigned(
                        shardId,
                        false,
                        RecoverySource.PeerRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                    )
                );
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable.build());
        }
        return builder.add(indexRoutingTableBuilder.build()).build();
    }

    private RoutingTable createRoutingTableReplicasRelocating(
        String indexName,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNode primaryHostingNodes,
        DiscoveryNode replicaHostingNode,
        DiscoveryNode replicaRelocatingNode
    ) {
        RoutingTable.Builder builder = RoutingTable.builder();
        Index index = new Index(indexName, UUID.randomUUID().toString());

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId(index, i);
            IndexShardRoutingTable.Builder indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingTable.addShard(
                TestShardRouting.newShardRouting(shardId, primaryHostingNodes.getId(), true, ShardRoutingState.STARTED)
            );
            for (int j = 0; j < numberOfReplicas; j++) {
                indexShardRoutingTable.addShard(
                    TestShardRouting.newShardRouting(
                        shardId,
                        replicaHostingNode.getId(),
                        replicaRelocatingNode.getId(),
                        false,
                        ShardRoutingState.RELOCATING
                    )
                );
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable.build());
        }
        return builder.add(indexRoutingTableBuilder.build()).build();
    }

    public static Metadata createIndexMetadataWithRemoteStoreSettings(String indexName) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
                .put(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), "dummy-tlog-repo")
                .put(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.getKey(), "dummy-segment-repo")
                .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT")
                .build()
        )
            .putCustom(
                REMOTE_STORE_CUSTOM_KEY,
                Map.of(
                    RemoteStoreEnums.PathType.NAME,
                    "dummy",
                    RemoteStoreEnums.PathHashAlgorithm.NAME,
                    "dummy",
                    IndexMetadata.TRANSLOG_METADATA_KEY,
                    "dummy"
                )
            )
            .build();
        return Metadata.builder().put(indexMetadata).build();
    }

    public static Metadata createIndexMetadataWithDocrepSettings(String indexName) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "DOCUMENT")
                .build()
        ).build();
        return Metadata.builder().put(indexMetadata).build();
    }

    private void assertRemoteSettingsApplied(IndexMetadata indexMetadata) {
        assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings()));
        assertTrue(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertTrue(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertEquals(ReplicationType.SEGMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexMetadata.getSettings()));
    }

    private void assertDocrepSettingsApplied(IndexMetadata indexMetadata) {
        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings()));
        assertFalse(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertFalse(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertEquals(ReplicationType.DOCUMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexMetadata.getSettings()));
    }

    private void assertCustomPathMetadataIsPresent(IndexMetadata indexMetadata) {
        assertNotNull(indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY));
        assertNotNull(indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY).get(RemoteStoreEnums.PathType.NAME));
        assertNotNull(indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY).get(RemoteStoreEnums.PathHashAlgorithm.NAME));
    }
}
