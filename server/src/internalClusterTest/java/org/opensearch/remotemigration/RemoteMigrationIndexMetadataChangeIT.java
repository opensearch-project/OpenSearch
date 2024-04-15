/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_STORE_CUSTOM_KEY;
import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_STORE_SEEDED_SHARDS_KEY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteMigrationIndexMetadataChangeIT extends MigrationBaseTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(InternalSettingsPlugin.class, MockFsRepositoryPlugin.class, MockTransportService.TestPlugin.class)
        ).collect(Collectors.toList());
    }

    public void testIndexSettingsUpdateAfterIndexMovedToRemoteThroughAllocationExclude() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        addRemote = false;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        String indexName = "migration-index-allocation-exclude";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAndAssertDocrepProperties(indexName, oneReplica);
        String replicationType;
        GetSettingsResponse response;
        String remoteStoreEnabled;

        initDocRepToRemoteMigration();
        addRemote = true;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        excludeNodeSet("type", "docrep");
        waitForRelocation();
        waitNoPendingTasksOnAll();

        response = internalCluster().client().admin().indices().prepareGetSettings(indexName).get();
        remoteStoreEnabled = response.getSetting(indexName, SETTING_REMOTE_STORE_ENABLED);
        replicationType = response.getSetting(indexName, SETTING_REPLICATION_TYPE);
        assertEquals(remoteStoreEnabled, "true");
        assertEquals(replicationType, "SEGMENT");
    }

    public void testIndexSettingsUpdateAfterIndexMovedToRemoteThroughManualReroute() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        List<String> docrepNodeNames = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        String indexName1 = "migration-index-manual-reroute-1";
        String indexName2 = "migration-index-manual-reroute-2";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAndAssertDocrepProperties(indexName1, oneReplica);
        createIndexAndAssertDocrepProperties(indexName2, oneReplica);

        initDocRepToRemoteMigration();
        stopShardRebalancing();

        addRemote = true;
        List<String> remoteNodeNames = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        String primaryNode = primaryNodeName(indexName1);
        String replicaNode = docrepNodeNames.stream()
            .filter(nodeName -> nodeName.equals(primaryNodeName(indexName1)) == false)
            .collect(Collectors.toList())
            .get(0);

        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName1, 0, primaryNode, remoteNodeNames.get(0)))
                .execute()
                .actionGet()
        );
        waitForRelocation();

        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName1, 0, replicaNode, remoteNodeNames.get(1)))
                .execute()
                .actionGet()
        );
        waitForRelocation();

        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName2, 0, primaryNodeName(indexName2), remoteNodeNames.get(0)))
                .execute()
                .actionGet()
        );
        waitForRelocation();
        waitNoPendingTasksOnAll();

        assertRemoteProperties(indexName1);
        assertDocrepProperties(indexName2);
    }

    public void testIndexSettingsUpdatedOnlyForMigratingIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        addRemote = false;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        String indexName = "migration-index";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        indexBulk(indexName, 100);
        refresh(indexName);
        ensureGreen(indexName);

        assertDocrepProperties(indexName);
        long initalMetadataVersion = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(indexName)
            .getVersion();

        initDocRepToRemoteMigration();
        addRemote = true;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        excludeNodeSet("type", "docrep");

        waitForRelocation();
        waitNoPendingTasksOnAll();
        assertRemoteProperties(indexName);
        assertTrue(
            initalMetadataVersion < internalCluster().client()
                .admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index(indexName)
                .getVersion()
        );

        String secondIndex = "remote-index";
        createIndex(
            secondIndex,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        indexBulk(secondIndex, 100);
        refresh(secondIndex);
        ensureGreen(secondIndex);

        waitNoPendingTasksOnAll();

        assertRemoteProperties(secondIndex);
        initalMetadataVersion = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(secondIndex)
            .getVersion();

        assertEquals(
            initalMetadataVersion,
            internalCluster().client().admin().cluster().prepareState().get().getState().metadata().index(secondIndex).getVersion()
        );
    }

    @TestLogging(reason = "", value = "org.opensearch.cluster.metadata:TRACE,org.opensearch.cluster.action.shard:TRACE")
    public void testCustomSeedingMetadata() throws Exception {
        String indexName = "custom-seeding-metadata-index";
        internalCluster().startClusterManagerOnlyNode();
        List<String> docRepNodeNames = internalCluster().startNodes(2, Settings.builder().put("node.attr._type", "docrep").build());

        // create index with 2 primaries
        Settings zeroReplica = Settings.builder().put("number_of_replicas", 0).put("number_of_shards", 4).build();
        createIndex(indexName, zeroReplica);
        indexBulk(indexName, 100);
        ensureGreen(indexName);

        stopShardRebalancing();
        initDocRepToRemoteMigration();
        // add remote node in mixed mode cluster
        addRemote = true;
        List<String> remoteNodeNames = internalCluster().startNodes(2, Settings.builder().put("node.attr._type", "remote").build());
        internalCluster().validateClusterFormed();

        assertEquals(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareGetRepositories(REPOSITORY_NAME, REPOSITORY_2_NAME)
                .get()
                .repositories()
                .size(),
            2
        );

        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName, 0, primaryNodeName(indexName, 0), remoteNodeNames.get(0)))
                .add(new MoveAllocationCommand(indexName, 1, primaryNodeName(indexName, 1), remoteNodeNames.get(0)))
                .add(new MoveAllocationCommand(indexName, 2, primaryNodeName(indexName, 2), remoteNodeNames.get(1)))
                .execute()
                .actionGet()
        );
        waitForRelocation();
        ensureGreen(indexName);
        waitNoPendingTasksOnAll();

        ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        HashSet<String> seededShards = new HashSet<>(
            Strings.commaDelimitedListToSet(
                indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY).get(IndexMetadata.REMOTE_STORE_SEEDED_SHARDS_KEY)
            )
        );
        HashSet<String> expectedSeededShards = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            expectedSeededShards.add("[" + indexName + "][" + i + "]");
        }
        assertEquals(expectedSeededShards, seededShards);

        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(indexName, 3, primaryNodeName(indexName, 3), remoteNodeNames.get(1)))
                .execute()
                .actionGet()
        );
        waitForRelocation();
        ensureGreen(indexName);
        waitNoPendingTasksOnAll();

        clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
        indexMetadata = clusterState.metadata().index(indexName);
        Map<String, String> currentCustomData = indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY);
        assertFalse(currentCustomData.containsKey(REMOTE_STORE_SEEDED_SHARDS_KEY));
        assertTrue(
            currentCustomData.containsKey(RemoteStoreEnums.PathType.NAME)
                && currentCustomData.containsKey(RemoteStoreEnums.PathHashAlgorithm.NAME)
        );
    }

    private void createIndexAndAssertDocrepProperties(String index, Settings settings) throws Exception {
        createIndex(index, settings);
        indexBulk(index, 100);
        refresh(index);
        ensureGreen(index);
        assertDocrepProperties(index);
    }

    private void assertDocrepProperties(String index) {
        GetSettingsResponse response = internalCluster().client().admin().indices().prepareGetSettings(index).get();
        String remoteStoreEnabled = response.getSetting(index, SETTING_REMOTE_STORE_ENABLED);
        String replicationType = response.getSetting(index, SETTING_REPLICATION_TYPE);
        assertNull(remoteStoreEnabled);
        assertEquals(replicationType, "DOCUMENT");
    }

    private void assertRemoteProperties(String index) {
        GetSettingsResponse response = internalCluster().client().admin().indices().prepareGetSettings(index).get();
        String remoteStoreEnabled = response.getSetting(index, SETTING_REMOTE_STORE_ENABLED);
        String replicationType = response.getSetting(index, SETTING_REPLICATION_TYPE);
        assertEquals(remoteStoreEnabled, "true");
        assertEquals(replicationType, "SEGMENT");
    }

    private void excludeNodeSet(String attr, String value) {
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._" + attr, value))
                .get()
        );
    }

    private void stopShardRebalancing() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none").build())
                .get()
        );
    }
}
