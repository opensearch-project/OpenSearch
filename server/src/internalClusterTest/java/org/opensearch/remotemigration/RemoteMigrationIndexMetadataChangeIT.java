/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
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

        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(indexName);
        asyncIndexingService.startIndexing();

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
        asyncIndexingService.stopIndexing();

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

        AsyncIndexingService indexOne = new AsyncIndexingService(indexName1);
        indexOne.startIndexing();

        AsyncIndexingService indexTwo = new AsyncIndexingService(indexName2);
        indexTwo.startIndexing();

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

        indexOne.stopIndexing();
        indexTwo.stopIndexing();

        assertRemoteProperties(indexName1);
        assertDocrepProperties(indexName2);
    }

    public void testIndexSettingsUpdatedOnlyForMigratingIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();

        addRemote = false;
        internalCluster().startDataOnlyNodes(2, Settings.builder().put("node.attr._type", "docrep").build());
        internalCluster().validateClusterFormed();

        String indexName = "migration-index";
        Settings oneReplica = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        createIndexAndAssertDocrepProperties(indexName, oneReplica);

        AsyncIndexingService indexingService = new AsyncIndexingService(indexName);
        indexingService.startIndexing();

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
        indexingService.stopIndexing();

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

    private void createIndexAndAssertDocrepProperties(String index, Settings settings) {
        createIndex(index, settings);
        refresh(index);
        ensureGreen(index);
        assertDocrepProperties(index);
    }

    private void assertDocrepProperties(String index) {
        GetSettingsResponse response = internalCluster().client().admin().indices().prepareGetSettings(index).get();
        String remoteStoreEnabled = response.getSetting(index, SETTING_REMOTE_STORE_ENABLED);
        String replicationType = response.getSetting(index, SETTING_REPLICATION_TYPE);
        String segmentRepo = response.getSetting(index, SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
        String translogRepo = response.getSetting(index, SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY);
        assertNull(remoteStoreEnabled);
        assertNull(segmentRepo);
        assertNull(translogRepo);
        assertEquals(replicationType, "DOCUMENT");
    }

    private void assertRemoteProperties(String index) {
        GetSettingsResponse response = internalCluster().client().admin().indices().prepareGetSettings(index).get();
        String remoteStoreEnabled = response.getSetting(index, SETTING_REMOTE_STORE_ENABLED);
        String replicationType = response.getSetting(index, SETTING_REPLICATION_TYPE);
        String segmentRepo = response.getSetting(index, SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
        String translogRepo = response.getSetting(index, SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY);
        assertEquals(remoteStoreEnabled, "true");
        assertNotNull(segmentRepo);
        assertNotNull(translogRepo);
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
