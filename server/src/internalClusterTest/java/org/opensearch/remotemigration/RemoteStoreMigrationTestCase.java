/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationTestCase extends MigrationBaseTestCase {
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    protected int minimumNumberOfReplicas() {
        return 1;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }

    public void testMixedModeAddRemoteNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        Client client = internalCluster().client(cmNodes.get(0));
        initDocRepToRemoteMigration();

        // add remote node in mixed mode cluster
        setAddRemote(true);
        internalCluster().startNode();
        internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // assert repo gets registered
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().getRepositories(gr).actionGet();
        assertEquals(1, getRepositoriesResponse.repositories().size());

        // add docrep mode in mixed mode cluster
        setAddRemote(true);
        internalCluster().startNode();
        assertBusy(() -> {
            assertEquals(client.admin().cluster().prepareClusterStats().get().getNodes().size(), internalCluster().getNodeNames().length);
        });

        // add incompatible remote node in remote mixed cluster
        Settings.Builder badSettings = Settings.builder()
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, "REPOSITORY_2_NAME", translogRepoPath))
            .put("discovery.initial_state_timeout", "500ms");
        String badNode = internalCluster().startNode(badSettings);
        assertTrue(client.admin().cluster().prepareClusterStats().get().getNodes().size() < internalCluster().getNodeNames().length);
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(badNode));
    }

    public void testMigrationDirections() {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        // add remote node in docrep cluster
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "docrep"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "random"));
        assertThrows(IllegalArgumentException.class, () -> client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    public void testNoShallowSnapshotInMixedMode() throws Exception {
        logger.info("Initialize remote cluster");
        addRemote = true;
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        Client client = internalCluster().client(cmNodes.get(0));

        logger.info("Add remote node");
        internalCluster().startNode();
        internalCluster().validateClusterFormed();

        logger.info("Create remote backed index");
        RemoteStoreMigrationShardAllocationBaseTestCase.createIndex("test", 0);
        RemoteStoreMigrationShardAllocationBaseTestCase.assertRemoteStoreBackedIndex("test");

        logger.info("Create shallow snapshot setting enabled repo");
        String shallowSnapshotRepoName = "shallow-snapshot-repo-name";
        Path shallowSnapshotRepoPath = randomRepoPath();
        assertAcked(
            clusterAdmin().preparePutRepository(shallowSnapshotRepoName)
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", shallowSnapshotRepoPath)
                        .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
                )
        );

        logger.info("Verify shallow snapshot creation");
        final String snapshot1 = "snapshot1";
        SnapshotInfo snapshotInfo1 = RemoteStoreMigrationShardAllocationBaseTestCase.createSnapshot(
            shallowSnapshotRepoName,
            snapshot1,
            "test"
        );
        assertEquals(snapshotInfo1.isRemoteStoreIndexShallowCopyEnabled(), true);

        logger.info("Set MIXED compatibility mode");
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        logger.info("Verify that new snapshot is not shallow");
        final String snapshot2 = "snapshot2";
        SnapshotInfo snapshotInfo2 = RemoteStoreMigrationShardAllocationBaseTestCase.createSnapshot(shallowSnapshotRepoName, snapshot2);
        assertEquals(snapshotInfo2.isRemoteStoreIndexShallowCopyEnabled(), false);
    }

    /*
    Tests end to end remote migration via Blue Green mechanism
    - Starts docrep nodes with multiple nodes, indices, replicas copies
    - Adds remote nodes to cluster
    - Excludes docrep nodes.
    - Asserts all shards are migrated to remote store
    - Asserts doc count across all shards
    - Continuos indexing with refresh/flush happening
     */
    public void testEndToEndRemoteMigration() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> docRepNodes = internalCluster().startNodes(2);
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        logger.info("---> Starting doc ingestion in parallel thread");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService("test");
        asyncIndexingService.startIndexing();

        setAddRemote(true);

        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        internalCluster().startNodes(2);

        assertAcked(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings()
                .setIndices("test")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put("index.routing.allocation.exclude._name", String.join(",", docRepNodes))
                        .build()
                )
                .get()
        );

        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(45))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertTrue(clusterHealthResponse.getRelocatingShards() == 0);
        logger.info("---> Stopping indexing thread");
        asyncIndexingService.stopIndexing();
        Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
        assertThat("node0 has 0 shards", shardCountByNodeId.get(docRepNodes.get(0)), equalTo(null));
        assertThat("node1 has 0 shards", shardCountByNodeId.get(docRepNodes.get(1)), equalTo(null));
        refresh("test");
        waitForReplication("test");
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test").setTrackTotalHits(true).get(),
            asyncIndexingService.getIndexedDocs()
        );
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test")
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                .get(),
            asyncIndexingService.getIndexedDocs()
        );
    }
}
