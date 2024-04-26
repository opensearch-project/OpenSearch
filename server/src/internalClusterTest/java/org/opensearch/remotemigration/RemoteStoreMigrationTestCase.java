/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.List;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationTestCase extends MigrationBaseTestCase {
    public void testMixedModeAddRemoteNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        Client client = internalCluster().client(cmNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // add remote node in mixed mode cluster
        addRemote = true;
        internalCluster().startNode();
        internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // assert repo gets registered
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().getRepositories(gr).actionGet();
        assertEquals(1, getRepositoriesResponse.repositories().size());

        // add docrep mode in mixed mode cluster
        addRemote = true;
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
}
