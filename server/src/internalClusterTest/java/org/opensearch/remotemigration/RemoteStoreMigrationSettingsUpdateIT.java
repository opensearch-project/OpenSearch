/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Optional;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.index.IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMigrationSettingsUpdateIT extends RemoteStoreMigrationShardAllocationBaseTestCase {

    private Client client;

    // remote store backed index setting tests

    public void testNewIndexIsRemoteStoreBackedForRemoteStoreDirectionAndMixedMode() {
        logger.info("Initialize cluster: gives non remote cluster manager");
        initializeCluster(false);

        String indexName1 = "test_index_1";
        String indexName2 = "test_index_2";

        logger.info("Add non-remote node");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        assertNodeInCluster(nonRemoteNodeName);

        logger.info("Create an index");
        prepareIndexWithoutReplica(Optional.of(indexName1));

        logger.info("Verify that non remote-backed index is created");
        assertNonRemoteStoreBackedIndex(indexName1);

        logger.info("Set mixed cluster compatibility mode and remote_store direction");
        setClusterMode(MIXED.mode);
        setDirection(REMOTE_STORE.direction);

        logger.info("Add remote node");
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        assertNodeInCluster(remoteNodeName);

        logger.info("Create another index");
        prepareIndexWithoutReplica(Optional.of(indexName2));

        logger.info("Verify that remote backed index is created");
        assertRemoteStoreBackedIndex(indexName2);
    }

    public void testNewRestoredIndexIsRemoteStoreBackedForRemoteStoreDirectionAndMixedMode() throws Exception {
        logger.info("Initialize cluster: gives non remote cluster manager");
        initializeCluster(false);

        logger.info("Add remote and non-remote nodes");
        setClusterMode(MIXED.mode);
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        assertNodeInCluster(nonRemoteNodeName);
        assertNodeInCluster(remoteNodeName);

        logger.info("Create a non remote-backed index");
        client.admin()
            .indices()
            .prepareCreate(TEST_INDEX)
            .setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            )
            .get();

        logger.info("Verify that non remote stored backed index is created");
        assertNonRemoteStoreBackedIndex(TEST_INDEX);

        logger.info("Create repository");
        String snapshotName = "test-snapshot";
        String snapshotRepoName = "test-restore-snapshot-repo";
        Path snapshotRepoNameAbsolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository(snapshotRepoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", snapshotRepoNameAbsolutePath))
        );

        logger.info("Create snapshot of non remote stored backed index");

        SnapshotInfo snapshotInfo = client().admin()
            .cluster()
            .prepareCreateSnapshot(snapshotRepoName, snapshotName)
            .setIndices(TEST_INDEX)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();

        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertTrue(snapshotInfo.successfulShards() > 0);
        assertEquals(0, snapshotInfo.failedShards());

        logger.info("Restore index from snapshot under NONE direction");
        String restoredIndexName1 = TEST_INDEX + "-restored1";
        restoreSnapshot(snapshotRepoName, snapshotName, restoredIndexName1);

        logger.info("Verify that restored index is non remote-backed");
        assertNonRemoteStoreBackedIndex(restoredIndexName1);

        logger.info("Restore index from snapshot under REMOTE_STORE direction");
        setDirection(REMOTE_STORE.direction);
        String restoredIndexName2 = TEST_INDEX + "-restored2";
        restoreSnapshot(snapshotRepoName, snapshotName, restoredIndexName2);

        logger.info("Verify that restored index is non remote-backed");
        assertRemoteStoreBackedIndex(restoredIndexName2);
    }

    // restore indices from a snapshot
    private void restoreSnapshot(String snapshotRepoName, String snapshotName, String restoredIndexName) {
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(false)
            .setIndices(TEST_INDEX)
            .setRenamePattern(TEST_INDEX)
            .setRenameReplacement(restoredIndexName)
            .get();

        assertEquals(restoreSnapshotResponse.status(), RestStatus.ACCEPTED);
        ensureGreen(restoredIndexName);
    }

    // verify that the created index is not remote store backed
    private void assertNonRemoteStoreBackedIndex(String indexName) {
        Settings indexSettings = client.admin().indices().prepareGetIndex().execute().actionGet().getSettings().get(indexName);
        assertEquals(ReplicationType.DOCUMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertNull(indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertNull(indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
    }

    // verify that the created index is remote store backed
    private void assertRemoteStoreBackedIndex(String indexName) {
        Settings indexSettings = client.admin().indices().prepareGetIndex().execute().actionGet().getSettings().get(indexName);
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals("true", indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(REPOSITORY_NAME, indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(REPOSITORY_2_NAME, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals(
            IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
            INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(indexSettings)
        );
    }

    // bootstrap a cluster
    private void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().startClusterManagerOnlyNode();
        client = internalCluster().client();
    }

}
