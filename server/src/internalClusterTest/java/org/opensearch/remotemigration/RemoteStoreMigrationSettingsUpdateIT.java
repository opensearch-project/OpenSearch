/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.STRICT;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMigrationSettingsUpdateIT extends RemoteStoreMigrationShardAllocationBaseTestCase {

    private Client client;
    private String nonRemoteNodeName;
    private String remoteNodeName;

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
        createIndex(TEST_INDEX, 0);

        logger.info("Verify that non remote stored backed index is created");
        assertNonRemoteStoreBackedIndex(TEST_INDEX);

        logger.info("Create repository");
        String snapshotName = "test-snapshot";
        String snapshotRepoName = "test-restore-snapshot-repo";
        Path snapshotRepoNameAbsolutePath = randomRepoPath().toAbsolutePath();
        createRepository(snapshotRepoName, "fs", Settings.builder().put("location", snapshotRepoNameAbsolutePath));

        logger.info("Create snapshot of non remote stored backed index");

        createSnapshot(snapshotRepoName, snapshotName, TEST_INDEX);

        logger.info("Restore index from snapshot under NONE direction");
        String restoredIndexName1 = TEST_INDEX + "-restored1";
        restoreSnapshot(snapshotRepoName, snapshotName, restoredIndexName1);
        ensureGreen(restoredIndexName1);

        logger.info("Verify that restored index is non remote-backed");
        assertNonRemoteStoreBackedIndex(restoredIndexName1);

        logger.info("Restore index from snapshot under REMOTE_STORE direction");
        setDirection(REMOTE_STORE.direction);
        String restoredIndexName2 = TEST_INDEX + "-restored2";
        restoreSnapshot(snapshotRepoName, snapshotName, restoredIndexName2);
        ensureGreen(restoredIndexName2);

        logger.info("Verify that restored index is non remote-backed");
        assertRemoteStoreBackedIndex(restoredIndexName2);
    }

    // compatibility mode setting test

    public void testSwitchToStrictMode() throws Exception {
        createMixedModeCluster();

        logger.info("Attempt switching to strict mode");
        SettingsException exception = assertThrows(SettingsException.class, () -> setClusterMode(STRICT.mode));
        assertEquals(
            "can not switch to STRICT compatibility mode when the cluster contains both remote and non-remote nodes",
            exception.getMessage()
        );

        stopRemoteNode();

        logger.info("Attempt switching to strict mode");
        setClusterMode(STRICT.mode);
    }

    public void testClearCompatibilityModeSetting() throws Exception {
        createMixedModeCluster();
        stopRemoteNode();

        logger.info("Attempt clearing compatibility mode");
        clearClusterMode();
    }

    private void stopRemoteNode() throws IOException {
        logger.info("Stop remote node so that cluster had only non-remote nodes");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(remoteNodeName));
        ensureStableCluster(2);
    }

    private void createMixedModeCluster() {
        logger.info("Initialize cluster");
        initializeCluster(false);

        logger.info("Create a mixed mode cluster");
        setClusterMode(MIXED.mode);
        addRemote = true;
        remoteNodeName = internalCluster().startNode();
        addRemote = false;
        nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        assertNodeInCluster(remoteNodeName);
        assertNodeInCluster(nonRemoteNodeName);
    }

    // bootstrap a cluster
    private void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().startClusterManagerOnlyNode();
        client = internalCluster().client();
    }

}
