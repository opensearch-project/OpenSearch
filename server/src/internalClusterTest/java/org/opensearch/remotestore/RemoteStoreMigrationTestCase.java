/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteStoreMigrationTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;

    static boolean addRemote = false;

    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (addRemote) {
            logger.info("Adding remote store node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        } else {
            logger.info("Adding docrep node");
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("discovery.initial_state_timeout", "500ms").build();
        }
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }

    public void testAddRemoteNode() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        Client client = internalCluster().client(cmNodes.get(0));
        addRemote = true;
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        internalCluster().startNode();
        internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // add incompatible remote node in remote mixed cluster
        Settings.Builder badSettings = Settings.builder()
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, "REPOSITORY_2_NAME", translogRepoPath))
            .put("discovery.initial_state_timeout", "500ms");
        String badNode = internalCluster().startNode(badSettings);
        assertTrue(client.admin().cluster().prepareClusterStats().get().getNodes().size() < internalCluster().getNodeNames().length);
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(badNode));

        // add remote node in docrep cluster
        updateSettingsRequest.persistentSettings(Settings.builder().put(DIRECTION_SETTING.getKey(), "docrep"));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        String wrongNode = internalCluster().startNode();
        assertTrue(client.admin().cluster().prepareClusterStats().get().getNodes().size() < internalCluster().getNodeNames().length);
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(wrongNode));
    }

    public void testAddDocRepNode() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        addRemote = false;
        Client client = internalCluster().client(cmNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
                .put(DIRECTION_SETTING.getKey(), "remote_store")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        String wrongNode = internalCluster().startNode();
        String[] allNodes = internalCluster().getNodeNames();
        assertTrue(client.admin().cluster().prepareClusterStats().get().getNodes().size() < allNodes.length);

        updateSettingsRequest.persistentSettings(Settings.builder().put(DIRECTION_SETTING.getKey(), "docrep"));
        assertAcked(client.admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        assertBusy(() -> assertTrue(client.admin().cluster().prepareClusterStats().get().getNodes().size() == allNodes.length));
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(wrongNode));
    }

}
