/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStatePublicationIT extends RemoteStoreBaseIntegTestCase {

    private static String INDEX_NAME = "test-index";

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String routingTableRepoName = "remote-routing-repo";
        String routingTableRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            routingTableRepoName
        );
        String routingTableRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            routingTableRepoName
        );
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, routingTableRepoName)
            .put(routingTableRepoTypeAttributeKey, ReloadableFsRepository.TYPE)
            .put(routingTableRepoSettingsAttributeKeyPrefix + "location", segmentRepoPath)
            .build();
    }

    public void testMasterReElectionUsesIncrementalUpload() throws IOException {
        prepareCluster(3, 2, INDEX_NAME, 1, 1);
        ensureStableCluster(5);
        ensureGreen(INDEX_NAME);
        // update settings on a random node
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "10mb").build()
                    )
                )
                .actionGet()
        );
        PersistedStateRegistry persistedStateRegistry = internalCluster().getClusterManagerNodeInstance(PersistedStateRegistry.class);
        GatewayMetaState.RemotePersistedState remotePersistedState = (GatewayMetaState.RemotePersistedState) persistedStateRegistry
            .getPersistedState(PersistedStateRegistry.PersistedStateType.REMOTE);
        ClusterMetadataManifest manifest = remotePersistedState.getLastAcceptedManifest();
        // force elected master to step down
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(4);

        persistedStateRegistry = internalCluster().getClusterManagerNodeInstance(PersistedStateRegistry.class);
        GatewayMetaState.RemotePersistedState persistedStateAfterElection = (GatewayMetaState.RemotePersistedState) persistedStateRegistry
            .getPersistedState(PersistedStateRegistry.PersistedStateType.REMOTE);
        ClusterMetadataManifest manifestAfterElection = persistedStateAfterElection.getLastAcceptedManifest();

        // coordination metadata is updated, it will be unequal
        assertNotEquals(manifest.getCoordinationMetadata(), manifestAfterElection.getCoordinationMetadata());
        // all other attributes are not uploaded again and will be pointing to same files in manifest after new master is elected
        assertEquals(manifest.getClusterUUID(), manifestAfterElection.getClusterUUID());
        assertEquals(manifest.getIndices(), manifestAfterElection.getIndices());
        assertEquals(manifest.getSettingsMetadata(), manifestAfterElection.getSettingsMetadata());
        assertEquals(manifest.getTemplatesMetadata(), manifestAfterElection.getTemplatesMetadata());
        assertEquals(manifest.getCustomMetadataMap(), manifestAfterElection.getCustomMetadataMap());
        assertEquals(manifest.getRoutingTableVersion(), manifest.getRoutingTableVersion());
        assertEquals(manifest.getIndicesRouting(), manifestAfterElection.getIndicesRouting());
    }
}
