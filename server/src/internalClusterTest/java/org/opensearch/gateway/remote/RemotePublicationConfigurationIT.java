/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotemigration.MigrationBaseTestCase;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collection;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests the compatibility between types of nodes based on the configured repositories
 *  Non Remote node [No Repositories configured]
 *  Remote Publish Configured Node [Cluster State + Routing Table]
 *  Remote Node [Cluster State + Segment + Translog]
 *  Remote Node With Routing Table [Cluster State + Segment + Translog + Routing Table]
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePublicationConfigurationIT extends MigrationBaseTestCase {
    private final String REMOTE_PRI_DOCREP_REP = "remote-primary-docrep-replica";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /* Adding the following mock plugins:
        - InternalSettingsPlugin : To override default intervals of retention lease and global ckp sync
        - MockFsRepositoryPlugin and MockTransportService.TestPlugin: To ensure remote interactions are not no-op and retention leases are properly propagated
         */
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(InternalSettingsPlugin.class, MockFsRepositoryPlugin.class, MockTransportService.TestPlugin.class)
        ).collect(Collectors.toList());
    }

    @Before
    public void setUp() throws Exception {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        super.setUp();
    }

    public Settings.Builder remotePublishConfiguredNodeSetting() {
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            REPOSITORY_NAME
        );
        String prefixModeVerificationSuffix = BlobStoreRepository.PREFIX_MODE_VERIFICATION_SETTING.getKey();
        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            REPOSITORY_NAME
        );
        String routingTableRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            ROUTING_TABLE_REPO_NAME
        );
        String routingTableRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            ROUTING_TABLE_REPO_NAME
        );

        Settings.Builder builder = Settings.builder()
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .put(stateRepoTypeAttributeKey, ReloadableFsRepository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", segmentRepoPath)
            .put(stateRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, true)
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, ROUTING_TABLE_REPO_NAME)
            .put(routingTableRepoTypeAttributeKey, ReloadableFsRepository.TYPE)
            .put(routingTableRepoSettingsAttributeKeyPrefix + "location", segmentRepoPath);
        return builder;
    }

    public Settings.Builder remoteWithRoutingTableNodeSetting() {
        // Remote Cluster with Routing table
        return Settings.builder()
            .put(
                buildRemoteStoreNodeAttributes(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    false
                )
            )
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true);
    }

    public void testRemoteClusterStateServiceNotInitialized_WhenNodeAttributesNotPresent() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        ensureStableCluster(3);
        ensureGreen();

        internalCluster().getDataOrClusterManagerNodeInstances(RemoteClusterStateService.class).forEach(Assert::assertNull);
    }

    public void testServiceInitialized_WhenNodeAttributesPresent() {
        internalCluster().startClusterManagerOnlyNode(
            buildRemoteStateNodeAttributes(REPOSITORY_NAME, segmentRepoPath, ReloadableFsRepository.TYPE)
        );
        internalCluster().startDataOnlyNodes(
            2,
            buildRemoteStateNodeAttributes(REPOSITORY_NAME, segmentRepoPath, ReloadableFsRepository.TYPE)
        );

        ensureStableCluster(3);
        ensureGreen();

        internalCluster().getDataOrClusterManagerNodeInstances(RemoteClusterStateService.class).forEach(Assert::assertNotNull);
    }

    public void testRemotePublishConfigNodeJoinNonRemoteCluster() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        Settings.Builder build = remotePublishConfiguredNodeSetting();
        internalCluster().startClusterManagerOnlyNode(build.build());
        internalCluster().startDataOnlyNodes(2, build.build());

        ensureStableCluster(6);
        ensureGreen();
    }

    public void testRemotePublishConfigNodeJoinRemoteCluster() throws Exception {
        // Remote Cluster without Routing table
        setAddRemote(true);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        setAddRemote(false);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        Settings.Builder build = remotePublishConfiguredNodeSetting();
        internalCluster().startClusterManagerOnlyNode(build.build());
        ensureStableCluster(4);
        ensureGreen();
    }

    public void testRemoteNodeWithRoutingTableJoinRemoteCluster() throws Exception {
        setAddRemote(true);
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        setAddRemote(false);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Remote Repo with Routing table
        Settings settings = remoteWithRoutingTableNodeSetting().build();
        internalCluster().startClusterManagerOnlyNode(settings);
        ensureStableCluster(4);
        ensureGreen();
    }

    public void testNonRemoteNodeJoinRemoteWithRoutingCluster() throws Exception {
        Settings settings = remoteWithRoutingTableNodeSetting().build();
        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        internalCluster().startClusterManagerOnlyNode();
        ensureStableCluster(4);
        ensureGreen();
    }

    public void testRemotePublishConfigNodeJoinRemoteWithRoutingCluster() throws Exception {
        Settings settings = remoteWithRoutingTableNodeSetting().build();
        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        internalCluster().startClusterManagerOnlyNode(remotePublishConfiguredNodeSetting().build());

        ensureStableCluster(4);
        ensureGreen();
    }

    public void testNonRemoteNodeJoiningPublishConfigCluster() throws Exception {
        Settings.Builder build = remotePublishConfiguredNodeSetting();
        internalCluster().startClusterManagerOnlyNode(build.build());
        internalCluster().startDataOnlyNodes(2, build.build());

        internalCluster().startClusterManagerOnlyNode();

        ensureStableCluster(4);
        ensureGreen();
    }

    public void testRemoteNodeJoiningPublishConfigCluster() throws Exception {
        Settings.Builder build = remotePublishConfiguredNodeSetting();
        internalCluster().startClusterManagerOnlyNode(build.build());
        internalCluster().startDataOnlyNodes(2, build.build());

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        setAddRemote(true);
        internalCluster().startClusterManagerOnlyNode();
        ensureStableCluster(4);
        ensureGreen();
    }

    public void testRemoteNodeWithRoutingTableJoiningPublishConfigCluster() throws Exception {
        Settings.Builder build = remotePublishConfiguredNodeSetting();
        internalCluster().startClusterManagerOnlyNode(build.build());
        internalCluster().startDataOnlyNodes(2, build.build());

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );

        Settings settings = Settings.builder()
            .put(
                buildRemoteStoreNodeAttributes(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    ROUTING_TABLE_REPO_NAME,
                    segmentRepoPath,
                    false
                )
            )
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        internalCluster().startClusterManagerOnlyNode(settings);

        ensureStableCluster(4);
        ensureGreen();
    }
}
