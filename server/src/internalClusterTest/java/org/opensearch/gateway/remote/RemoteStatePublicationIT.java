/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.applicationtemplates.SystemTemplatesService;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RemoteStatePublicationIT extends RemoteStoreBaseIntegTestCase {

    private static String INDEX_NAME = "test-index";
    private boolean isRemoteStateEnabled = true;
    private String isRemotePublicationEnabled = "true";

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
        isRemoteStateEnabled = true;
        isRemotePublicationEnabled = "true";
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES, Boolean.TRUE.toString())
            .put(SystemTemplatesService.SETTING_APPLICATION_BASED_CONFIGURATION_TEMPLATES_ENABLED.getKey(), Boolean.TRUE.toString())
            .put(FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL, isRemotePublicationEnabled)
            .build();
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
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), isRemoteStateEnabled)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, routingTableRepoName)
            .put(routingTableRepoTypeAttributeKey, ReloadableFsRepository.TYPE)
            .put(routingTableRepoSettingsAttributeKeyPrefix + "location", segmentRepoPath)
            .build();
    }

    public void testPublication() throws Exception {
        // create cluster with multi node (3 master + 2 data)
        prepareClusterWithDefaultContext(3, 2, INDEX_NAME, 1, 2);
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

        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REPOSITORY_NAME);

        Map<String, Integer> globalMetadataFiles = getMetadataFiles(repository, RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN);

        assertTrue(globalMetadataFiles.containsKey(COORDINATION_METADATA));
        assertTrue(globalMetadataFiles.containsKey(SETTING_METADATA));
        assertTrue(globalMetadataFiles.containsKey(TRANSIENT_SETTING_METADATA));
        assertTrue(globalMetadataFiles.containsKey(TEMPLATES_METADATA));
        assertTrue(globalMetadataFiles.keySet().stream().anyMatch(key -> key.startsWith(CUSTOM_METADATA)));

        Map<String, Integer> ephemeralMetadataFiles = getMetadataFiles(
            repository,
            RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN
        );

        assertTrue(ephemeralMetadataFiles.containsKey(CLUSTER_BLOCKS));
        assertTrue(ephemeralMetadataFiles.containsKey(DISCOVERY_NODES));

        Map<String, Integer> manifestFiles = getMetadataFiles(repository, RemoteClusterMetadataManifest.MANIFEST);
        assertTrue(manifestFiles.containsKey(RemoteClusterMetadataManifest.MANIFEST));

        // get settings from each node and verify that it is updated
        Settings settings = clusterService().getSettings();
        logger.info("settings : {}", settings);
        for (Client client : clients()) {
            ClusterStateResponse response = client.admin().cluster().prepareState().clear()
                .setMetadata(true)
                .setLocal(true)
                .get();

            String refreshSetting = response.getState()
                .metadata()
                .settings()
                .get(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey());

            assertEquals("10mb", refreshSetting);


            // Verify context is present in metadata
            assertEquals(new Context(CONTEXT_NAME), response.getState()
                .metadata().indices().get(INDEX_NAME).context());
        }
    }

    public void testRemotePublicationDisableIfRemoteStateDisabled() {
        // only disable remote state
        isRemoteStateEnabled = false;
        // create cluster with multi node with in-consistent settings
        prepareCluster(3, 2, INDEX_NAME, 1, 2);
        // assert cluster is stable, ensuring publication falls back to legacy transport with inconsistent settings
        ensureStableCluster(5);
        ensureGreen(INDEX_NAME);

        assertNull(internalCluster().getCurrentClusterManagerNodeInstance(RemoteClusterStateService.class));
    }

    public void testRemotePublicationDownloadStats() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        String dataNode = internalCluster().getDataNodeNames().stream().collect(Collectors.toList()).get(0);

        NodesStatsResponse nodesStatsResponseDataNode = client().admin()
            .cluster()
            .prepareNodesStats(dataNode)
            .addMetric(NodesStatsRequest.Metric.DISCOVERY.metricName())
            .get();

        assertDataNodeDownloadStats(nodesStatsResponseDataNode);

    }

    private void assertDataNodeDownloadStats(NodesStatsResponse nodesStatsResponse) {
        // assert cluster state stats for data node
        DiscoveryStats dataNodeDiscoveryStats = nodesStatsResponse.getNodes().get(0).getDiscoveryStats();
        assertNotNull(dataNodeDiscoveryStats.getClusterStateStats());
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getUpdateSuccess());
        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(0).getSuccessCount() > 0);
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(0).getFailedCount());
        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(0).getTotalTimeInMillis() > 0);

        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(1).getSuccessCount() > 0);
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(1).getFailedCount());
        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(1).getTotalTimeInMillis() > 0);
    }

    private Map<String, Integer> getMetadataFiles(BlobStoreRepository repository, String subDirectory) throws IOException {
        BlobPath metadataPath = repository.basePath()
            .add(
                Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(getClusterState().getClusterName().value().getBytes(StandardCharsets.UTF_8))
            )
            .add(RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN)
            .add(getClusterState().metadata().clusterUUID())
            .add(subDirectory);
        return repository.blobStore().blobContainer(metadataPath).listBlobs().keySet().stream().map(fileName -> {
            logger.info(fileName);
            return fileName.split(DELIMITER)[0];
        }).collect(Collectors.toMap(Function.identity(), key -> 1, Integer::sum));
    }
}
