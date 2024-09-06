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
import org.opensearch.cluster.coordination.CoordinationState;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING_KEY;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RemoteStatePublicationIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-index";
    private static final String REMOTE_STATE_PREFIX = "!";
    private static final String REMOTE_ROUTING_PREFIX = "_";
    private boolean isRemoteStateEnabled = true;
    private boolean isRemotePublicationEnabled = true;
    private boolean hasRemoteStateCharPrefix;
    private boolean hasRemoteRoutingCharPrefix;

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
        isRemoteStateEnabled = true;
        isRemotePublicationEnabled = true;
        hasRemoteStateCharPrefix = randomBoolean();
        hasRemoteRoutingCharPrefix = randomBoolean();
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
            .put(REMOTE_PUBLICATION_SETTING_KEY, isRemotePublicationEnabled)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, routingTableRepoName)
            .put(routingTableRepoTypeAttributeKey, ReloadableFsRepository.TYPE)
            .put(routingTableRepoSettingsAttributeKeyPrefix + "location", segmentRepoPath)
            .put(
                RemoteClusterStateService.REMOTE_CLUSTER_STATE_CHECKSUM_VALIDATION_MODE_SETTING.getKey(),
                RemoteClusterStateService.RemoteClusterStateValidationMode.FAILURE
            )
            .put(REMOTE_PUBLICATION_SETTING_KEY, isRemotePublicationEnabled)
            .put(
                RemoteClusterStateService.CLUSTER_REMOTE_STORE_STATE_PATH_PREFIX.getKey(),
                hasRemoteStateCharPrefix ? REMOTE_STATE_PREFIX : ""
            )
            .put(
                RemoteRoutingTableBlobStore.CLUSTER_REMOTE_STORE_ROUTING_TABLE_PATH_PREFIX.getKey(),
                hasRemoteRoutingCharPrefix ? REMOTE_ROUTING_PREFIX : ""
            )
            .put(RemoteIndexMetadataManager.REMOTE_INDEX_METADATA_PATH_TYPE_SETTING.getKey(), PathType.HASHED_PREFIX.toString())
            .put(
                RemoteIndexMetadataManager.REMOTE_INDEX_METADATA_PATH_HASH_ALGO_SETTING.getKey(),
                PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString()
            )
            .build();
    }

    public void testPublication() throws Exception {
        // create cluster with multi node (3 master + 2 data)
        prepareCluster(3, 2, INDEX_NAME, 1, 2);
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

        RemoteClusterStateService remoteClusterStateService = internalCluster().getInstance(
            RemoteClusterStateService.class,
            internalCluster().getClusterManagerName()
        );
        ClusterMetadataManifest manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().metadata().clusterUUID()
        ).get();
        assertThat(manifest.getIndices().size(), is(1));
        if (hasRemoteStateCharPrefix) {
            for (UploadedIndexMetadata md : manifest.getIndices()) {
                assertThat(md.getUploadedFilename(), startsWith(REMOTE_STATE_PREFIX));
            }
        }
        assertThat(manifest.getIndicesRouting().size(), is(1));
        if (hasRemoteRoutingCharPrefix) {
            for (UploadedIndexMetadata md : manifest.getIndicesRouting()) {
                assertThat(md.getUploadedFilename(), startsWith(REMOTE_ROUTING_PREFIX));
            }
        }

        // get settings from each node and verify that it is updated
        Settings settings = clusterService().getSettings();
        logger.info("settings : {}", settings);
        for (Client client : clients()) {
            ClusterStateResponse response = client.admin().cluster().prepareState().clear().setMetadata(true).get();
            String refreshSetting = response.getState()
                .metadata()
                .settings()
                .get(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey());
            assertEquals("10mb", refreshSetting);
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

    public void testMasterReElectionUsesIncrementalUpload() throws IOException {
        prepareCluster(3, 2, INDEX_NAME, 1, 1);
        PersistedStateRegistry persistedStateRegistry = internalCluster().getClusterManagerNodeInstance(PersistedStateRegistry.class);
        GatewayMetaState.RemotePersistedState remotePersistedState = (GatewayMetaState.RemotePersistedState) persistedStateRegistry
            .getPersistedState(PersistedStateRegistry.PersistedStateType.REMOTE);
        ClusterMetadataManifest manifest = remotePersistedState.getLastAcceptedManifest();
        // force elected master to step down
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(4);

        persistedStateRegistry = internalCluster().getClusterManagerNodeInstance(PersistedStateRegistry.class);
        CoordinationState.PersistedState persistedStateAfterElection = persistedStateRegistry.getPersistedState(
            PersistedStateRegistry.PersistedStateType.REMOTE
        );
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

    public void testVotingConfigAreCommitted() throws ExecutionException, InterruptedException {
        prepareCluster(3, 2, INDEX_NAME, 1, 2);
        ensureStableCluster(5);
        ensureGreen(INDEX_NAME);
        // add two new nodes to the cluster, to update the voting config
        internalCluster().startClusterManagerOnlyNodes(2, Settings.EMPTY);
        ensureStableCluster(7);

        internalCluster().getInstances(PersistedStateRegistry.class).forEach(persistedStateRegistry -> {
            CoordinationState.PersistedState localState = persistedStateRegistry.getPersistedState(
                PersistedStateRegistry.PersistedStateType.LOCAL
            );
            CoordinationState.PersistedState remoteState = persistedStateRegistry.getPersistedState(
                PersistedStateRegistry.PersistedStateType.REMOTE
            );
            if (remoteState != null) {
                assertEquals(
                    localState.getLastAcceptedState().getLastCommittedConfiguration(),
                    remoteState.getLastAcceptedState().getLastCommittedConfiguration()
                );
                assertEquals(5, remoteState.getLastAcceptedState().getLastCommittedConfiguration().getNodeIds().size());
            }
        });
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
