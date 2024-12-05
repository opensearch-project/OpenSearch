/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationState;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.coordination.PublishClusterStateStats;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.tasks.Task;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.action.admin.cluster.node.info.NodesInfoRequest.Metric.SETTINGS;
import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.DISCOVERY;
import static org.opensearch.cluster.coordination.PublicationTransportHandler.PUBLISH_REMOTE_STATE_ACTION_NAME;
import static org.opensearch.cluster.coordination.PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME;
import static org.opensearch.cluster.metadata.Metadata.isGlobalStateEquals;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING_KEY;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteDownloadStats.CHECKSUM_VALIDATION_FAILED_COUNT;
import static org.opensearch.gateway.remote.RemotePersistenceStats.DIFF_DOWNLOAD_STATS;
import static org.opensearch.gateway.remote.RemotePersistenceStats.FULL_DOWNLOAD_STATS;
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
        clearInterceptedActions();
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InterceptingTransportService.TestPlugin.class);
        return plugins;
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

        RemoteClusterStateService remoteClusterStateService = internalCluster().getCurrentClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );

        assertFalse(remoteClusterStateService.isRemotePublicationEnabled());
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
            .addMetric(DISCOVERY.metricName())
            .get();

        assertDataNodeDownloadStats(nodesStatsResponseDataNode.getNodes().get(0));
    }

    public void testRemotePublicationDisabledByRollingRestart() throws Exception {
        prepareCluster(3, 2, INDEX_NAME, 1, 2);
        ensureStableCluster(5);
        ensureGreen(INDEX_NAME);

        Set<String> clusterManagers = internalCluster().getClusterManagerNames();
        Set<String> restartedMasters = new HashSet<>();

        for (String clusterManager : clusterManagers) {
            internalCluster().restartNode(clusterManager, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) {
                    restartedMasters.add(nodeName);
                    return Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, false).build();
                }

                @Override
                public void doAfterNodes(int n, Client client) {
                    String activeCM = internalCluster().getClusterManagerName();
                    Set<String> followingCMs = clusterManagers.stream()
                        .filter(node -> !Objects.equals(node, activeCM))
                        .collect(Collectors.toSet());
                    boolean activeCMRestarted = restartedMasters.contains(activeCM);
                    NodesStatsResponse response = client().admin()
                        .cluster()
                        .prepareNodesStats(followingCMs.toArray(new String[0]))
                        .clear()
                        .addMetric(DISCOVERY.metricName())
                        .get();
                    // after master is flipped to restarted master, publication should happen on Transport
                    response.getNodes().forEach(nodeStats -> {
                        if (activeCMRestarted) {
                            PublishClusterStateStats stats = nodeStats.getDiscoveryStats().getPublishStats();
                            assertTrue(
                                stats.getFullClusterStateReceivedCount() > 0 || stats.getCompatibleClusterStateDiffReceivedCount() > 0
                            );
                        } else {
                            DiscoveryStats stats = nodeStats.getDiscoveryStats();
                            assertEquals(0, stats.getPublishStats().getFullClusterStateReceivedCount());
                            assertEquals(0, stats.getPublishStats().getCompatibleClusterStateDiffReceivedCount());
                            assertEquals(0, stats.getPublishStats().getIncompatibleClusterStateDiffReceivedCount());
                        }
                    });

                    NodesInfoResponse nodesInfoResponse = client().admin()
                        .cluster()
                        .prepareNodesInfo(activeCM)
                        .clear()
                        .addMetric(SETTINGS.metricName())
                        .get();
                    // if masterRestarted is true Publication Setting should be false, and vice versa
                    assertTrue(REMOTE_PUBLICATION_SETTING.get(nodesInfoResponse.getNodes().get(0).getSettings()) != activeCMRestarted);

                    followingCMs.forEach(node -> {
                        PersistedStateRegistry registry = internalCluster().getInstance(PersistedStateRegistry.class, node);
                        CoordinationState.PersistedState remoteState = registry.getPersistedState(
                            PersistedStateRegistry.PersistedStateType.REMOTE
                        );
                        if (activeCMRestarted) {
                            assertNull(remoteState.getLastAcceptedState());
                            assertNull(remoteState.getLastAcceptedManifest());
                        } else {
                            ClusterState localState = registry.getPersistedState(PersistedStateRegistry.PersistedStateType.LOCAL)
                                .getLastAcceptedState();
                            ClusterState remotePersistedState = remoteState.getLastAcceptedState();
                            assertTrue(isGlobalStateEquals(localState.metadata(), remotePersistedState.metadata()));
                            assertEquals(localState.nodes(), remotePersistedState.nodes());
                            assertEquals(localState.routingTable(), remotePersistedState.routingTable());
                            assertEquals(localState.customs(), remotePersistedState.customs());
                        }
                    });
                }
            });

        }
        ensureGreen(INDEX_NAME);
        ensureStableCluster(5);

        String activeCM = internalCluster().getClusterManagerName();
        Set<String> followingCMs = clusterManagers.stream().filter(node -> !Objects.equals(node, activeCM)).collect(Collectors.toSet());
        NodesStatsResponse response = client().admin()
            .cluster()
            .prepareNodesStats(followingCMs.toArray(new String[0]))
            .clear()
            .addMetric(DISCOVERY.metricName())
            .get();
        response.getNodes().forEach(nodeStats -> {
            PublishClusterStateStats stats = nodeStats.getDiscoveryStats().getPublishStats();
            assertTrue(stats.getFullClusterStateReceivedCount() > 0 || stats.getCompatibleClusterStateDiffReceivedCount() > 0);
        });
        NodesInfoResponse nodesInfoResponse = client().admin()
            .cluster()
            .prepareNodesInfo(activeCM)
            .clear()
            .addMetric(SETTINGS.metricName())
            .get();
        // if masterRestarted is true Publication Setting should be false, and vice versa
        assertFalse(REMOTE_PUBLICATION_SETTING.get(nodesInfoResponse.getNodes().get(0).getSettings()));

        followingCMs.forEach(node -> {
            PersistedStateRegistry registry = internalCluster().getInstance(PersistedStateRegistry.class, node);
            CoordinationState.PersistedState remoteState = registry.getPersistedState(PersistedStateRegistry.PersistedStateType.REMOTE);
            assertNull(remoteState.getLastAcceptedState());
            assertNull(remoteState.getLastAcceptedManifest());
        });
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

    public void testRemotePublicationDisabledAfterSettingChange() throws ExecutionException, InterruptedException {
        prepareCluster(3, 2, Settings.EMPTY);
        ensureStableCluster(5);
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 2));
        ensureGreen(INDEX_NAME);

        assertEquals(0, (int) getRequestCount(PUBLISH_STATE_ACTION_NAME));
        assertTrue(getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME) > 0);

        // disable remote publication
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, false).build())
            .get();

        clearInterceptedActions();
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);

        createIndex(INDEX_NAME + "2", remoteStoreIndexSettings(1, 2));
        ensureGreen(INDEX_NAME + "2");

        assertTrue(getRequestCount(PUBLISH_STATE_ACTION_NAME) > 0);
        assertEquals(0, (int) getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME));
    }

    public void testRemotePublicationEnabledAfterSettingChange() {
        isRemotePublicationEnabled = false;
        prepareCluster(3, 2, Settings.EMPTY);
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);

        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 2));
        ensureGreen(INDEX_NAME);

        assertTrue(getRequestCount(PUBLISH_STATE_ACTION_NAME) > 0);
        assertEquals(0, (int) getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME));

        // enable remote publication
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, true).build())
            .get();

        clearInterceptedActions();
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);

        createIndex(INDEX_NAME + "2", remoteStoreIndexSettings(1, 2));
        ensureGreen(INDEX_NAME + "2");

        assertEquals(0, (int) getRequestCount(PUBLISH_STATE_ACTION_NAME));
        assertTrue(getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME) > 0);
    }

    public void testRemotePublicationSettingChangePersistedAfterRestart() throws Exception {
        isRemotePublicationEnabled = false;
        prepareCluster(3, 2, Settings.EMPTY);
        ensureStableCluster(5);
        // enable remote publication
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, true).build())
            .get();
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 2));
        ensureGreen(INDEX_NAME);

        assertEquals(0, (int) getRequestCount(PUBLISH_STATE_ACTION_NAME));
        assertTrue(getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME) > 0);

        // disable remote publication
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, false).build())
            .get();

        clearInterceptedActions();
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);

        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public void doAfterNodes(int n, Client client) {
                interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);
            }
        });

        ensureStableCluster(5);
        assertEquals(0, (int) getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME));
        assertTrue(getRequestCount(PUBLISH_STATE_ACTION_NAME) > 0);
    }

    public void testRemotePublicationSettingChangePersistedAfterFullRestart() throws Exception {
        isRemotePublicationEnabled = false;
        prepareCluster(3, 2, Settings.EMPTY);
        ensureStableCluster(5);
        // enable remote publication
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, true).build())
            .get();
        interceptTransportActions(PUBLISH_REMOTE_STATE_ACTION_NAME, PUBLISH_STATE_ACTION_NAME);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 2));
        ensureGreen(INDEX_NAME);

        assertEquals(0, (int) getRequestCount(PUBLISH_STATE_ACTION_NAME));
        assertTrue(getRequestCount(PUBLISH_REMOTE_STATE_ACTION_NAME) > 0);

        // disable remote publication
        internalCluster().client()
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_PUBLICATION_SETTING_KEY, false).build())
            .get();

        internalCluster().fullRestart();

        ensureStableCluster(5);
        NodesStatsResponse response = internalCluster().client()
            .admin()
            .cluster()
            .prepareNodesStats(internalCluster().getNodeNames())
            .clear()
            .addMetric(DISCOVERY.metricName())
            .get();
        response.getNodes().forEach(nodeStats -> {
            DiscoveryStats discoveryStats = nodeStats.getDiscoveryStats();
            assertNotNull(discoveryStats.getClusterStateStats());
            // ensure none of the nodes received remote publication
            discoveryStats.getClusterStateStats()
                .getPersistenceStats()
                .stream()
                .filter(
                    persistedStateStats -> persistedStateStats.getStatsName().equals(FULL_DOWNLOAD_STATS)
                        || persistedStateStats.getStatsName().equals(DIFF_DOWNLOAD_STATS)
                )
                .forEach(persistedStateStats -> {
                    assertEquals(0, persistedStateStats.getSuccessCount());
                    assertEquals(0, persistedStateStats.getFailedCount());
                    assertEquals(0, persistedStateStats.getTotalTimeInMillis());
                });
        });
    }

    private void assertDataNodeDownloadStats(NodeStats nodeStats) {
        // assert cluster state stats for data node
        DiscoveryStats dataNodeDiscoveryStats = nodeStats.getDiscoveryStats();
        assertNotNull(dataNodeDiscoveryStats.getClusterStateStats());
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getUpdateSuccess());
        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(0).getSuccessCount() > 0);
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(0).getFailedCount());
        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(0).getTotalTimeInMillis() > 0);
        assertEquals(
            0,
            dataNodeDiscoveryStats.getClusterStateStats()
                .getPersistenceStats()
                .get(0)
                .getExtendedFields()
                .get(CHECKSUM_VALIDATION_FAILED_COUNT)
                .get()
        );

        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(1).getSuccessCount() > 0);
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(1).getFailedCount());
        assertTrue(dataNodeDiscoveryStats.getClusterStateStats().getPersistenceStats().get(1).getTotalTimeInMillis() > 0);
        assertEquals(
            0,
            dataNodeDiscoveryStats.getClusterStateStats()
                .getPersistenceStats()
                .get(1)
                .getExtendedFields()
                .get(CHECKSUM_VALIDATION_FAILED_COUNT)
                .get()
        );
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

    private static void clearInterceptedActions() {
        Iterable<PluginsService> pluginsServices = internalCluster().getInstances(PluginsService.class);
        for (PluginsService pluginsService : pluginsServices) {
            pluginsService.filterPlugins(InterceptingTransportService.TestPlugin.class).stream().findFirst().get().instance
                .clearInterceptedActions();
        }
    }

    private static void interceptTransportActions(String... actions) {
        Iterable<PluginsService> pluginsServices = internalCluster().getInstances(PluginsService.class);
        for (PluginsService pluginsService : pluginsServices) {
            pluginsService.filterPlugins(InterceptingTransportService.TestPlugin.class).stream().findFirst().get().instance
                .interceptTransportActions(actions);
        }
    }

    private static Integer getRequestCount(String action) {
        Iterable<PluginsService> pluginsServices = internalCluster().getInstances(PluginsService.class);
        return pluginsServices.iterator()
            .next()
            .filterPlugins(InterceptingTransportService.TestPlugin.class)
            .stream()
            .findFirst()
            .get().instance.getRequestCount(action);
    }

    public static class InterceptingTransportService implements TransportInterceptor {

        public static class TestPlugin extends Plugin implements NetworkPlugin {
            public final InterceptingTransportService instance = new InterceptingTransportService();

            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext
            ) {
                return Collections.singletonList(instance);
            }
        }

        private final Set<String> actions = new HashSet<>();
        private final Map<String, Integer> requests = new HashMap<>();

        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
            String action,
            String executor,
            boolean forceExecution,
            TransportRequestHandler<T> actualHandler
        ) {
            return new InterceptingTransportService.InterceptingHandler<>(action, actualHandler);
        }

        synchronized Integer getRequestCount(String action) {
            return requests.getOrDefault(action, 0);
        }

        synchronized void interceptTransportActions(String... actions) {
            Collections.addAll(this.actions, actions);
        }

        synchronized void clearInterceptedActions() {
            actions.clear();
            requests.clear();
        }

        private class InterceptingHandler<T extends TransportRequest> implements TransportRequestHandler<T> {
            private final String action;
            private final TransportRequestHandler<T> handler;
            private final Logger logger = LogManager.getLogger(InterceptingHandler.class);

            InterceptingHandler(String action, TransportRequestHandler<T> handler) {
                this.action = action;
                this.handler = handler;
            }

            @Override
            public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
                synchronized (this) {
                    if (actions.contains(action)) {
                        Integer requestCount = requests.getOrDefault(action, 0);
                        requests.put(action, requestCount + 1);
                        logger.info("intercepted action: {} count: {}", action, requestCount + 1);
                    }
                }
                handler.messageReceived(request, channel, task);
            }
        }
    }
}
