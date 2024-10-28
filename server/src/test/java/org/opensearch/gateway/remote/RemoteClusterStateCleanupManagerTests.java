/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService;
import org.opensearch.cluster.routing.remote.NoopRemoteRoutingTableService;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V1;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V2;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V3;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.AsyncStaleFileDeletion;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.RETAINED_MANIFESTS;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.SKIP_CLEANUP_STATE_CHANGES;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.encodeString;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getFormattedIndexFileName;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteClusterStateCleanupManagerTests extends OpenSearchTestCase {
    private RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;
    private ClusterSettings clusterSettings;
    private ClusterApplierService clusterApplierService;
    private ClusterState clusterState;
    private Metadata metadata;
    private RemoteClusterStateService remoteClusterStateService;
    private RemoteManifestManager remoteManifestManager;
    private RemoteRoutingTableService remoteRoutingTableService;
    private ClusterService clusterService;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);

        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            "remote_store_repository"
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            "remote_store_repository"
        );

        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "remote_store_repository")
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put(stateRepoTypeAttributeKey, FsRepository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", "randomRepoPath")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();

        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterApplierService = mock(ClusterApplierService.class);
        clusterState = mock(ClusterState.class);
        metadata = mock(Metadata.class);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterState.getClusterName()).thenReturn(new ClusterName("test"));
        when(metadata.clusterUUID()).thenReturn("testUUID");
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterApplierService.state()).thenReturn(clusterState);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);

        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(blobStoreRepository);

        remoteManifestManager = mock(RemoteManifestManager.class);
        remoteClusterStateService = mock(RemoteClusterStateService.class);
        when(remoteClusterStateService.getRemoteManifestManager()).thenReturn(remoteManifestManager);
        when(remoteClusterStateService.getRemoteStateStats()).thenReturn(new RemotePersistenceStats());
        when(remoteClusterStateService.getThreadpool()).thenReturn(threadPool);
        when(remoteClusterStateService.getBlobStore()).thenReturn(blobStore);
        when(remoteClusterStateService.getBlobStoreRepository()).thenReturn(blobStoreRepository);
        remoteRoutingTableService = mock(InternalRemoteRoutingTableService.class);
        remoteClusterStateCleanupManager = new RemoteClusterStateCleanupManager(
            remoteClusterStateService,
            clusterService,
            remoteRoutingTableService
        );
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteClusterStateCleanupManager.close();
        threadPool.shutdown();
    }

    public void testDeleteClusterMetadata() throws IOException {
        String clusterUUID = "clusterUUID";
        String clusterName = "test-cluster";
        List<BlobMetadata> inactiveBlobs = Arrays.asList(
            new PlainBlobMetadata("manifest1.dat", 1L),
            new PlainBlobMetadata("manifest2.dat", 1L),
            new PlainBlobMetadata("manifest3.dat", 1L),
            new PlainBlobMetadata("manifest6.dat", 1L)
        );
        List<BlobMetadata> activeBlobs = Arrays.asList(
            new PlainBlobMetadata("manifest4.dat", 1L),
            new PlainBlobMetadata("manifest5.dat", 1L),
            new PlainBlobMetadata("manifest7.dat", 1L)
        );
        UploadedIndexMetadata index1Metadata = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1__1");
        UploadedIndexMetadata index2Metadata = new UploadedIndexMetadata("index2", "indexUUID2", "index_metadata2__2");
        UploadedIndexMetadata index1UpdatedMetadata = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1_updated__2");
        UploadedMetadataAttribute coordinationMetadata = new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination_metadata");
        UploadedMetadataAttribute templateMetadata = new UploadedMetadataAttribute(TEMPLATES_METADATA, "template_metadata");
        UploadedMetadataAttribute settingMetadata = new UploadedMetadataAttribute(SETTING_METADATA, "settings_metadata");
        UploadedMetadataAttribute coordinationMetadataUpdated = new UploadedMetadataAttribute(
            COORDINATION_METADATA,
            "coordination_metadata_updated"
        );
        UploadedMetadataAttribute templateMetadataUpdated = new UploadedMetadataAttribute(TEMPLATES_METADATA, "template_metadata_updated");
        UploadedMetadataAttribute settingMetadataUpdated = new UploadedMetadataAttribute(SETTING_METADATA, "settings_metadata_updated");
        ClusterMetadataManifest manifest1 = ClusterMetadataManifest.builder()
            .indices(List.of(index1Metadata))
            .globalMetadataFileName("global_metadata")
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V1)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .build();
        ClusterMetadataManifest manifest2 = ClusterMetadataManifest.builder(manifest1)
            .indices(List.of(index1Metadata, index2Metadata))
            .codecVersion(CODEC_V2)
            .globalMetadataFileName(null)
            .coordinationMetadata(coordinationMetadata)
            .templatesMetadata(templateMetadata)
            .settingMetadata(settingMetadata)
            .build();
        ClusterMetadataManifest manifest3 = ClusterMetadataManifest.builder(manifest2)
            .indices(List.of(index1UpdatedMetadata, index2Metadata))
            .settingMetadata(settingMetadataUpdated)
            .build();

        UploadedIndexMetadata index3Metadata = new UploadedIndexMetadata("index3", "indexUUID3", "index_metadata3__2");
        UploadedIndexMetadata index4Metadata = new UploadedIndexMetadata("index4", "indexUUID4", "index_metadata4__2");
        List<UploadedIndexMetadata> indicesRouting1 = List.of(index3Metadata, index4Metadata);
        List<UploadedIndexMetadata> indicesRouting2 = List.of(index4Metadata);
        ClusterMetadataManifest manifest6 = ClusterMetadataManifest.builder()
            .indices(List.of(index1Metadata))
            .coordinationMetadata(coordinationMetadataUpdated)
            .templatesMetadata(templateMetadataUpdated)
            .settingMetadata(settingMetadataUpdated)
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V2)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .routingTableVersion(0L)
            .indicesRouting(indicesRouting1)
            .build();
        ClusterMetadataManifest manifest7 = ClusterMetadataManifest.builder()
            .indices(List.of(index2Metadata))
            .coordinationMetadata(coordinationMetadataUpdated)
            .templatesMetadata(templateMetadataUpdated)
            .settingMetadata(settingMetadataUpdated)
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V2)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .routingTableVersion(0L)
            .indicesRouting(indicesRouting2)
            .build();

        // active manifest have reference to index1Updated, index2, settingsUpdated, coordinationUpdated, templates, templatesUpdated
        ClusterMetadataManifest manifest4 = ClusterMetadataManifest.builder(manifest3)
            .coordinationMetadata(coordinationMetadataUpdated)
            .build();
        ClusterMetadataManifest manifest5 = ClusterMetadataManifest.builder(manifest4).templatesMetadata(templateMetadataUpdated).build();

        when(remoteManifestManager.fetchRemoteClusterMetadataManifest(eq(clusterName), eq(clusterUUID), any())).thenReturn(
            manifest4,
            manifest5,
            manifest7,
            manifest1,
            manifest2,
            manifest3,
            manifest6
        );
        when(remoteManifestManager.getManifestFolderPath(eq(clusterName), eq(clusterUUID))).thenReturn(
            new BlobPath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID).add(MANIFEST)
        );
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(any())).thenReturn(container);
        doNothing().when(container).deleteBlobsIgnoringIfNotExists(any());
        remoteClusterStateCleanupManager.start();
        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        verify(container).deleteBlobsIgnoringIfNotExists(
            List.of(
                // coordination/setting metadata is from CODEC_V2, the uploaded filename with contain the complete path
                coordinationMetadata.getUploadedFilename(),
                settingMetadata.getUploadedFilename(),
                new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + "global_metadata.dat"
            )
        );
        verify(container).deleteBlobsIgnoringIfNotExists(List.of(getFormattedIndexFileName(index1Metadata.getUploadedFilePath())));
        Set<String> staleManifest = new HashSet<>();
        inactiveBlobs.forEach(
            blob -> staleManifest.add(
                remoteClusterStateService.getRemoteManifestManager().getManifestFolderPath(clusterName, clusterUUID).buildAsString() + blob
                    .name()
            )
        );
        verify(container).deleteBlobsIgnoringIfNotExists(new ArrayList<>(staleManifest));
        verify(remoteRoutingTableService).deleteStaleIndexRoutingPaths(List.of(index3Metadata.getUploadedFilename()));
    }

    public void testDeleteStaleIndicesRoutingDiffFile() throws IOException {
        String clusterUUID = "clusterUUID";
        String clusterName = "test-cluster";
        List<BlobMetadata> inactiveBlobs = Arrays.asList(new PlainBlobMetadata("manifest1.dat", 1L));
        List<BlobMetadata> activeBlobs = Arrays.asList(new PlainBlobMetadata("manifest2.dat", 1L));

        UploadedMetadataAttribute coordinationMetadata = new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination_metadata");
        UploadedMetadataAttribute templateMetadata = new UploadedMetadataAttribute(TEMPLATES_METADATA, "template_metadata");
        UploadedMetadataAttribute settingMetadata = new UploadedMetadataAttribute(SETTING_METADATA, "settings_metadata");
        UploadedMetadataAttribute coordinationMetadataUpdated = new UploadedMetadataAttribute(
            COORDINATION_METADATA,
            "coordination_metadata_updated"
        );

        UploadedIndexMetadata index1Metadata = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1__2");
        UploadedIndexMetadata index2Metadata = new UploadedIndexMetadata("index2", "indexUUID2", "index_metadata2__2");
        List<UploadedIndexMetadata> indicesRouting1 = List.of(index1Metadata);
        List<UploadedIndexMetadata> indicesRouting2 = List.of(index2Metadata);
        ClusterStateDiffManifest diffManifest1 = ClusterStateDiffManifest.builder().indicesRoutingDiffPath("index1RoutingDiffPath").build();
        ClusterStateDiffManifest diffManifest2 = ClusterStateDiffManifest.builder().indicesRoutingDiffPath("index2RoutingDiffPath").build();

        ClusterMetadataManifest manifest1 = ClusterMetadataManifest.builder()
            .indices(List.of(index1Metadata))
            .coordinationMetadata(coordinationMetadataUpdated)
            .templatesMetadata(templateMetadata)
            .settingMetadata(settingMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V3)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .routingTableVersion(0L)
            .indicesRouting(indicesRouting1)
            .diffManifest(diffManifest1)
            .build();
        ClusterMetadataManifest manifest2 = ClusterMetadataManifest.builder(manifest1)
            .indices(List.of(index2Metadata))
            .indicesRouting(indicesRouting2)
            .diffManifest(diffManifest2)
            .build();

        BlobContainer blobContainer = mock(BlobContainer.class);
        doThrow(IOException.class).when(blobContainer).delete();
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        remoteClusterStateCleanupManager.start();
        when(remoteManifestManager.getManifestFolderPath(eq(clusterName), eq(clusterUUID))).thenReturn(
            new BlobPath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID).add(MANIFEST)
        );
        when(remoteManifestManager.fetchRemoteClusterMetadataManifest(eq(clusterName), eq(clusterUUID), any())).thenReturn(
            manifest2,
            manifest1
        );
        remoteClusterStateCleanupManager = new RemoteClusterStateCleanupManager(
            remoteClusterStateService,
            clusterService,
            remoteRoutingTableService
        );
        remoteClusterStateCleanupManager.start();
        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        verify(remoteRoutingTableService).deleteStaleIndexRoutingDiffPaths(List.of("index1RoutingDiffPath"));
    }

    public void testDeleteClusterMetadataNoOpsRoutingTableService() throws IOException {
        String clusterUUID = "clusterUUID";
        String clusterName = "test-cluster";
        List<BlobMetadata> inactiveBlobs = Arrays.asList(new PlainBlobMetadata("manifest1.dat", 1L));
        List<BlobMetadata> activeBlobs = Arrays.asList(new PlainBlobMetadata("manifest2.dat", 1L));

        UploadedMetadataAttribute coordinationMetadata = new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination_metadata");
        UploadedMetadataAttribute templateMetadata = new UploadedMetadataAttribute(TEMPLATES_METADATA, "template_metadata");
        UploadedMetadataAttribute settingMetadata = new UploadedMetadataAttribute(SETTING_METADATA, "settings_metadata");
        UploadedMetadataAttribute coordinationMetadataUpdated = new UploadedMetadataAttribute(
            COORDINATION_METADATA,
            "coordination_metadata_updated"
        );

        UploadedIndexMetadata index1Metadata = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1__2");
        UploadedIndexMetadata index2Metadata = new UploadedIndexMetadata("index2", "indexUUID2", "index_metadata2__2");
        List<UploadedIndexMetadata> indicesRouting1 = List.of(index1Metadata);
        List<UploadedIndexMetadata> indicesRouting2 = List.of(index2Metadata);

        ClusterMetadataManifest manifest1 = ClusterMetadataManifest.builder()
            .indices(List.of(index1Metadata))
            .coordinationMetadata(coordinationMetadataUpdated)
            .templatesMetadata(templateMetadata)
            .settingMetadata(settingMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V2)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .routingTableVersion(0L)
            .indicesRouting(indicesRouting1)
            .build();
        ClusterMetadataManifest manifest2 = ClusterMetadataManifest.builder(manifest1)
            .indices(List.of(index2Metadata))
            .indicesRouting(indicesRouting2)
            .build();

        BlobContainer blobContainer = mock(BlobContainer.class);
        doThrow(IOException.class).when(blobContainer).delete();
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        remoteClusterStateCleanupManager.start();
        when(remoteManifestManager.getManifestFolderPath(eq(clusterName), eq(clusterUUID))).thenReturn(
            new BlobPath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID).add(MANIFEST)
        );
        when(remoteManifestManager.fetchRemoteClusterMetadataManifest(eq(clusterName), eq(clusterUUID), any())).thenReturn(
            manifest2,
            manifest1
        );
        remoteRoutingTableService = mock(NoopRemoteRoutingTableService.class);
        remoteClusterStateCleanupManager = new RemoteClusterStateCleanupManager(
            remoteClusterStateService,
            clusterService,
            remoteRoutingTableService
        );
        remoteClusterStateCleanupManager.start();
        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        verify(remoteRoutingTableService).deleteStaleIndexRoutingPaths(List.of(index1Metadata.getUploadedFilename()));
    }

    public void testDeleteStaleClusterUUIDs() throws IOException {
        final ClusterState clusterState = RemoteClusterStateServiceTests.generateClusterStateWithOneIndex()
            .nodes(RemoteClusterStateServiceTests.nodesWithLocalNodeClusterManager())
            .build();
        ClusterMetadataManifest clusterMetadataManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID("cluster-uuid1")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .build();

        BlobPath blobPath = new BlobPath().add("random-path");
        BlobContainer uuidContainerContainer = mock(BlobContainer.class);
        BlobContainer manifest2Container = mock(BlobContainer.class);
        BlobContainer manifest3Container = mock(BlobContainer.class);
        when(blobStore.blobContainer(any())).then(invocation -> {
            BlobPath blobPath1 = invocation.getArgument(0);
            if (blobPath1.buildAsString().endsWith("cluster-state/")) {
                return uuidContainerContainer;
            } else if (blobPath1.buildAsString().contains("cluster-state/cluster-uuid2/")) {
                return manifest2Container;
            } else if (blobPath1.buildAsString().contains("cluster-state/cluster-uuid3/")) {
                return manifest3Container;
            } else {
                throw new IllegalArgumentException("Unexpected blob path " + blobPath1);
            }
        });
        when(
            manifest2Container.listBlobsByPrefixInSortedOrder(
                MANIFEST + DELIMITER,
                Integer.MAX_VALUE,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(List.of(new PlainBlobMetadata("mainfest2", 1L)));
        when(
            manifest3Container.listBlobsByPrefixInSortedOrder(
                MANIFEST + DELIMITER,
                Integer.MAX_VALUE,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(List.of(new PlainBlobMetadata("mainfest3", 1L)));
        Set<String> uuids = new HashSet<>(Arrays.asList("cluster-uuid1", "cluster-uuid2", "cluster-uuid3"));
        when(remoteClusterStateService.getAllClusterUUIDs(any())).thenReturn(uuids);
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        remoteClusterStateCleanupManager.start();
        remoteClusterStateCleanupManager.deleteStaleClusterUUIDs(clusterState, clusterMetadataManifest);
        try {
            assertBusy(() -> {
                verify(manifest2Container, times(1)).delete();
                verify(manifest3Container, times(1)).delete();
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testRemoteStateCleanupFailureStats() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        doThrow(IOException.class).when(blobContainer).delete();
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        remoteClusterStateCleanupManager.start();
        remoteClusterStateCleanupManager.deleteStaleUUIDsClusterMetadata("cluster1", Arrays.asList("cluster-uuid1"));
        try {
            assertBusy(() -> {
                // wait for stats to get updated
                assertTrue(remoteClusterStateCleanupManager.getStats() != null);
                assertEquals(0, remoteClusterStateCleanupManager.getStats().getUploadStats().getSuccessCount());
                assertEquals(1, remoteClusterStateCleanupManager.getStats().getCleanupAttemptFailedCount());
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testIndexRoutingFilesCleanupFailureStats() throws Exception {
        String clusterUUID = "clusterUUID";
        String clusterName = "test-cluster";
        List<BlobMetadata> inactiveBlobs = Arrays.asList(new PlainBlobMetadata("manifest1.dat", 1L));
        List<BlobMetadata> activeBlobs = Arrays.asList(new PlainBlobMetadata("manifest2.dat", 1L));

        UploadedMetadataAttribute coordinationMetadata = new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination_metadata");
        UploadedMetadataAttribute templateMetadata = new UploadedMetadataAttribute(TEMPLATES_METADATA, "template_metadata");
        UploadedMetadataAttribute settingMetadata = new UploadedMetadataAttribute(SETTING_METADATA, "settings_metadata");
        UploadedMetadataAttribute coordinationMetadataUpdated = new UploadedMetadataAttribute(
            COORDINATION_METADATA,
            "coordination_metadata_updated"
        );

        UploadedIndexMetadata index1Metadata = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1__2");
        UploadedIndexMetadata index2Metadata = new UploadedIndexMetadata("index2", "indexUUID2", "index_metadata2__2");
        List<UploadedIndexMetadata> indicesRouting1 = List.of(index1Metadata);
        List<UploadedIndexMetadata> indicesRouting2 = List.of(index2Metadata);

        ClusterMetadataManifest manifest1 = ClusterMetadataManifest.builder()
            .indices(List.of(index1Metadata))
            .coordinationMetadata(coordinationMetadataUpdated)
            .templatesMetadata(templateMetadata)
            .settingMetadata(settingMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V2)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .routingTableVersion(0L)
            .indicesRouting(indicesRouting1)
            .build();
        ClusterMetadataManifest manifest2 = ClusterMetadataManifest.builder(manifest1)
            .indices(List.of(index2Metadata))
            .indicesRouting(indicesRouting2)
            .build();

        BlobContainer blobContainer = mock(BlobContainer.class);
        doThrow(IOException.class).when(blobContainer).delete();
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        remoteClusterStateCleanupManager.start();
        when(remoteManifestManager.getManifestFolderPath(eq(clusterName), eq(clusterUUID))).thenReturn(
            new BlobPath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID).add(MANIFEST)
        );
        when(remoteManifestManager.fetchRemoteClusterMetadataManifest(eq(clusterName), eq(clusterUUID), any())).thenReturn(
            manifest1,
            manifest2
        );
        doNothing().when(remoteRoutingTableService).deleteStaleIndexRoutingPaths(any());

        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        assertBusy(() -> {
            // wait for stats to get updated
            assertNotNull(remoteClusterStateCleanupManager.getStats());
            assertEquals(0, remoteClusterStateCleanupManager.getStats().getIndexRoutingFilesCleanupAttemptFailedCount());
        });

        doThrow(IOException.class).when(remoteRoutingTableService).deleteStaleIndexRoutingPaths(any());
        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        assertBusy(() -> {
            // wait for stats to get updated
            assertNotNull(remoteClusterStateCleanupManager.getStats());
            assertEquals(1, remoteClusterStateCleanupManager.getStats().getIndexRoutingFilesCleanupAttemptFailedCount());
        });
    }

    public void testIndicesRoutingDiffFilesCleanupFailureStats() throws Exception {
        String clusterUUID = "clusterUUID";
        String clusterName = "test-cluster";
        List<BlobMetadata> inactiveBlobs = Arrays.asList(new PlainBlobMetadata("manifest1.dat", 1L));
        List<BlobMetadata> activeBlobs = Arrays.asList(new PlainBlobMetadata("manifest2.dat", 1L));

        UploadedMetadataAttribute coordinationMetadata = new UploadedMetadataAttribute(COORDINATION_METADATA, "coordination_metadata");
        UploadedMetadataAttribute templateMetadata = new UploadedMetadataAttribute(TEMPLATES_METADATA, "template_metadata");
        UploadedMetadataAttribute settingMetadata = new UploadedMetadataAttribute(SETTING_METADATA, "settings_metadata");
        UploadedMetadataAttribute coordinationMetadataUpdated = new UploadedMetadataAttribute(
            COORDINATION_METADATA,
            "coordination_metadata_updated"
        );

        UploadedIndexMetadata index1Metadata = new UploadedIndexMetadata("index1", "indexUUID1", "index_metadata1__2");
        UploadedIndexMetadata index2Metadata = new UploadedIndexMetadata("index2", "indexUUID2", "index_metadata2__2");
        List<UploadedIndexMetadata> indicesRouting1 = List.of(index1Metadata);
        List<UploadedIndexMetadata> indicesRouting2 = List.of(index2Metadata);
        ClusterStateDiffManifest diffManifest1 = ClusterStateDiffManifest.builder().indicesRoutingDiffPath("index1RoutingDiffPath").build();
        ClusterStateDiffManifest diffManifest2 = ClusterStateDiffManifest.builder().indicesRoutingDiffPath("index2RoutingDiffPath").build();

        ClusterMetadataManifest manifest1 = ClusterMetadataManifest.builder()
            .indices(List.of(index1Metadata))
            .coordinationMetadata(coordinationMetadataUpdated)
            .templatesMetadata(templateMetadata)
            .settingMetadata(settingMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .codecVersion(CODEC_V3)
            .stateUUID(randomAlphaOfLength(10))
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(ClusterState.UNKNOWN_UUID)
            .committed(true)
            .routingTableVersion(0L)
            .indicesRouting(indicesRouting1)
            .diffManifest(diffManifest1)
            .build();
        ClusterMetadataManifest manifest2 = ClusterMetadataManifest.builder(manifest1)
            .indices(List.of(index2Metadata))
            .indicesRouting(indicesRouting2)
            .diffManifest(diffManifest2)
            .build();

        BlobContainer blobContainer = mock(BlobContainer.class);
        doThrow(IOException.class).when(blobContainer).delete();
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        remoteClusterStateCleanupManager.start();
        when(remoteManifestManager.getManifestFolderPath(eq(clusterName), eq(clusterUUID))).thenReturn(
            new BlobPath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID).add(MANIFEST)
        );
        when(remoteManifestManager.fetchRemoteClusterMetadataManifest(eq(clusterName), eq(clusterUUID), any())).thenReturn(
            manifest1,
            manifest2
        );
        doNothing().when(remoteRoutingTableService).deleteStaleIndexRoutingDiffPaths(any());

        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        assertBusy(() -> {
            // wait for stats to get updated
            assertNotNull(remoteClusterStateCleanupManager.getStats());
            assertEquals(0, remoteClusterStateCleanupManager.getStats().getIndicesRoutingDiffFileCleanupAttemptFailedCount());
        });

        doThrow(IOException.class).when(remoteRoutingTableService).deleteStaleIndexRoutingDiffPaths(any());
        remoteClusterStateCleanupManager.deleteClusterMetadata(clusterName, clusterUUID, activeBlobs, inactiveBlobs);
        assertBusy(() -> {
            // wait for stats to get updated
            assertNotNull(remoteClusterStateCleanupManager.getStats());
            assertEquals(1, remoteClusterStateCleanupManager.getStats().getIndicesRoutingDiffFileCleanupAttemptFailedCount());
        });
    }

    public void testSingleConcurrentExecutionOfStaleManifestCleanup() throws Exception {
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            callCount.incrementAndGet();
            if (latch.await(5000, TimeUnit.SECONDS) == false) {
                throw new Exception("Timed out waiting for delete task queuing to complete");
            }
            return null;
        }).when(blobContainer)
            .listBlobsByPrefixInSortedOrder(
                any(String.class),
                any(int.class),
                any(BlobContainer.BlobNameSortOrder.class),
                any(ActionListener.class)
            );

        remoteClusterStateCleanupManager.start();
        remoteClusterStateCleanupManager.deleteStaleClusterMetadata("cluster-name", "cluster-uuid", RETAINED_MANIFESTS);
        remoteClusterStateCleanupManager.deleteStaleClusterMetadata("cluster-name", "cluster-uuid", RETAINED_MANIFESTS);

        latch.countDown();
        assertBusy(() -> assertEquals(1, callCount.get()));
    }

    public void testRemoteClusterStateCleanupSetting() {
        remoteClusterStateCleanupManager.start();
        // verify default value
        assertEquals(CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT, remoteClusterStateCleanupManager.getStaleFileCleanupInterval());

        // verify update interval
        int cleanupInterval = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder().put("cluster.remote_store.state.cleanup_interval", cleanupInterval + "s").build();
        clusterSettings.applySettings(newSettings);
        assertEquals(cleanupInterval, remoteClusterStateCleanupManager.getStaleFileCleanupInterval().seconds());
    }

    public void testRemoteCleanupTaskScheduled() {
        AbstractAsyncTask cleanupTask = remoteClusterStateCleanupManager.getStaleFileDeletionTask();
        assertNull(cleanupTask);
        // now the task should be initialized
        remoteClusterStateCleanupManager.start();
        assertNotNull(remoteClusterStateCleanupManager.getStaleFileDeletionTask());
        assertTrue(remoteClusterStateCleanupManager.getStaleFileDeletionTask().mustReschedule());
        assertEquals(
            clusterSettings.get(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING),
            remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval()
        );
        assertTrue(remoteClusterStateCleanupManager.getStaleFileDeletionTask().isScheduled());
        assertFalse(remoteClusterStateCleanupManager.getStaleFileDeletionTask().isClosed());
    }

    public void testRemoteCleanupSkipsOnOnlyElectedClusterManager() {
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.isLocalNodeElectedClusterManager()).thenReturn(false);
        when(clusterState.nodes()).thenReturn(nodes);
        RemoteClusterStateCleanupManager spyManager = spy(remoteClusterStateCleanupManager);
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> callCount.incrementAndGet()).when(spyManager).deleteStaleClusterMetadata(any(), any(), anyInt());
        spyManager.cleanUpStaleFiles();
        assertEquals(0, callCount.get());

        when(nodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(clusterState.version()).thenReturn(randomLongBetween(11, 20));
        spyManager.cleanUpStaleFiles();
        assertEquals(1, callCount.get());
    }

    public void testRemoteCleanupSkipsIfVersionIncrementLessThanThreshold() {
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        long version = randomLongBetween(1, SKIP_CLEANUP_STATE_CHANGES);
        when(clusterApplierService.state()).thenReturn(clusterState);
        when(nodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterState.version()).thenReturn(version);

        RemoteClusterStateCleanupManager spyManager = spy(remoteClusterStateCleanupManager);
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> callCount.incrementAndGet()).when(spyManager).deleteStaleClusterMetadata(any(), any(), anyInt());

        remoteClusterStateCleanupManager.cleanUpStaleFiles();
        assertEquals(0, callCount.get());
    }

    public void testRemoteCleanupCallsDeleteIfVersionIncrementGreaterThanThreshold() {
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        long version = randomLongBetween(SKIP_CLEANUP_STATE_CHANGES + 1, SKIP_CLEANUP_STATE_CHANGES + 10);
        when(clusterApplierService.state()).thenReturn(clusterState);
        when(nodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterState.version()).thenReturn(version);

        RemoteClusterStateCleanupManager spyManager = spy(remoteClusterStateCleanupManager);
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> callCount.incrementAndGet()).when(spyManager).deleteStaleClusterMetadata(any(), any(), anyInt());

        // using spied cleanup manager so that stubbed deleteStaleClusterMetadata is called
        spyManager.cleanUpStaleFiles();
        assertEquals(1, callCount.get());
    }

    public void testRemoteCleanupSchedulesEvenAfterFailure() {
        remoteClusterStateCleanupManager.start();
        RemoteClusterStateCleanupManager spyManager = spy(remoteClusterStateCleanupManager);
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            callCount.incrementAndGet();
            throw new RuntimeException("Test exception");
        }).when(spyManager).cleanUpStaleFiles();
        AsyncStaleFileDeletion task = new AsyncStaleFileDeletion(spyManager);
        assertTrue(task.isScheduled());
        task.run();
        // Task is still scheduled after the failure
        assertTrue(task.isScheduled());
        assertEquals(1, callCount.get());

        task.run();
        // Task is still scheduled after the failure
        assertTrue(task.isScheduled());
        assertEquals(2, callCount.get());
    }
}
