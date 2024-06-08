/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteIndexPathUploader;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestCustomMetadata;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.RemoteClusterStateService.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.RemoteClusterStateService.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateService.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateService.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateService.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateService.MANIFEST_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteClusterStateService.METADATA_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteClusterStateService.SETTING_METADATA;
import static org.opensearch.gateway.remote.RemoteClusterStateService.TEMPLATES_METADATA;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterStateServiceTests extends OpenSearchTestCase {

    private RemoteClusterStateService remoteClusterStateService;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;
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
            .put(stateRepoTypeAttributeKey, FsRepository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", "randomRepoPath")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();

        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );

        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            settings,
            clusterService,
            () -> 0L,
            threadPool,
            List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings))
        );
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteClusterStateService.close();
        threadPool.shutdown();
    }

    public void testFailWriteFullMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().build();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, randomAlphaOfLength(10));
        Assert.assertThat(manifest, nullValue());
    }

    public void testFailInitializationWhenRemoteStateDisabled() {
        final Settings settings = Settings.builder().build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertThrows(
            AssertionError.class,
            () -> new RemoteClusterStateService(
                "test-node-id",
                repositoriesServiceSupplier,
                settings,
                clusterService,
                () -> 0L,
                threadPool,
                List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings))
            )
        );
    }

    public void testFailInitializeWhenRepositoryNotSet() {
        doThrow(new RepositoryMissingException("repository missing")).when(repositoriesService).repository("remote_store_repository");
        assertThrows(RepositoryMissingException.class, () -> remoteClusterStateService.start());
    }

    public void testFailWriteFullMetadataWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(filterRepository);
        assertThrows(AssertionError.class, () -> remoteClusterStateService.start());
    }

    public void testWriteFullMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid");
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));
        assertThat(manifest.getGlobalMetadataFileName(), nullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getSettingsMetadata(), notNullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertFalse(manifest.getCustomMetadataMap().isEmpty());
    }

    public void testWriteFullMetadataInParallelSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        ConcurrentHashMap<String, WriteContext> capturedWriteContext = new ConcurrentHashMap<>();
        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            WriteContext writeContext = writeContextArgumentCaptor.getValue();
            capturedWriteContext.put(writeContext.getFileName().split(DELIMITER)[0], writeContextArgumentCaptor.getValue());
            return null;
        }).when(container).asyncBlobUpload(writeContextArgumentCaptor.capture(), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid");

        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getGlobalMetadataFileName(), nullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getSettingsMetadata(), notNullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertThat(manifest.getCustomMetadataMap().size(), not(0));
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));

        assertEquals(7, actionListenerArgumentCaptor.getAllValues().size());
        assertEquals(7, writeContextArgumentCaptor.getAllValues().size());

        byte[] writtenBytes = capturedWriteContext.get("metadata")
            .getStreamProvider(Integer.MAX_VALUE)
            .provideStream(0)
            .getInputStream()
            .readAllBytes();
        IndexMetadata writtenIndexMetadata = RemoteClusterStateService.INDEX_METADATA_FORMAT.deserialize(
            capturedWriteContext.get("metadata").getFileName(),
            blobStoreRepository.getNamedXContentRegistry(),
            new BytesArray(writtenBytes)
        );

        assertEquals(capturedWriteContext.get("metadata").getWritePriority(), WritePriority.URGENT);
        assertEquals(writtenIndexMetadata.getNumberOfShards(), 1);
        assertEquals(writtenIndexMetadata.getNumberOfReplicas(), 0);
        assertEquals(writtenIndexMetadata.getIndex().getName(), "test-index");
        assertEquals(writtenIndexMetadata.getIndex().getUUID(), "index-uuid");
        long expectedChecksum = RemoteTransferContainer.checksumOfChecksum(new ByteArrayIndexInput("metadata-filename", writtenBytes), 8);
        if (capturedWriteContext.get("metadata").doRemoteDataIntegrityCheck()) {
            assertEquals(capturedWriteContext.get("metadata").getExpectedChecksum().longValue(), expectedChecksum);
        } else {
            assertEquals(capturedWriteContext.get("metadata").getExpectedChecksum(), null);
        }

    }

    public void testWriteFullMetadataFailureForGlobalMetadata() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer((i) -> {
            // For async write action listener will be called from different thread, replicating same behaviour here.
            new Thread(new Runnable() {
                @Override
                public void run() {
                    actionListenerArgumentCaptor.getValue().onFailure(new RuntimeException("Cannot upload to remote"));
                }
            }).start();
            return null;
        }).when(container).asyncBlobUpload(any(WriteContext.class), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        assertThrows(
            RemoteClusterStateService.RemoteStateTransferException.class,
            () -> remoteClusterStateService.writeFullMetadata(clusterState, randomAlphaOfLength(10))
        );
    }

    public void testTimeoutWhileWritingManifestFile() throws IOException {
        // verify update metadata manifest upload timeout
        int metadataManifestUploadTimeout = 2;
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.metadata_manifest.upload_timeout", metadataManifestUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);

        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer((i) -> { // For Global Metadata
            actionListenerArgumentCaptor.getValue().onResponse(null);
            return null;
        }).doAnswer((i) -> { // For Index Metadata
            actionListenerArgumentCaptor.getValue().onResponse(null);
            return null;
        }).doAnswer((i) -> {
            // For Manifest file perform No Op, so latch in code will timeout
            return null;
        }).when(container).asyncBlobUpload(any(WriteContext.class), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        try {
            remoteClusterStateService.writeFullMetadata(clusterState, randomAlphaOfLength(10));
        } catch (Exception e) {
            assertTrue(e instanceof RemoteClusterStateService.RemoteStateTransferException);
            assertTrue(e.getMessage().contains("Timed out waiting for transfer of following metadata to complete"));
        }
    }

    public void testWriteFullMetadataInParallelFailureForIndexMetadata() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            return null;
        }).doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onFailure(new RuntimeException("Cannot upload to remote"));
            return null;
        }).when(container).asyncBlobUpload(any(WriteContext.class), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        assertThrows(
            RemoteClusterStateService.RemoteStateTransferException.class,
            () -> remoteClusterStateService.writeFullMetadata(clusterState, randomAlphaOfLength(10))
        );
        assertEquals(0, remoteClusterStateService.getStats().getSuccessCount());
    }

    public void testFailWriteIncrementalMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(clusterState, clusterState, null);
        Assert.assertThat(manifest, nullValue());
        assertEquals(0, remoteClusterStateService.getStats().getSuccessCount());
    }

    public void testFailWriteIncrementalMetadataWhenTermChanged() {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(2L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();
        assertThrows(
            AssertionError.class,
            () -> remoteClusterStateService.writeIncrementalMetadata(previousClusterState, clusterState, null)
        );
    }

    public void testWriteIncrementalMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(Collections.emptyList()).build();

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        );
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    /*
     * Here we will verify the migration of manifest file from codec V0.
     *
     * Initially codec version is 0 and global metadata is also null, we will perform index metadata update.
     * In final manifest codec version should be 2 and have metadata files updated,
     * even if it was not changed in this cluster state update
     */
    public void testMigrationFromCodecV0ManifestToCodecV2Manifest() throws IOException {
        verifyCodecMigrationManifest(ClusterMetadataManifest.CODEC_V0);
    }

    /*
     * Here we will verify the migration of manifest file from codec V1.
     *
     * Initially codec version is 1 and a global metadata file is there, we will perform index metadata update.
     * In final manifest codec version should be 2 and have metadata files updated,
     * even if it was not changed in this cluster state update
     */
    public void testMigrationFromCodecV1ManifestToCodecV2Manifest() throws IOException {
        verifyCodecMigrationManifest(ClusterMetadataManifest.CODEC_V1);
    }

    private void verifyCodecMigrationManifest(int previousCodec) throws IOException {
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .nodes(nodesWithLocalNodeClusterManager())
            .build();

        // Update only index metadata
        final IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(0).build();
        Metadata newMetadata = Metadata.builder(previousClusterState.metadata()).put(indexMetadata, true).build();
        ClusterState newClusterState = ClusterState.builder(previousClusterState).metadata(newMetadata).build();

        // previous manifest with codec 0 and null global metadata
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .codecVersion(previousCodec)
            .globalMetadataFileName(null)
            .indices(Collections.emptyList())
            .build();

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifestAfterUpdate = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            newClusterState,
            previousManifest
        );

        // global metadata is updated
        assertThat(manifestAfterUpdate.hasMetadataAttributesFiles(), is(true));
        // Manifest file with codec version with 1 is updated.
        assertThat(manifestAfterUpdate.getCodecVersion(), is(MANIFEST_CURRENT_CODEC_VERSION));
    }

    public void testWriteIncrementalGlobalMetadataFromCodecV0Success() throws IOException {
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(Collections.emptyList()).build();

        verifyWriteIncrementalGlobalMetadataFromOlderCodecSuccess(previousManifest);
    }

    public void testWriteIncrementalGlobalMetadataFromCodecV1Success() throws IOException {
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .codecVersion(1)
            .globalMetadataFileName("global-metadata-file")
            .indices(Collections.emptyList())
            .build();

        verifyWriteIncrementalGlobalMetadataFromOlderCodecSuccess(previousManifest);
    }

    private void verifyWriteIncrementalGlobalMetadataFromOlderCodecSuccess(ClusterMetadataManifest previousManifest) throws IOException {
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        );

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .codecVersion(3)
            .indices(Collections.emptyList())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertNull(manifest.getGlobalMetadataFileName());
        assertNotNull(manifest.getCoordinationMetadata());
        assertNotNull(manifest.getSettingsMetadata());
        assertNotNull(manifest.getTemplatesMetadata());
        assertNotEquals(0, manifest.getCustomMetadataMap().size());

        assertEquals(expectedManifest.getClusterTerm(), manifest.getClusterTerm());
        assertEquals(expectedManifest.getStateVersion(), manifest.getStateVersion());
        assertEquals(expectedManifest.getClusterUUID(), manifest.getClusterUUID());
        assertEquals(expectedManifest.getStateUUID(), manifest.getStateUUID());
        assertEquals(expectedManifest.getCodecVersion(), manifest.getCodecVersion());
    }

    public void testCoordinationMetadataOnlyUpdated() throws IOException {
        // Updating the voting config, as updating the term will upload the full cluster state and other files will also get updated
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .coordinationMetadata(
                        CoordinationMetadata.builder(initialClusterState.coordinationMetadata())
                            .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("excludedNodeId", "excludedNodeName"))
                            .build()
                    )
                    .build()
            )
            .build();
        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
            });

            // coordination metadata file would have changed
            assertFalse(
                metadataAfterUpdate.getCoordinationMetadata()
                    .getUploadedFilename()
                    .equalsIgnoreCase(initialMetadata.getCoordinationMetadata().getUploadedFilename())
            );
            // Other files will be equal
            assertEquals(
                metadataAfterUpdate.getSettingsMetadata().getUploadedFilename(),
                initialMetadata.getSettingsMetadata().getUploadedFilename()
            );
            assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
        });
    }

    public void testSettingsMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata()).persistentSettings(Settings.builder().put("foo", "bar").build()).build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
            });

            // setting metadata file would have changed
            assertFalse(
                metadataAfterUpdate.getSettingsMetadata()
                    .getUploadedFilename()
                    .equalsIgnoreCase(initialMetadata.getSettingsMetadata().getUploadedFilename())
            );
            assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
            assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
        });
    }

    public void testTemplatesMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .templates(
                        TemplatesMetadata.builder()
                            .put(
                                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                                    .patterns(Arrays.asList("bar-*", "foo-*"))
                                    .settings(
                                        Settings.builder()
                                            .put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
            });

            // template metadata file would have changed
            assertFalse(
                metadataAfterUpdate.getTemplatesMetadata()
                    .getUploadedFilename()
                    .equalsIgnoreCase(initialMetadata.getTemplatesMetadata().getUploadedFilename())
            );
            assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
            assertEquals(metadataAfterUpdate.getSettingsMetadata(), initialMetadata.getSettingsMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
        });
    }

    public void testCustomMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom("custom_metadata_type", new CustomMetadata1("mock_custom_metadata"))
                    .build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
                // custom metadata map would have changed
                assertNotEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
                assertEquals(initialMetadata.getCustomMetadataMap().size() + 1, metadataAfterUpdate.getCustomMetadataMap().size());
                initialMetadata.getCustomMetadataMap().forEach((k, v) -> {
                    assertTrue(metadataAfterUpdate.getCustomMetadataMap().containsKey(k));
                    assertEquals(v, metadataAfterUpdate.getCustomMetadataMap().get(k));
                });
                assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
                assertEquals(metadataAfterUpdate.getSettingsMetadata(), initialMetadata.getSettingsMetadata());
                assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            });
        });
    }

    public void testCustomMetadataDeletedUpdatedAndAdded() throws IOException {
        // setup
        mockBlobStoreObjects();

        // Initial cluster state with index.
        final ClusterState initialClusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest initialManifest = remoteClusterStateService.writeFullMetadata(initialClusterState, "_na_");

        ClusterState clusterState1 = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom("custom1", new CustomMetadata1("mock_custom_metadata1"))
                    .putCustom("custom2", new CustomMetadata1("mock_custom_metadata2"))
                    .putCustom("custom3", new CustomMetadata1("mock_custom_metadata3"))
            )
            .build();

        ClusterMetadataManifest manifest1 = remoteClusterStateService.writeIncrementalMetadata(
            initialClusterState,
            clusterState1,
            initialManifest
        );
        // remove custom1 from the cluster state, update custom2, custom3 is at it is, added custom4
        ClusterState clusterState2 = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom("custom2", new CustomMetadata1("mock_updated_custom_metadata"))
                    .putCustom("custom3", new CustomMetadata1("mock_custom_metadata3"))
                    .putCustom("custom4", new CustomMetadata1("mock_custom_metadata4"))
            )
            .build();
        ClusterMetadataManifest manifest2 = remoteClusterStateService.writeIncrementalMetadata(clusterState1, clusterState2, manifest1);
        // custom1 is removed
        assertFalse(manifest2.getCustomMetadataMap().containsKey("custom1"));
        // custom2 is updated
        assertNotEquals(manifest1.getCustomMetadataMap().get("custom2"), manifest2.getCustomMetadataMap().get("custom2"));
        // custom3 is unchanged
        assertEquals(manifest1.getCustomMetadataMap().get("custom3"), manifest2.getCustomMetadataMap().get("custom3"));
        // custom4 is added
        assertTrue(manifest2.getCustomMetadataMap().containsKey("custom4"));
        assertFalse(manifest1.getCustomMetadataMap().containsKey("custom4"));
    }

    /*
     * Here we will verify global metadata is not uploaded again if change is only in index metadata
     */
    public void testIndexMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialState) -> ClusterState.builder(initialState)
            .metadata(
                Metadata.builder(initialState.metadata())
                    .put(
                        IndexMetadata.builder("test" + randomAlphaOfLength(3))
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                                    .build()
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
            assertEquals(metadataAfterUpdate.getSettingsMetadata(), initialMetadata.getSettingsMetadata());
            assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
            assertEquals(initialMetadata.getIndices().size() + 1, metadataAfterUpdate.getIndices().size());
        });
    }

    public void testIndexMetadataDeletedUpdatedAndAdded() throws IOException {
        // setup
        mockBlobStoreObjects();

        // Initial cluster state with index.
        final ClusterState initialClusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest initialManifest = remoteClusterStateService.writeFullMetadata(initialClusterState, "_na_");
        String initialIndex = "test-index";
        Index index1 = new Index("test-index-1", "index-uuid-1");
        Index index2 = new Index("test-index-2", "index-uuid-2");
        Settings idxSettings1 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
            .build();
        IndexMetadata indexMetadata1 = new IndexMetadata.Builder(index1.getName()).settings(idxSettings1)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Settings idxSettings2 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index2.getUUID())
            .build();
        IndexMetadata indexMetadata2 = new IndexMetadata.Builder(index2.getName()).settings(idxSettings2)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState1 = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.getMetadata())
                    .put(indexMetadata1, true)
                    .put(indexMetadata2, true)
                    .remove(initialIndex)
                    .build()
            )
            .build();
        ClusterMetadataManifest manifest1 = remoteClusterStateService.writeIncrementalMetadata(
            initialClusterState,
            clusterState1,
            initialManifest
        );
        // verify that initial index is removed, and new index are added
        assertEquals(1, initialManifest.getIndices().size());
        assertEquals(2, manifest1.getIndices().size());
        assertTrue(initialManifest.getIndices().stream().anyMatch(indexMetadata -> indexMetadata.getIndexName().equals(initialIndex)));
        assertFalse(manifest1.getIndices().stream().anyMatch(indexMetadata -> indexMetadata.getIndexName().equals(initialIndex)));
        // update index1, index2 is unchanged
        indexMetadata1 = new IndexMetadata.Builder(indexMetadata1).version(indexMetadata1.getVersion() + 1).build();
        ClusterState clusterState2 = ClusterState.builder(clusterState1)
            .metadata(Metadata.builder(clusterState1.getMetadata()).put(indexMetadata1, true).build())
            .build();
        ClusterMetadataManifest manifest2 = remoteClusterStateService.writeIncrementalMetadata(clusterState1, clusterState2, manifest1);
        // index1 is updated
        assertEquals(2, manifest2.getIndices().size());
        assertEquals(
            1,
            manifest2.getIndices().stream().filter(uploadedIndex -> uploadedIndex.getIndexName().equals(index1.getName())).count()
        );
        assertNotEquals(
            manifest2.getIndices()
                .stream()
                .filter(uploadedIndex -> uploadedIndex.getIndexName().equals(index1.getName()))
                .findFirst()
                .get()
                .getUploadedFilename(),
            manifest1.getIndices()
                .stream()
                .filter(uploadedIndex -> uploadedIndex.getIndexName().equals(index1.getName()))
                .findFirst()
                .get()
                .getUploadedFilename()
        );
    }

    private void verifyMetadataAttributeOnlyUpdated(
        Function<ClusterState, ClusterState> clusterStateUpdater,
        BiConsumer<ClusterMetadataManifest, ClusterMetadataManifest> assertions
    ) throws IOException {
        // setup
        mockBlobStoreObjects();

        // Initial cluster state with index.
        final ClusterState initialClusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest initialManifest = remoteClusterStateService.writeFullMetadata(initialClusterState, "_na_");

        ClusterState newClusterState = clusterStateUpdater.apply(initialClusterState);

        // updating remote cluster state with global metadata
        final ClusterMetadataManifest manifestAfterMetadataUpdate;
        if (initialClusterState.term() == newClusterState.term()) {
            manifestAfterMetadataUpdate = remoteClusterStateService.writeIncrementalMetadata(
                initialClusterState,
                newClusterState,
                initialManifest
            );
        } else {
            manifestAfterMetadataUpdate = remoteClusterStateService.writeFullMetadata(newClusterState, initialClusterState.stateUUID());
        }

        assertions.accept(initialManifest, manifestAfterMetadataUpdate);
    }

    public void testReadLatestMetadataManifestFailedIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenThrow(IOException.class);

        remoteClusterStateService.start();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while fetching latest manifest file for remote cluster state");
    }

    public void testReadLatestMetadataManifestFailedNoManifestFileInRemote() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(List.of());

        remoteClusterStateService.start();
        Optional<ClusterMetadataManifest> manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );
        assertEquals(manifest, Optional.empty());
    }

    public void testReadLatestMetadataManifestFailedManifestFileRemoveAfterFetchInRemote() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        BlobMetadata blobMetadata = new PlainBlobMetadata("manifestFileName", 1);
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(Arrays.asList(blobMetadata));
        when(blobContainer.readBlob("manifestFileName")).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.start();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while downloading cluster metadata - manifestFileName");
    }

    public void testReadLatestMetadataManifestSuccessButNoIndexMetadata() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .codecVersion(ClusterMetadataManifest.CODEC_V0)
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainer(blobContainer, expectedManifest, Map.of());

        remoteClusterStateService.start();
        assertEquals(
            remoteClusterStateService.getLatestClusterState(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID())
                .getMetadata()
                .getIndices()
                .size(),
            0
        );
    }

    public void testReadLatestMetadataManifestSuccessButIndexMetadataFetchIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainer(blobContainer, expectedManifest, Map.of());
        when(blobContainer.readBlob(uploadedIndexMetadata.getUploadedFilename() + ".dat")).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.start();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterState(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            ).getMetadata().getIndices()
        );
        assertEquals(e.getMessage(), "Error while downloading IndexMetadata - " + uploadedIndexMetadata.getUploadedFilename());
    }

    public void testReadLatestMetadataManifestSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .codecVersion(ClusterMetadataManifest.CODEC_V0)
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        mockBlobContainer(mockBlobStoreObjects(), expectedManifest, new HashMap<>());
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        ).get();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    public void testReadGlobalMetadata() throws IOException {
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(new NamedXContentRegistry(
            List.of(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE), IndexGraveyard::fromXContent))));
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();

        long prevClusterStateVersion = 13L;
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(prevClusterStateVersion)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .codecVersion(MANIFEST_CURRENT_CODEC_VERSION)
            .coordinationMetadata(new UploadedMetadataAttribute(COORDINATION_METADATA, "mock-coordination-file"))
            .settingMetadata(new UploadedMetadataAttribute(SETTING_METADATA, "mock-setting-file"))
            .templatesMetadata(new UploadedMetadataAttribute(TEMPLATES_METADATA, "mock-templates-file"))
            .put(IndexGraveyard.TYPE, new UploadedMetadataAttribute(IndexGraveyard.TYPE, "mock-custom-" +IndexGraveyard.TYPE+ "-file"))
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .routingTableVersion(1)
            .indicesRouting(List.of())
            .build();

        Metadata expactedMetadata = Metadata.builder().persistentSettings(Settings.builder().put("readonly", true).build()).build();
        mockBlobContainerForGlobalMetadata(mockBlobStoreObjects(), expectedManifest, expactedMetadata);

         ClusterState newClusterState = remoteClusterStateService.getLatestClusterState(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );

        assertTrue(Metadata.isGlobalStateEquals(newClusterState.getMetadata(), expactedMetadata));

        long newClusterStateVersion = newClusterState.getVersion();
        assert prevClusterStateVersion == newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is not equal to current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );
    }

    public void testReadGlobalMetadataIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        String globalIndexMetadataName = "global-metadata-file";
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .codecVersion(ClusterMetadataManifest.CODEC_V1)
            .globalMetadataFileName(globalIndexMetadataName)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        Metadata expactedMetadata = Metadata.builder().persistentSettings(Settings.builder().put("readonly", true).build()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainerForGlobalMetadata(blobContainer, expectedManifest, expactedMetadata);

        when(blobContainer.readBlob(RemoteClusterStateService.GLOBAL_METADATA_FORMAT.blobName(globalIndexMetadataName))).thenThrow(
            FileNotFoundException.class
        );

        remoteClusterStateService.start();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterState(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while downloading Global Metadata - " + globalIndexMetadataName);
    }

    public void testReadLatestIndexMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();

        final Index index = new Index("test-index", "index-uuid");
        String fileName = "metadata-" + index.getUUID();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(index.getName(), index.getUUID(), fileName);
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(11)
            .numberOfReplicas(10)
            .build();

        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .codecVersion(ClusterMetadataManifest.CODEC_V0)
            .build();

        mockBlobContainer(mockBlobStoreObjects(), expectedManifest, Map.of(index.getUUID(), indexMetadata));

        Map<String, IndexMetadata> indexMetadataMap = remoteClusterStateService.getLatestClusterState(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        ).getMetadata().getIndices();

        assertEquals(indexMetadataMap.size(), 1);
        assertEquals(indexMetadataMap.get(index.getName()).getIndex().getName(), index.getName());
        assertEquals(indexMetadataMap.get(index.getName()).getNumberOfShards(), indexMetadata.getNumberOfShards());
        assertEquals(indexMetadataMap.get(index.getName()).getNumberOfReplicas(), indexMetadata.getNumberOfReplicas());
    }

    public void testMarkLastStateAsCommittedSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(indices).build();

        final ClusterMetadataManifest manifest = remoteClusterStateService.markLastStateAsCommitted(clusterState, previousManifest);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    public void testGetValidPreviousClusterUUID() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            "cluster-uuid2"
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers);

        remoteClusterStateService.start();
        String previousClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster");
        assertThat(previousClusterUUID, equalTo("cluster-uuid3"));
    }

    public void testGetValidPreviousClusterUUIDForInvalidChain() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            "cluster-uuid2",
            "cluster-uuid5",
            "cluster-uuid4"
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers);

        remoteClusterStateService.start();
        assertThrows(IllegalStateException.class, () -> remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster"));
    }

    public void testGetValidPreviousClusterUUIDWithMultipleChains() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid3",
            "cluster-uuid1"
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, randomBoolean(), Collections.emptyMap());

        remoteClusterStateService.start();
        String previousClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster");
        assertThat(previousClusterUUID, equalTo("cluster-uuid3"));
    }

    public void testGetValidPreviousClusterUUIDWithInvalidMultipleChains() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            ClusterState.UNKNOWN_UUID
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers);

        remoteClusterStateService.start();
        assertThrows(IllegalStateException.class, () -> remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster"));
    }

    public void testGetValidPreviousClusterUUIDWhenLastUUIDUncommitted() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            "cluster-uuid2"
        );
        Map<String, Boolean> clusterUUIDCommitted = Map.of("cluster-uuid1", true, "cluster-uuid2", true, "cluster-uuid3", false);
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, clusterUUIDCommitted);

        remoteClusterStateService.start();
        String previousClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster");
        assertThat(previousClusterUUID, equalTo("cluster-uuid2"));
    }

    public void testRemoteStateStats() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid");

        assertTrue(remoteClusterStateService.getStats() != null);
        assertEquals(1, remoteClusterStateService.getStats().getSuccessCount());
        assertEquals(0, remoteClusterStateService.getStats().getCleanupAttemptFailedCount());
        assertEquals(0, remoteClusterStateService.getStats().getFailedCount());
    }

    public void testFileNames() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        String indexMetadataFileName = RemoteClusterStateService.indexMetadataFileName(indexMetadata);
        String[] splittedIndexMetadataFileName = indexMetadataFileName.split(DELIMITER);
        assertThat(indexMetadataFileName.split(DELIMITER).length, is(4));
        assertThat(splittedIndexMetadataFileName[0], is(METADATA_FILE_PREFIX));
        assertThat(splittedIndexMetadataFileName[1], is(RemoteStoreUtils.invertLong(indexMetadata.getVersion())));
        assertThat(splittedIndexMetadataFileName[3], is(String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION)));

        verifyManifestFileNameWithCodec(MANIFEST_CURRENT_CODEC_VERSION);
        verifyManifestFileNameWithCodec(ClusterMetadataManifest.CODEC_V1);
        verifyManifestFileNameWithCodec(ClusterMetadataManifest.CODEC_V0);
    }

    private void verifyManifestFileNameWithCodec(int codecVersion) {
        int term = randomIntBetween(5, 10);
        int version = randomIntBetween(5, 10);
        String manifestFileName = RemoteClusterStateService.getManifestFileName(term, version, true, codecVersion);
        assertThat(manifestFileName.split(DELIMITER).length, is(6));
        String[] splittedName = manifestFileName.split(DELIMITER);
        assertThat(splittedName[0], is(MANIFEST_FILE_PREFIX));
        assertThat(splittedName[1], is(RemoteStoreUtils.invertLong(term)));
        assertThat(splittedName[2], is(RemoteStoreUtils.invertLong(version)));
        assertThat(splittedName[3], is("C"));
        assertThat(splittedName[5], is(String.valueOf(codecVersion)));

        manifestFileName = RemoteClusterStateService.getManifestFileName(term, version, false, codecVersion);
        splittedName = manifestFileName.split(DELIMITER);
        assertThat(splittedName[3], is("P"));
    }

    public void testIndexMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteClusterStateService.INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteClusterStateService.getIndexMetadataUploadTimeout()
        );

        // verify update index metadata upload timeout
        int indexMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.index_metadata.upload_timeout", indexMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(indexMetadataUploadTimeout, remoteClusterStateService.getIndexMetadataUploadTimeout().seconds());
    }

    public void testMetadataManifestUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteClusterStateService.METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
            remoteClusterStateService.getMetadataManifestUploadTimeout()
        );

        // verify update metadata manifest upload timeout
        int metadataManifestUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.metadata_manifest.upload_timeout", metadataManifestUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(metadataManifestUploadTimeout, remoteClusterStateService.getMetadataManifestUploadTimeout().seconds());
    }

    public void testGlobalMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteClusterStateService.GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteClusterStateService.getGlobalMetadataUploadTimeout()
        );

        // verify update global metadata upload timeout
        int globalMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.global_metadata.upload_timeout", globalMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(globalMetadataUploadTimeout, remoteClusterStateService.getGlobalMetadataUploadTimeout().seconds());
    }

    private void mockObjectsForGettingPreviousClusterUUID(Map<String, String> clusterUUIDsPointers) throws IOException {
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, false, Collections.emptyMap());
    }

    private void mockObjectsForGettingPreviousClusterUUID(
        Map<String, String> clusterUUIDsPointers,
        Map<String, Boolean> clusterUUIDCommitted
    ) throws IOException {
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, false, clusterUUIDCommitted);
    }

    private void mockObjectsForGettingPreviousClusterUUID(
        Map<String, String> clusterUUIDsPointers,
        boolean differGlobalMetadata,
        Map<String, Boolean> clusterUUIDCommitted
    ) throws IOException {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        BlobContainer blobContainer1 = mock(BlobContainer.class);
        BlobContainer blobContainer2 = mock(BlobContainer.class);
        BlobContainer blobContainer3 = mock(BlobContainer.class);
        BlobContainer uuidBlobContainer = mock(BlobContainer.class);
        when(blobContainer1.path()).thenReturn(blobPath);
        when(blobContainer2.path()).thenReturn(blobPath);
        when(blobContainer3.path()).thenReturn(blobPath);

        mockBlobContainerForClusterUUIDs(uuidBlobContainer, clusterUUIDsPointers.keySet());
        List<UploadedIndexMetadata> uploadedIndexMetadataList1 = List.of(
            new UploadedIndexMetadata("index1", "index-uuid1", "key1"),
            new UploadedIndexMetadata("index2", "index-uuid2", "key2")
        );
        Map<String, UploadedMetadataAttribute> customMetadataMap = new HashMap<>();
        final ClusterMetadataManifest clusterManifest1 = generateClusterMetadataManifest(
            "cluster-uuid1",
            clusterUUIDsPointers.get("cluster-uuid1"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList1,
            customMetadataMap,
            new UploadedMetadataAttribute(COORDINATION_METADATA, "key3"),
            new UploadedMetadataAttribute(SETTING_METADATA, "key4"),
            new UploadedMetadataAttribute(TEMPLATES_METADATA, "key5"),
            clusterUUIDCommitted.getOrDefault("cluster-uuid1", true)
        );
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetadata indexMetadata1 = IndexMetadata.builder("index1")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexMetadata2 = IndexMetadata.builder("index2")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Metadata metadata1 = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .build();
        Map<String, IndexMetadata> indexMetadataMap1 = Map.of("index-uuid1", indexMetadata1, "index-uuid2", indexMetadata2);
        mockBlobContainerForGlobalMetadata(blobContainer1, clusterManifest1, metadata1);
        mockBlobContainer(blobContainer1, clusterManifest1, indexMetadataMap1, ClusterMetadataManifest.CODEC_V3);

        List<UploadedIndexMetadata> uploadedIndexMetadataList2 = List.of(
            new UploadedIndexMetadata("index1", "index-uuid1", "key1"),
            new UploadedIndexMetadata("index2", "index-uuid2", "key2")
        );
        final ClusterMetadataManifest clusterManifest2 = generateClusterMetadataManifest(
            "cluster-uuid2",
            clusterUUIDsPointers.get("cluster-uuid2"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList2,
            customMetadataMap,
            new UploadedMetadataAttribute(COORDINATION_METADATA, "key3"),
            new UploadedMetadataAttribute(SETTING_METADATA, "key4"),
            new UploadedMetadataAttribute(TEMPLATES_METADATA, "key5"),
            clusterUUIDCommitted.getOrDefault("cluster-uuid2", true)
        );
        IndexMetadata indexMetadata3 = IndexMetadata.builder("index1")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexMetadata4 = IndexMetadata.builder("index2")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Metadata metadata2 = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .build();
        Map<String, IndexMetadata> indexMetadataMap2 = Map.of("index-uuid1", indexMetadata3, "index-uuid2", indexMetadata4);
        mockBlobContainerForGlobalMetadata(blobContainer2, clusterManifest2, metadata2);
        mockBlobContainer(blobContainer2, clusterManifest2, indexMetadataMap2, ClusterMetadataManifest.CODEC_V3);

        // differGlobalMetadata controls which one of IndexMetadata or Metadata object would be different
        // when comparing cluster-uuid3 and cluster-uuid1 state.
        // if set true, only Metadata will differ b/w cluster uuid1 and cluster uuid3.
        // If set to false, only IndexMetadata would be different
        // Adding difference in EXACTLY on of these randomly will help us test if our uuid trimming logic compares both
        // IndexMetadata and Metadata when deciding if the remote state b/w two different cluster uuids is same.
        List<UploadedIndexMetadata> uploadedIndexMetadataList3 = differGlobalMetadata
            ? new ArrayList<>(uploadedIndexMetadataList1)
            : List.of(new UploadedIndexMetadata("index1", "index-uuid1", "key1"));
        IndexMetadata indexMetadata5 = IndexMetadata.builder("index1")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Map<String, IndexMetadata> indexMetadataMap3 = differGlobalMetadata
            ? new HashMap<>(indexMetadataMap1)
            : Map.of("index-uuid1", indexMetadata5);
        Metadata metadata3 = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), !differGlobalMetadata).build())
            .build();
        final ClusterMetadataManifest clusterManifest3 = generateClusterMetadataManifest(
            "cluster-uuid3",
            clusterUUIDsPointers.get("cluster-uuid3"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList3,
            customMetadataMap,
            new UploadedMetadataAttribute(COORDINATION_METADATA, "key3"),
            new UploadedMetadataAttribute(SETTING_METADATA, "key4"),
            new UploadedMetadataAttribute(TEMPLATES_METADATA, "key5"),
            clusterUUIDCommitted.getOrDefault("cluster-uuid3", true)
        );
        mockBlobContainerForGlobalMetadata(blobContainer3, clusterManifest3, metadata3);
        mockBlobContainer(blobContainer3, clusterManifest3, indexMetadataMap3, ClusterMetadataManifest.CODEC_V3);

        ArrayList<BlobContainer> mockBlobContainerOrderedList = new ArrayList<>(
            List.of(blobContainer1, blobContainer1, blobContainer3, blobContainer3, blobContainer2, blobContainer2)
        );

        if (differGlobalMetadata) {
            mockBlobContainerOrderedList.addAll(
                List.of(blobContainer3, blobContainer1, blobContainer3, blobContainer1, blobContainer1, blobContainer3)
            );
        }
        mockBlobContainerOrderedList.addAll(
            List.of(blobContainer2, blobContainer1, blobContainer2, blobContainer1, blobContainer1, blobContainer2)
        );
        BlobContainer[] mockBlobContainerOrderedArray = new BlobContainer[mockBlobContainerOrderedList.size()];
        mockBlobContainerOrderedList.toArray(mockBlobContainerOrderedArray);
        when(blobStore.blobContainer(ArgumentMatchers.any())).thenReturn(uuidBlobContainer, mockBlobContainerOrderedArray);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
    }

    private ClusterMetadataManifest generateV1ClusterMetadataManifest(
        String clusterUUID,
        String previousClusterUUID,
        String stateUUID,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        String globalMetadataFileName,
        Boolean isUUIDCommitted
    ) {
        return ClusterMetadataManifest.builder()
            .indices(uploadedIndexMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID(stateUUID)
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(previousClusterUUID)
            .committed(true)
            .clusterUUIDCommitted(isUUIDCommitted)
            .globalMetadataFileName(globalMetadataFileName)
            .codecVersion(ClusterMetadataManifest.CODEC_V1)
            .build();
    }

    private ClusterMetadataManifest generateClusterMetadataManifest(
        String clusterUUID,
        String previousClusterUUID,
        String stateUUID,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        Map<String, UploadedMetadataAttribute> customMetadataMap,
        UploadedMetadataAttribute coordinationMetadata,
        UploadedMetadataAttribute settingsMetadata,
        UploadedMetadataAttribute templatesMetadata,
        Boolean isUUIDCommitted
    ) {
        return ClusterMetadataManifest.builder()
            .indices(uploadedIndexMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID(stateUUID)
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(previousClusterUUID)
            .committed(true)
            .clusterUUIDCommitted(isUUIDCommitted)
            .coordinationMetadata(coordinationMetadata)
            .settingMetadata(settingsMetadata)
            .templatesMetadata(templatesMetadata)
            .customMetadataMap(customMetadataMap)
            .codecVersion(MANIFEST_CURRENT_CODEC_VERSION)
            .build();
    }

    private BlobContainer mockBlobStoreObjects() {
        return mockBlobStoreObjects(BlobContainer.class);
    }

    private BlobContainer mockBlobStoreObjects(Class<? extends BlobContainer> blobContainerClazz) {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(blobContainerClazz);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        return blobContainer;
    }

    private void mockBlobContainerForClusterUUIDs(BlobContainer blobContainer, Set<String> clusterUUIDs) throws IOException {
        Map<String, BlobContainer> blobContainerMap = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            blobContainerMap.put(clusterUUID, mockBlobStoreObjects());
        }
        when(blobContainer.children()).thenReturn(blobContainerMap);
    }

    private void mockBlobContainer(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Map<String, IndexMetadata> indexMetadataMap
    ) throws IOException {
        mockBlobContainer(blobContainer, clusterMetadataManifest, indexMetadataMap, ClusterMetadataManifest.CODEC_V0);
    }

    private void mockBlobContainer(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Map<String, IndexMetadata> indexMetadataMap,
        int codecVersion
    ) throws IOException {
        String manifestFileName = codecVersion >= ClusterMetadataManifest.CODEC_V1
            ? "manifest__manifestFileName__abcd__abcd__abcd__" + codecVersion
            : "manifestFileName";
        BlobMetadata blobMetadata = new PlainBlobMetadata(manifestFileName, 1);
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(Arrays.asList(blobMetadata));

        BytesReference bytes = RemoteClusterStateService.CLUSTER_METADATA_MANIFEST_FORMAT.serialize(
            clusterMetadataManifest,
            manifestFileName,
            blobStoreRepository.getCompressor(),
            FORMAT_PARAMS
        );
        when(blobContainer.readBlob(manifestFileName)).thenReturn(new ByteArrayInputStream(bytes.streamInput().readAllBytes()));

        clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
            try {
                IndexMetadata indexMetadata = indexMetadataMap.get(uploadedIndexMetadata.getIndexUUID());
                if (indexMetadata == null) {
                    return;
                }
                String fileName = uploadedIndexMetadata.getUploadedFilename();
                when(blobContainer.readBlob(fileName + ".dat")).thenAnswer((invocationOnMock) -> {
                    BytesReference bytesIndexMetadata = RemoteClusterStateService.INDEX_METADATA_FORMAT.serialize(
                        indexMetadata,
                        fileName,
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesIndexMetadata.streamInput().readAllBytes());
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void mockBlobContainerForGlobalMetadata(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Metadata metadata
    ) throws IOException {
        int codecVersion = clusterMetadataManifest.getCodecVersion();
        String mockManifestFileName = "manifest__1__2__C__456__" + codecVersion;
        BlobMetadata blobMetadata = new PlainBlobMetadata(mockManifestFileName, 1);
        when(
            blobContainer.listBlobsByPrefixInSortedOrder(
                "manifest" + RemoteClusterStateService.DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(Arrays.asList(blobMetadata));

        BytesReference bytes = RemoteClusterStateService.CLUSTER_METADATA_MANIFEST_FORMAT.serialize(
            clusterMetadataManifest,
            mockManifestFileName,
            blobStoreRepository.getCompressor(),
            FORMAT_PARAMS
        );
        when(blobContainer.readBlob(mockManifestFileName)).thenReturn(new ByteArrayInputStream(bytes.streamInput().readAllBytes()));
        if (codecVersion >= ClusterMetadataManifest.CODEC_V2) {
            String coordinationFileName = getFileNameFromPath(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename());
            when(blobContainer.readBlob(RemoteClusterStateService.COORDINATION_METADATA_FORMAT.blobName(coordinationFileName))).thenAnswer(
                (invocationOnMock) -> {
                    BytesReference bytesReference = RemoteClusterStateService.COORDINATION_METADATA_FORMAT.serialize(
                        metadata.coordinationMetadata(),
                        coordinationFileName,
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
                }
            );

            String settingsFileName = getFileNameFromPath(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename());
            when(blobContainer.readBlob(RemoteClusterStateService.SETTINGS_METADATA_FORMAT.blobName(settingsFileName))).thenAnswer(
                (invocationOnMock) -> {
                    BytesReference bytesReference = RemoteClusterStateService.SETTINGS_METADATA_FORMAT.serialize(
                        metadata.persistentSettings(),
                        settingsFileName,
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
                }
            );

            String templatesFileName = getFileNameFromPath(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename());
            when(blobContainer.readBlob(RemoteClusterStateService.TEMPLATES_METADATA_FORMAT.blobName(templatesFileName))).thenAnswer(
                (invocationOnMock) -> {
                    BytesReference bytesReference = RemoteClusterStateService.TEMPLATES_METADATA_FORMAT.serialize(
                        metadata.templatesMetadata(),
                        templatesFileName,
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
                }
            );

            Map<String, String> customFileMap = clusterMetadataManifest.getCustomMetadataMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> getFileNameFromPath(entry.getValue().getUploadedFilename())));

            for (Map.Entry<String, String> entry : customFileMap.entrySet()) {
                String custom = entry.getKey();
                String fileName = entry.getValue();
                when(blobContainer.readBlob(RemoteClusterStateService.CUSTOM_METADATA_FORMAT.blobName(fileName))).thenAnswer(
                    (invocation) -> {
                        BytesReference bytesReference = RemoteClusterStateService.CUSTOM_METADATA_FORMAT.serialize(
                            metadata.custom(custom),
                            fileName,
                            blobStoreRepository.getCompressor(),
                            FORMAT_PARAMS
                        );
                        return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
                    }
                );
            }
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V1) {
            String[] splitPath = clusterMetadataManifest.getGlobalMetadataFileName().split("/");
            when(blobContainer.readBlob(RemoteClusterStateService.GLOBAL_METADATA_FORMAT.blobName(splitPath[splitPath.length - 1])))
                .thenAnswer((invocationOnMock) -> {
                    BytesReference bytesGlobalMetadata = RemoteClusterStateService.GLOBAL_METADATA_FORMAT.serialize(
                        metadata,
                        "global-metadata-file",
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesGlobalMetadata.streamInput().readAllBytes());
                });
        }
    }

    private String getFileNameFromPath(String filePath) {
        String[] splitPath = filePath.split("/");
        return splitPath[splitPath.length - 1];
    }

    private static ClusterState.Builder generateClusterStateWithGlobalMetadata() {
        final Settings clusterSettings = Settings.builder().put("cluster.blocks.read_only", true).build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .persistentSettings(clusterSettings)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .build()
            );
    }

    static ClusterState.Builder generateClusterStateWithOneIndex() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final Settings settings = Settings.builder().put("mock-settings", true).build();
        final TemplatesMetadata templatesMetadata = TemplatesMetadata.EMPTY_METADATA;
        final CustomMetadata1 customMetadata1 = new CustomMetadata1("custom-metadata-1");
        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .version(randomNonNegativeLong())
                    .put(indexMetadata, true)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .persistentSettings(settings)
                    .templates(templatesMetadata)
                    .putCustom(customMetadata1.getWriteableName(), customMetadata1)
                    .build()
            );
    }

    static DiscoveryNodes nodesWithLocalNodeClusterManager() {
        return DiscoveryNodes.builder().clusterManagerNodeId("cluster-manager-id").localNodeId("cluster-manager-id").build();
    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

}
