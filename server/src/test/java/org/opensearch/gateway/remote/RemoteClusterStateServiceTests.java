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
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
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
import org.opensearch.index.remote.RemoteIndexPathUploader;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.RemoteClusterStateService.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateService.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateService.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateService.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateService.MANIFEST_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteClusterStateService.METADATA_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteClusterStateService.RETAINED_MANIFESTS;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteClusterStateServiceTests extends OpenSearchTestCase {

    private RemoteClusterStateService remoteClusterStateService;
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
            clusterSettings,
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
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertThrows(
            AssertionError.class,
            () -> new RemoteClusterStateService(
                "test-node-id",
                repositoriesServiceSupplier,
                settings,
                clusterSettings,
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
    }

    public void testWriteFullMetadataInParallelSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        AtomicReference<WriteContext> capturedWriteContext = new AtomicReference<>();
        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            return null;
        }).doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            capturedWriteContext.set(writeContextArgumentCaptor.getValue());
            return null;
        }).doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
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
        assertThat(manifest.getGlobalMetadataFileName(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));

        assertEquals(actionListenerArgumentCaptor.getAllValues().size(), 3);
        assertEquals(writeContextArgumentCaptor.getAllValues().size(), 3);

        byte[] writtenBytes = capturedWriteContext.get()
            .getStreamProvider(Integer.MAX_VALUE)
            .provideStream(0)
            .getInputStream()
            .readAllBytes();
        IndexMetadata writtenIndexMetadata = RemoteClusterStateService.INDEX_METADATA_FORMAT.deserialize(
            capturedWriteContext.get().getFileName(),
            blobStoreRepository.getNamedXContentRegistry(),
            new BytesArray(writtenBytes)
        );

        assertEquals(capturedWriteContext.get().getWritePriority(), WritePriority.URGENT);
        assertEquals(writtenIndexMetadata.getNumberOfShards(), 1);
        assertEquals(writtenIndexMetadata.getNumberOfReplicas(), 0);
        assertEquals(writtenIndexMetadata.getIndex().getName(), "test-index");
        assertEquals(writtenIndexMetadata.getIndex().getUUID(), "index-uuid");
        long expectedChecksum = RemoteTransferContainer.checksumOfChecksum(new ByteArrayIndexInput("metadata-filename", writtenBytes), 8);
        if (capturedWriteContext.get().doRemoteDataIntegrityCheck()) {
            assertEquals(capturedWriteContext.get().getExpectedChecksum().longValue(), expectedChecksum);
        } else {
            assertEquals(capturedWriteContext.get().getExpectedChecksum(), null);
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
            assertTrue(e.getMessage().contains("Timed out waiting for transfer of manifest file to complete"));
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
     * Here we will verify the migration of manifest file from codec V0 and V1.
     *
     * Initially codec version is 0 and global metadata is also null, we will perform index metadata update.
     * In final manifest codec version should be 1 and
     * global metadata should be updated, even if it was not changed in this cluster state update
     */
    public void testMigrationFromCodecV0ManifestToCodecV1Manifest() throws IOException {
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
            .codecVersion(ClusterMetadataManifest.CODEC_V0)
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
        assertThat(manifestAfterUpdate.getGlobalMetadataFileName(), notNullValue());
        // Manifest file with codec version with 1 is updated.
        assertThat(manifestAfterUpdate.getCodecVersion(), is(ClusterMetadataManifest.CODEC_V1));
    }

    public void testWriteIncrementalGlobalMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .codecVersion(2)
            .globalMetadataFileName("global-metadata-file")
            .indices(Collections.emptyList())
            .build();

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        );

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(Collections.emptyList())
            .globalMetadataFileName("mock-filename")
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getGlobalMetadataFileName(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    /*
     * Here we will verify index metadata is not uploaded again if change is only in global metadata
     */
    public void testGlobalMetadataOnlyUpdated() throws IOException {
        // setup
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState initialClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata).version(randomNonNegativeLong()))
            .build();
        final ClusterMetadataManifest initialManifest = ClusterMetadataManifest.builder()
            .codecVersion(2)
            .globalMetadataFileName("global-metadata-file")
            .indices(Collections.emptyList())
            .build();
        remoteClusterStateService.start();

        // Initial cluster state with index.
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        // Updating remote cluster state with changing index metadata
        final ClusterMetadataManifest manifestAfterIndexMetadataUpdate = remoteClusterStateService.writeIncrementalMetadata(
            initialClusterState,
            clusterState,
            initialManifest
        );

        // new cluster state where only global metadata is different
        Metadata newMetadata = Metadata.builder(clusterState.metadata())
            .persistentSettings(Settings.builder().put("cluster.blocks.read_only", true).build())
            .version(randomNonNegativeLong())
            .build();
        ClusterState newClusterState = ClusterState.builder(clusterState).metadata(newMetadata).build();

        // updating remote cluster state with global metadata
        final ClusterMetadataManifest manifestAfterGlobalMetadataUpdate = remoteClusterStateService.writeIncrementalMetadata(
            clusterState,
            newClusterState,
            manifestAfterIndexMetadataUpdate
        );

        // Verify that index metadata information is same in manifest files
        assertThat(manifestAfterIndexMetadataUpdate.getIndices().size(), is(manifestAfterGlobalMetadataUpdate.getIndices().size()));
        assertThat(
            manifestAfterIndexMetadataUpdate.getIndices().get(0).getIndexName(),
            is(manifestAfterGlobalMetadataUpdate.getIndices().get(0).getIndexName())
        );
        assertThat(
            manifestAfterIndexMetadataUpdate.getIndices().get(0).getIndexUUID(),
            is(manifestAfterGlobalMetadataUpdate.getIndices().get(0).getIndexUUID())
        );

        // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata update
        assertThat(
            manifestAfterIndexMetadataUpdate.getIndices().get(0).getUploadedFilename(),
            is(manifestAfterGlobalMetadataUpdate.getIndices().get(0).getUploadedFilename())
        );

        // global metadata file would have changed
        assertFalse(
            manifestAfterIndexMetadataUpdate.getGlobalMetadataFileName()
                .equalsIgnoreCase(manifestAfterGlobalMetadataUpdate.getGlobalMetadataFileName())
        );
    }

    /*
     * Here we will verify global metadata is not uploaded again if change is only in index metadata
     */
    public void testIndexMetadataOnlyUpdated() throws IOException {
        // setup
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState initialClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();
        final ClusterMetadataManifest initialManifest = ClusterMetadataManifest.builder()
            .codecVersion(2)
            .indices(Collections.emptyList())
            .build();
        remoteClusterStateService.start();

        // Initial cluster state with global metadata.
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();

        // Updating remote cluster state with changing global metadata
        final ClusterMetadataManifest manifestAfterGlobalMetadataUpdate = remoteClusterStateService.writeIncrementalMetadata(
            initialClusterState,
            clusterState,
            initialManifest
        );

        // new cluster state where only Index metadata is different
        final IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(0).build();
        Metadata newMetadata = Metadata.builder(clusterState.metadata()).put(indexMetadata, true).build();
        ClusterState newClusterState = ClusterState.builder(clusterState).metadata(newMetadata).build();

        // updating remote cluster state with index metadata
        final ClusterMetadataManifest manifestAfterIndexMetadataUpdate = remoteClusterStateService.writeIncrementalMetadata(
            clusterState,
            newClusterState,
            manifestAfterGlobalMetadataUpdate
        );

        // Verify that global metadata information is same in manifest files after updating index Metadata
        // since timestamp is part of file name, if file name is same we can confirm that file is not update in index metadata update
        assertThat(
            manifestAfterIndexMetadataUpdate.getGlobalMetadataFileName(),
            is(manifestAfterGlobalMetadataUpdate.getGlobalMetadataFileName())
        );

        // Index metadata would have changed
        assertThat(manifestAfterGlobalMetadataUpdate.getIndices().size(), is(0));
        assertThat(manifestAfterIndexMetadataUpdate.getIndices().size(), is(1));
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
            .globalMetadataFileName("global-metadata-file")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
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
            .codecVersion(MANIFEST_CURRENT_CODEC_VERSION)
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

    public void testDeleteStaleClusterUUIDs() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
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
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
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
        Map<String, BlobContainer> blobMetadataMap = Map.of(
            "cluster-uuid1",
            mock(BlobContainer.class),
            "cluster-uuid2",
            mock(BlobContainer.class),
            "cluster-uuid3",
            mock(BlobContainer.class)
        );
        when(uuidContainerContainer.children()).thenReturn(blobMetadataMap);
        when(
            manifest2Container.listBlobsByPrefixInSortedOrder(
                MANIFEST_FILE_PREFIX + DELIMITER,
                Integer.MAX_VALUE,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(List.of(new PlainBlobMetadata("mainfest2", 1L)));
        when(
            manifest3Container.listBlobsByPrefixInSortedOrder(
                MANIFEST_FILE_PREFIX + DELIMITER,
                Integer.MAX_VALUE,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(List.of(new PlainBlobMetadata("mainfest3", 1L)));
        remoteClusterStateService.start();
        remoteClusterStateService.deleteStaleClusterUUIDs(clusterState, clusterMetadataManifest);
        try {
            assertBusy(() -> {
                verify(manifest2Container, times(1)).delete();
                verify(manifest3Container, times(1)).delete();
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public void testRemoteStateCleanupFailureStats() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        doThrow(IOException.class).when(blobContainer).delete();
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        remoteClusterStateService.start();
        remoteClusterStateService.deleteStaleUUIDsClusterMetadata("cluster1", Arrays.asList("cluster-uuid1"));
        try {
            assertBusy(() -> {
                // wait for stats to get updated
                assertTrue(remoteClusterStateService.getStats() != null);
                assertEquals(0, remoteClusterStateService.getStats().getSuccessCount());
                assertEquals(1, remoteClusterStateService.getStats().getCleanupAttemptFailedCount());
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

        int term = randomIntBetween(5, 10);
        int version = randomIntBetween(5, 10);
        String manifestFileName = RemoteClusterStateService.getManifestFileName(term, version, true);
        assertThat(manifestFileName.split(DELIMITER).length, is(6));
        String[] splittedName = manifestFileName.split(DELIMITER);
        assertThat(splittedName[0], is(MANIFEST_FILE_PREFIX));
        assertThat(splittedName[1], is(RemoteStoreUtils.invertLong(term)));
        assertThat(splittedName[2], is(RemoteStoreUtils.invertLong(version)));
        assertThat(splittedName[3], is("C"));
        assertThat(splittedName[5], is(String.valueOf(MANIFEST_CURRENT_CODEC_VERSION)));

        manifestFileName = RemoteClusterStateService.getManifestFileName(term, version, false);
        splittedName = manifestFileName.split(DELIMITER);
        assertThat(splittedName[3], is("P"));
    }

    public void testSingleConcurrentExecutionOfStaleManifestCleanup() throws Exception {
        BlobContainer blobContainer = mock(BlobContainer.class);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
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

        remoteClusterStateService.start();
        remoteClusterStateService.deleteStaleClusterMetadata("cluster-name", "cluster-uuid", RETAINED_MANIFESTS);
        remoteClusterStateService.deleteStaleClusterMetadata("cluster-name", "cluster-uuid", RETAINED_MANIFESTS);

        latch.countDown();
        assertBusy(() -> assertEquals(1, callCount.get()));
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
        final ClusterMetadataManifest clusterManifest1 = generateClusterMetadataManifest(
            "cluster-uuid1",
            clusterUUIDsPointers.get("cluster-uuid1"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList1,
            "test-metadata1",
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
        mockBlobContainer(blobContainer1, clusterManifest1, indexMetadataMap1, ClusterMetadataManifest.CODEC_V1);

        List<UploadedIndexMetadata> uploadedIndexMetadataList2 = List.of(
            new UploadedIndexMetadata("index1", "index-uuid1", "key1"),
            new UploadedIndexMetadata("index2", "index-uuid2", "key2")
        );
        final ClusterMetadataManifest clusterManifest2 = generateClusterMetadataManifest(
            "cluster-uuid2",
            clusterUUIDsPointers.get("cluster-uuid2"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList2,
            "test-metadata2",
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
        mockBlobContainer(blobContainer2, clusterManifest2, indexMetadataMap2, ClusterMetadataManifest.CODEC_V1);

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
            "test-metadata3",
            clusterUUIDCommitted.getOrDefault("cluster-uuid3", true)
        );
        mockBlobContainerForGlobalMetadata(blobContainer3, clusterManifest3, metadata3);
        mockBlobContainer(blobContainer3, clusterManifest3, indexMetadataMap3, ClusterMetadataManifest.CODEC_V1);

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

    private ClusterMetadataManifest generateClusterMetadataManifest(
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
            ? "manifest__manifestFileName__abcd__abcd__abcd__1"
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
        String mockManifestFileName = "manifest__1__2__C__456__1";
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

        String[] splitPath = clusterMetadataManifest.getGlobalMetadataFileName().split("/");
        when(blobContainer.readBlob(RemoteClusterStateService.GLOBAL_METADATA_FORMAT.blobName(splitPath[splitPath.length - 1]))).thenAnswer(
            (invocationOnMock) -> {
                BytesReference bytesGlobalMetadata = RemoteClusterStateService.GLOBAL_METADATA_FORMAT.serialize(
                    metadata,
                    "global-metadata-file",
                    blobStoreRepository.getCompressor(),
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytesGlobalMetadata.streamInput().readAllBytes());
            }
        );
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

    private static ClusterState.Builder generateClusterStateWithOneIndex() {
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

        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .version(randomNonNegativeLong())
                    .put(indexMetadata, true)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .build()
            );
    }

    private static DiscoveryNodes nodesWithLocalNodeClusterManager() {
        return DiscoveryNodes.builder().clusterManagerNodeId("cluster-manager-id").localNodeId("cluster-manager-id").build();
    }

}
