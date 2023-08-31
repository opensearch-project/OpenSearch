/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
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
import java.util.Map;
import java.util.function.Supplier;

import org.mockito.ArgumentMatchers;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterStateServiceTests extends OpenSearchTestCase {

    private RemoteClusterStateService remoteClusterStateService;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        final Settings settings = Settings.builder()
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_REPOSITORY_SETTING.getKey(), "remote_store_repository")
            .build();
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(new NamedXContentRegistry(new ArrayList<>()));
        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        );
    }

    public void testFailWriteFullMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().build();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState);
        Assert.assertThat(manifest, nullValue());
    }

    public void testFailInitializationWhenRemoteStateDisabled() throws IOException {
        final Settings settings = Settings.builder().build();
        assertThrows(
            AssertionError.class,
            () -> new RemoteClusterStateService(
                "test-node-id",
                repositoriesServiceSupplier,
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            )
        );
    }

    public void testFailWriteFullMetadataWhenRepositoryNotSet() {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        doThrow(new RepositoryMissingException("repository missing")).when(repositoriesService).repository("remote_store_repository");
        assertThrows(RepositoryMissingException.class, () -> remoteClusterStateService.writeFullMetadata(clusterState));
    }

    public void testFailWriteFullMetadataWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(filterRepository);
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        assertThrows(AssertionError.class, () -> remoteClusterStateService.writeFullMetadata(clusterState));
    }

    public void testWriteFullMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState);
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
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

    public void testFailWriteIncrementalMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().build();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(clusterState, clusterState, null);
        Assert.assertThat(manifest, nullValue());
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

        remoteClusterStateService.ensureRepositorySet();
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

    public void testReadLatestMetadataManifestFailedIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(
            blobContainer.listBlobsByPrefixInSortedOrder(
                "manifest" + RemoteClusterStateService.DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenThrow(IOException.class);

        remoteClusterStateService.ensureRepositorySet();
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
        when(
            blobContainer.listBlobsByPrefixInSortedOrder(
                "manifest" + RemoteClusterStateService.DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(List.of());

        remoteClusterStateService.ensureRepositorySet();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Remote Cluster State not found - " + clusterState.metadata().clusterUUID());
    }

    public void testReadLatestMetadataManifestFailedManifestFileRemoveAfterFetchInRemote() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        BlobMetadata blobMetadata = new PlainBlobMetadata("manifestFileName", 1);
        when(
            blobContainer.listBlobsByPrefixInSortedOrder(
                "manifest" + RemoteClusterStateService.DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(Arrays.asList(blobMetadata));
        when(blobContainer.readBlob("manifestFileName")).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.ensureRepositorySet();
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
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainer(blobContainer, expectedManifest, Map.of());

        remoteClusterStateService.ensureRepositorySet();
        assertEquals(
            remoteClusterStateService.getLatestIndexMetadata(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID())
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
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainer(blobContainer, expectedManifest, Map.of());
        when(blobContainer.readBlob(uploadedIndexMetadata.getUploadedFilename() + ".dat")).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.ensureRepositorySet();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestIndexMetadata(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
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
            .build();

        mockBlobContainer(mockBlobStoreObjects(), expectedManifest, new HashMap<>());
        remoteClusterStateService.ensureRepositorySet();
        final ClusterMetadataManifest manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    public void testReadLatestIndexMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.ensureRepositorySet();

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
            .build();

        mockBlobContainer(mockBlobStoreObjects(), expectedManifest, Map.of(index.getUUID(), indexMetadata));

        Map<String, IndexMetadata> indexMetadataMap = remoteClusterStateService.getLatestIndexMetadata(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );

        assertEquals(indexMetadataMap.size(), 1);
        assertEquals(indexMetadataMap.get(index.getUUID()).getIndex().getName(), index.getName());
        assertEquals(indexMetadataMap.get(index.getUUID()).getNumberOfShards(), indexMetadata.getNumberOfShards());
        assertEquals(indexMetadataMap.get(index.getUUID()).getNumberOfReplicas(), indexMetadata.getNumberOfReplicas());
    }

    public void testMarkLastStateAsCommittedSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.ensureRepositorySet();
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

    private BlobContainer mockBlobStoreObjects() {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(ArgumentMatchers.any())).thenReturn(blobContainer);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        return blobContainer;
    }

    private void mockBlobContainer(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Map<String, IndexMetadata> indexMetadataMap
    ) throws IOException {
        BlobMetadata blobMetadata = new PlainBlobMetadata("manifestFileName", 1);
        when(
            blobContainer.listBlobsByPrefixInSortedOrder(
                "manifest" + RemoteClusterStateService.DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            )
        ).thenReturn(Arrays.asList(blobMetadata));

        BytesReference bytes = RemoteClusterStateService.CLUSTER_METADATA_MANIFEST_FORMAT.serialize(
            clusterMetadataManifest,
            "manifestFileName",
            blobStoreRepository.getCompressor()
        );
        when(blobContainer.readBlob("manifestFileName")).thenReturn(new ByteArrayInputStream(bytes.streamInput().readAllBytes()));

        clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
            try {
                IndexMetadata indexMetadata = indexMetadataMap.get(uploadedIndexMetadata.getIndexUUID());
                if (indexMetadata == null) {
                    return;
                }
                String fileName = uploadedIndexMetadata.getUploadedFilename();
                BytesReference bytesIndexMetadata = RemoteClusterStateService.INDEX_METADATA_FORMAT.serialize(
                    indexMetadata,
                    fileName,
                    blobStoreRepository.getCompressor()
                );
                when(blobContainer.readBlob(fileName + ".dat")).thenReturn(
                    new ByteArrayInputStream(bytesIndexMetadata.streamInput().readAllBytes())
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
                Metadata.builder().put(indexMetadata, true).clusterUUID("cluster-uuid").coordinationMetadata(coordinationMetadata).build()
            );
    }

    private static DiscoveryNodes nodesWithLocalNodeClusterManager() {
        return DiscoveryNodes.builder().clusterManagerNodeId("cluster-manager-id").localNodeId("cluster-manager-id").build();
    }

}
