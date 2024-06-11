/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;

import static org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService.INDEX_ROUTING_FILE_PREFIX;
import static org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService.INDEX_ROUTING_PATH_TOKEN;
import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.gateway.remote.RemoteClusterStateService.DELIMITER;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteRoutingTableServiceTests extends OpenSearchTestCase {

    private InternalRemoteRoutingTableService remoteRoutingTableService;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;
    private BlobContainer blobContainer;
    private BlobPath basePath;

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);

        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), false)
            .build();

        blobStoreRepository = mock(BlobStoreRepository.class);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        blobStore = mock(BlobStore.class);
        blobContainer = mock(BlobContainer.class);
        when(repositoriesService.repository("routing_repository")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);

        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);

        basePath = BlobPath.cleanPath().add("base-path");

        remoteRoutingTableService = new InternalRemoteRoutingTableService(
            repositoriesServiceSupplier,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteRoutingTableService.close();
    }

    public void testFailInitializationWhenRemoteRoutingDisabled() {
        final Settings settings = Settings.builder().build();
        assertThrows(
            AssertionError.class,
            () -> new InternalRemoteRoutingTableService(
                repositoriesServiceSupplier,
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        );
    }

    public void testFailStartWhenRepositoryNotSet() {
        doThrow(new RepositoryMissingException("repository missing")).when(repositoriesService).repository("routing_repository");
        assertThrows(RepositoryMissingException.class, () -> remoteRoutingTableService.start());
    }

    public void testFailStartWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("routing_repository")).thenReturn(filterRepository);
        assertThrows(AssertionError.class, () -> remoteRoutingTableService.start());
    }

    public void testGetIndicesRoutingMapDiff() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final Index index = new Index(indexName, "uuid");
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> diff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(routingTable, routingTable);
        assertEquals(0, diff.getUpserts().size());
        assertEquals(0, diff.getDeletes().size());

        // Reversing order to check for equality without order.
        IndexRoutingTable indexRouting = routingTable.getIndicesRouting().get(indexName);
        IndexRoutingTable indexRoutingTableReversed = IndexRoutingTable.builder(index)
            .addShard(indexRouting.getShards().get(0).replicaShards().get(0))
            .addShard(indexRouting.getShards().get(0).primaryShard())
            .build();
        RoutingTable routingTable2 = RoutingTable.builder().add(indexRoutingTableReversed).build();

        diff = remoteRoutingTableService.getIndicesRoutingMapDiff(routingTable, routingTable2);
        assertEquals(0, diff.getUpserts().size());
        assertEquals(0, diff.getDeletes().size());
    }

    public void testGetIndicesRoutingMapDiffIndexAdded() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(randomInt(1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        String indexName2 = randomAlphaOfLength(randomIntBetween(1, 50));
        int noOfShards = randomInt(1000);
        int noOfReplicas = randomInt(10);
        final IndexMetadata indexMetadata2 = new IndexMetadata.Builder(indexName2).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid2")
                .build()
        ).numberOfShards(noOfShards).numberOfReplicas(noOfReplicas).build();
        RoutingTable routingTable2 = RoutingTable.builder(routingTable).addAsNew(indexMetadata2).build();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> diff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(routingTable, routingTable2);
        assertEquals(1, diff.getUpserts().size());
        assertNotNull(diff.getUpserts().get(indexName2));
        assertEquals(noOfShards, diff.getUpserts().get(indexName2).getShards().size());

        assertEquals(0, diff.getDeletes().size());
    }

    public void testGetIndicesRoutingMapDiffShardChanged() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final Index index = new Index(indexName, "uuid");
        int noOfShards = randomInt(1000);
        int noOfReplicas = randomInt(10);
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(noOfShards).numberOfReplicas(noOfReplicas).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        final IndexMetadata indexMetadata2 = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(noOfShards + 1).numberOfReplicas(noOfReplicas).build();
        RoutingTable routingTable2 = RoutingTable.builder().addAsNew(indexMetadata2).build();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> diff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(routingTable, routingTable2);
        assertEquals(1, diff.getUpserts().size());
        assertNotNull(diff.getUpserts().get(indexName));
        assertEquals(noOfShards + 1, diff.getUpserts().get(indexName).getShards().size());
        assertEquals(noOfReplicas + 1, diff.getUpserts().get(indexName).getShards().get(0).getSize());
        assertEquals(0, diff.getDeletes().size());

        final IndexMetadata indexMetadata3 = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(noOfShards + 1).numberOfReplicas(noOfReplicas + 1).build();
        RoutingTable routingTable3 = RoutingTable.builder().addAsNew(indexMetadata3).build();

        diff = remoteRoutingTableService.getIndicesRoutingMapDiff(routingTable2, routingTable3);
        assertEquals(1, diff.getUpserts().size());
        assertNotNull(diff.getUpserts().get(indexName));
        assertEquals(noOfShards + 1, diff.getUpserts().get(indexName).getShards().size());
        assertEquals(noOfReplicas + 2, diff.getUpserts().get(indexName).getShards().get(0).getSize());

        assertEquals(0, diff.getDeletes().size());
    }

    public void testGetIndicesRoutingMapDiffShardDetailChanged() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final Index index = new Index(indexName, "uuid");
        int noOfShards = randomInt(1000);
        int noOfReplicas = randomInt(10);
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(noOfShards).numberOfReplicas(noOfReplicas).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        RoutingTable routingTable2 = RoutingTable.builder().addAsRecovery(indexMetadata).build();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> diff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(routingTable, routingTable2);
        assertEquals(1, diff.getUpserts().size());
        assertNotNull(diff.getUpserts().get(indexName));
        assertEquals(noOfShards, diff.getUpserts().get(indexName).getShards().size());
        assertEquals(noOfReplicas + 1, diff.getUpserts().get(indexName).getShards().get(0).getSize());
        assertEquals(0, diff.getDeletes().size());
    }

    public void testGetIndicesRoutingMapDiffIndexDeleted() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(randomInt(1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        String indexName2 = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata2 = new IndexMetadata.Builder(indexName2).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid2")
                .build()
        ).numberOfShards(randomInt(1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable2 = RoutingTable.builder().addAsNew(indexMetadata2).build();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> diff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(routingTable, routingTable2);
        assertEquals(1, diff.getUpserts().size());
        assertNotNull(diff.getUpserts().get(indexName2));

        assertEquals(1, diff.getDeletes().size());
        assertEquals(indexName, diff.getDeletes().get(0));
    }

    public void testGetIndexRoutingAsyncAction() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState clusterState = createClusterState(indexName);
        BlobPath expectedPath = getPath();

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = mock(LatchedActionListener.class);
        when(blobStore.blobContainer(expectedPath)).thenReturn(blobContainer);

        remoteRoutingTableService.start();
        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getIndexRoutingAsyncAction(
            clusterState,
            clusterState.routingTable().getIndicesRouting().get(indexName),
            listener,
            basePath
        );
        assertNotNull(runnable);
        runnable.run();

        String expectedFilePrefix = String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(clusterState.term()),
            RemoteStoreUtils.invertLong(clusterState.version())
        );
        verify(blobContainer, times(1)).writeBlob(startsWith(expectedFilePrefix), any(StreamInput.class), anyLong(), eq(true));
        verify(listener, times(1)).onResponse(any(ClusterMetadataManifest.UploadedMetadata.class));
    }

    public void testGetIndexRoutingAsyncActionFailureInBlobRepo() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState clusterState = createClusterState(indexName);
        BlobPath expectedPath = getPath();

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = mock(LatchedActionListener.class);
        when(blobStore.blobContainer(expectedPath)).thenReturn(blobContainer);
        doThrow(new IOException("testing failure")).when(blobContainer).writeBlob(anyString(), any(StreamInput.class), anyLong(), eq(true));

        remoteRoutingTableService.start();
        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getIndexRoutingAsyncAction(
            clusterState,
            clusterState.routingTable().getIndicesRouting().get(indexName),
            listener,
            basePath
        );
        assertNotNull(runnable);
        runnable.run();
        String expectedFilePrefix = String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(clusterState.term()),
            RemoteStoreUtils.invertLong(clusterState.version())
        );
        verify(blobContainer, times(1)).writeBlob(startsWith(expectedFilePrefix), any(StreamInput.class), anyLong(), eq(true));
        verify(listener, times(1)).onFailure(any(RemoteClusterStateService.RemoteStateTransferException.class));
    }

    public void testGetIndexRoutingAsyncActionAsyncRepo() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState clusterState = createClusterState(indexName);
        BlobPath expectedPath = getPath();

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = mock(LatchedActionListener.class);
        blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(blobStore.blobContainer(expectedPath)).thenReturn(blobContainer);
        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        ConcurrentHashMap<String, WriteContext> capturedWriteContext = new ConcurrentHashMap<>();

        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            WriteContext writeContext = writeContextArgumentCaptor.getValue();
            capturedWriteContext.put(writeContext.getFileName().split(DELIMITER)[0], writeContextArgumentCaptor.getValue());
            return null;
        }).when((AsyncMultiStreamBlobContainer) blobContainer)
            .asyncBlobUpload(writeContextArgumentCaptor.capture(), actionListenerArgumentCaptor.capture());

        remoteRoutingTableService.start();
        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getIndexRoutingAsyncAction(
            clusterState,
            clusterState.routingTable().getIndicesRouting().get(indexName),
            listener,
            basePath
        );
        assertNotNull(runnable);
        runnable.run();

        String expectedFilePrefix = String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(clusterState.term()),
            RemoteStoreUtils.invertLong(clusterState.version())
        );
        assertEquals(1, actionListenerArgumentCaptor.getAllValues().size());
        assertEquals(1, writeContextArgumentCaptor.getAllValues().size());
        assertNotNull(capturedWriteContext.get("index_routing"));
        assertEquals(capturedWriteContext.get("index_routing").getWritePriority(), WritePriority.URGENT);
        assertTrue(capturedWriteContext.get("index_routing").getFileName().startsWith(expectedFilePrefix));
    }

    public void testGetIndexRoutingAsyncActionAsyncRepoFailureInRepo() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState clusterState = createClusterState(indexName);
        BlobPath expectedPath = getPath();

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = mock(LatchedActionListener.class);
        blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(blobStore.blobContainer(expectedPath)).thenReturn(blobContainer);

        doThrow(new IOException("Testing failure")).when((AsyncMultiStreamBlobContainer) blobContainer)
            .asyncBlobUpload(any(WriteContext.class), any(ActionListener.class));

        remoteRoutingTableService.start();
        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getIndexRoutingAsyncAction(
            clusterState,
            clusterState.routingTable().getIndicesRouting().get(indexName),
            listener,
            basePath
        );
        assertNotNull(runnable);
        runnable.run();
        verify(listener, times(1)).onFailure(any(RemoteClusterStateService.RemoteStateTransferException.class));
    }

    public void testGetAllUploadedIndicesRouting() {
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().build();
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );

        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndiceRoutingMetadata = remoteRoutingTableService
            .getAllUploadedIndicesRouting(previousManifest, List.of(uploadedIndexMetadata), List.of());
        assertNotNull(allIndiceRoutingMetadata);
        assertEquals(1, allIndiceRoutingMetadata.size());
        assertEquals(uploadedIndexMetadata, allIndiceRoutingMetadata.get(0));
    }

    public void testGetAllUploadedIndicesRoutingExistingIndexInManifest() {
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .indicesRouting(List.of(uploadedIndexMetadata))
            .build();

        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndiceRoutingMetadata = remoteRoutingTableService
            .getAllUploadedIndicesRouting(previousManifest, List.of(uploadedIndexMetadata), List.of());
        assertNotNull(allIndiceRoutingMetadata);
        assertEquals(1, allIndiceRoutingMetadata.size());
        assertEquals(uploadedIndexMetadata, allIndiceRoutingMetadata.get(0));
    }

    public void testGetAllUploadedIndicesRoutingNewIndexFromManifest() {
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .indicesRouting(List.of(uploadedIndexMetadata))
            .build();
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata2 = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index2",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );

        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndiceRoutingMetadata = remoteRoutingTableService
            .getAllUploadedIndicesRouting(previousManifest, List.of(uploadedIndexMetadata2), List.of());
        assertNotNull(allIndiceRoutingMetadata);
        assertEquals(2, allIndiceRoutingMetadata.size());
        assertEquals(uploadedIndexMetadata, allIndiceRoutingMetadata.get(0));
        assertEquals(uploadedIndexMetadata2, allIndiceRoutingMetadata.get(1));
    }

    public void testGetAllUploadedIndicesRoutingIndexDeleted() {
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata2 = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index2",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .indicesRouting(List.of(uploadedIndexMetadata, uploadedIndexMetadata2))
            .build();

        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndiceRoutingMetadata = remoteRoutingTableService
            .getAllUploadedIndicesRouting(previousManifest, List.of(uploadedIndexMetadata2), List.of("test-index"));
        assertNotNull(allIndiceRoutingMetadata);
        assertEquals(1, allIndiceRoutingMetadata.size());
        assertEquals(uploadedIndexMetadata2, allIndiceRoutingMetadata.get(0));
    }

    public void testGetAllUploadedIndicesRoutingNoChange() {
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata2 = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index2",
            "index-uuid",
            "index-filename",
            InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .indicesRouting(List.of(uploadedIndexMetadata, uploadedIndexMetadata2))
            .build();

        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndiceRoutingMetadata = remoteRoutingTableService
            .getAllUploadedIndicesRouting(previousManifest, List.of(), List.of());
        assertNotNull(allIndiceRoutingMetadata);
        assertEquals(2, allIndiceRoutingMetadata.size());
        assertEquals(uploadedIndexMetadata, allIndiceRoutingMetadata.get(0));
        assertEquals(uploadedIndexMetadata2, allIndiceRoutingMetadata.get(1));
    }

    private ClusterState createClusterState(String indexName) {
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(randomInt(1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(routingTable)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(1L).build()))
            .version(2L)
            .build();
    }

    private BlobPath getPath() {
        BlobPath indexRoutingPath = basePath.add(INDEX_ROUTING_PATH_TOKEN);
        return RemoteStoreEnums.PathType.HASHED_PREFIX.path(
            RemoteStorePathStrategy.BasePathInput.builder().basePath(indexRoutingPath).indexUUID("uuid").build(),
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64
        );
    }
}
