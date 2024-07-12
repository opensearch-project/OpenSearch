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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.TestCapturingListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import org.mockito.Mockito;

import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.gateway.remote.ClusterMetadataManifestTests.randomUploadedIndexMetadataList;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_FILE;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_METADATA_PREFIX;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private Compressor compressor;
    private BlobStoreTransferService blobStoreTransferService;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);

        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = mock(ClusterService.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        blobStoreRepository = mock(BlobStoreRepository.class);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        blobStore = mock(BlobStore.class);
        blobContainer = mock(BlobContainer.class);
        when(repositoriesService.repository(anyString())).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        compressor = new NoneCompressor();
        basePath = BlobPath.cleanPath().add("base-path");
        when(blobStoreRepository.basePath()).thenReturn(basePath);
        remoteRoutingTableService = new InternalRemoteRoutingTableService(
            repositoriesServiceSupplier,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            "test-cluster"
        );
        remoteRoutingTableService.doStart();
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteRoutingTableService.doClose();
        threadPool.shutdown();
    }

    public void testFailInitializationWhenRemoteRoutingDisabled() {
        final Settings settings = Settings.builder().build();
        assertThrows(
            AssertionError.class,
            () -> new InternalRemoteRoutingTableService(
                repositoriesServiceSupplier,
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                "test-cluster"
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

    public void testGetChangedIndicesRouting() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final Index index = new Index(indexName, "uuid");
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTable).build();

        assertEquals(
            0,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), state.getRoutingTable()).getUpserts().size()
        );

        // Reversing order to check for equality without order.
        IndexRoutingTable indexRouting = routingTable.getIndicesRouting().get(indexName);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(indexRouting.getShards().get(0).replicaShards().get(0))
            .addShard(indexRouting.getShards().get(0).primaryShard())
            .build();
        ClusterState newState = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
        assertEquals(
            0,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), newState.getRoutingTable()).getUpserts().size()
        );
    }

    public void testGetIndicesRoutingMapDiffIndexAdded() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(between(1, 1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        String indexName2 = randomAlphaOfLength(randomIntBetween(1, 50));
        int noOfShards = between(1, 1000);
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
        int noOfShards = between(1, 1000);
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
        int noOfShards = between(1, 1000);
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
        ).numberOfShards(between(1, 1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();

        String indexName2 = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata2 = new IndexMetadata.Builder(indexName2).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid2")
                .build()
        ).numberOfShards(between(1, 1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable2 = RoutingTable.builder().addAsNew(indexMetadata2).build();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> diff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(routingTable, routingTable2);
        assertEquals(1, diff.getUpserts().size());
        assertNotNull(diff.getUpserts().get(indexName2));

        assertEquals(1, diff.getDeletes().size());
        assertEquals(indexName, diff.getDeletes().get(0));
    }

    public void testGetAllUploadedIndicesRouting() {
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().build();
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "index-filename",
            INDEX_ROUTING_METADATA_PREFIX
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
            INDEX_ROUTING_METADATA_PREFIX
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
            INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .indicesRouting(List.of(uploadedIndexMetadata))
            .build();
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata2 = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index2",
            "index-uuid",
            "index-filename",
            INDEX_ROUTING_METADATA_PREFIX
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
            INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata2 = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index2",
            "index-uuid",
            "index-filename",
            INDEX_ROUTING_METADATA_PREFIX
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
            INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata2 = new ClusterMetadataManifest.UploadedIndexMetadata(
            "test-index2",
            "index-uuid",
            "index-filename",
            INDEX_ROUTING_METADATA_PREFIX
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

    public void testIndicesRoutingDiffWhenIndexDeleted() {

        ClusterState state = createIndices(randomIntBetween(1, 100));
        RoutingTable routingTable = state.routingTable();

        List<String> allIndices = new ArrayList<>();
        routingTable.getIndicesRouting().forEach((k, v) -> allIndices.add(k));

        String indexNameToDelete = allIndices.get(randomIntBetween(0, allIndices.size() - 1));
        RoutingTable updatedRoutingTable = RoutingTable.builder(routingTable).remove(indexNameToDelete).build();

        assertEquals(
            1,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().size()
        );
        assertEquals(
            indexNameToDelete,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().get(0)
        );
    }

    public void testIndicesRoutingDiffWhenIndexDeletedAndAdded() {

        ClusterState state = createIndices(randomIntBetween(1, 100));
        RoutingTable routingTable = state.routingTable();

        List<String> allIndices = new ArrayList<>();
        routingTable.getIndicesRouting().forEach((k, v) -> allIndices.add(k));

        String indexNameToDelete = allIndices.get(randomIntBetween(0, allIndices.size() - 1));
        RoutingTable.Builder updatedRoutingTableBuilder = RoutingTable.builder(routingTable).remove(indexNameToDelete);

        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable updatedRoutingTable = updatedRoutingTableBuilder.addAsNew(indexMetadata).build();

        assertEquals(
            1,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().size()
        );
        assertEquals(
            indexNameToDelete,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().get(0)
        );

        assertEquals(
            1,
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getUpserts().size()
        );
        assertTrue(
            remoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable)
                .getUpserts()
                .containsKey(indexName)
        );
    }

    public void testGetAsyncIndexRoutingReadAction() throws Exception {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState clusterState = createClusterState(indexName);
        String uploadedFileName = String.format(Locale.ROOT, "index-routing/" + indexName);
        when(blobContainer.readBlob(indexName)).thenReturn(
            INDEX_ROUTING_TABLE_FORMAT.serialize(
                clusterState.getRoutingTable().getIndicesRouting().get(indexName),
                uploadedFileName,
                compressor
            ).streamInput()
        );
        TestCapturingListener<IndexRoutingTable> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteRoutingTableService.getAsyncIndexRoutingReadAction(
            "cluster-uuid",
            uploadedFileName,
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();

        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        IndexRoutingTable indexRoutingTable = listener.getResult();
        assertEquals(clusterState.getRoutingTable().getIndicesRouting().get(indexName), indexRoutingTable);
    }

    public void testGetAsyncIndexRoutingWriteAction() throws Exception {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        ClusterState clusterState = createClusterState(indexName);
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(WritePriority.URGENT), any(ActionListener.class));

        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteRoutingTableService.getAsyncIndexRoutingWriteAction(
            clusterState.metadata().clusterUUID(),
            clusterState.term(),
            clusterState.version(),
            clusterState.getRoutingTable().indicesRouting().get(indexName),
            new LatchedActionListener<>(listener, latch)
        ).run();
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();

        assertEquals(INDEX_ROUTING_METADATA_PREFIX + indexName, uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(8, pathTokens.length);
        assertEquals(pathTokens[1], "base-path");
        String[] fileNameTokens = pathTokens[7].split(DELIMITER);

        assertEquals(4, fileNameTokens.length);
        assertEquals(fileNameTokens[0], INDEX_ROUTING_FILE);
        assertEquals(fileNameTokens[1], RemoteStoreUtils.invertLong(1L));
        assertEquals(fileNameTokens[2], RemoteStoreUtils.invertLong(2L));
        assertThat(RemoteStoreUtils.invertLong(fileNameTokens[3]), lessThanOrEqualTo(System.currentTimeMillis()));
    }

    public void testGetUpdatedIndexRoutingTableMetadataWhenNoChange() {
        List<String> updatedIndicesRouting = new ArrayList<>();
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRouting = randomUploadedIndexMetadataList();
        List<ClusterMetadataManifest.UploadedIndexMetadata> updatedIndexMetadata = remoteRoutingTableService
            .getUpdatedIndexRoutingTableMetadata(updatedIndicesRouting, indicesRouting);
        assertEquals(0, updatedIndexMetadata.size());
    }

    public void testGetUpdatedIndexRoutingTableMetadataWhenIndexIsUpdated() {
        List<String> updatedIndicesRouting = new ArrayList<>();
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRouting = randomUploadedIndexMetadataList();
        ClusterMetadataManifest.UploadedIndexMetadata expectedIndexRouting = indicesRouting.get(
            randomIntBetween(0, indicesRouting.size() - 1)
        );
        updatedIndicesRouting.add(expectedIndexRouting.getIndexName());
        List<ClusterMetadataManifest.UploadedIndexMetadata> updatedIndexMetadata = remoteRoutingTableService
            .getUpdatedIndexRoutingTableMetadata(updatedIndicesRouting, indicesRouting);
        assertEquals(1, updatedIndexMetadata.size());
        assertEquals(expectedIndexRouting, updatedIndexMetadata.get(0));
    }

    private ClusterState createIndices(int numberOfIndices) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
            final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                    .build()
            ).numberOfShards(1).numberOfReplicas(1).build();

            routingTableBuilder.addAsNew(indexMetadata);
        }
        return ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTableBuilder.build()).build();
    }

    private ClusterState createClusterState(String indexName) {
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(between(1, 1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(routingTable)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(1L).build()))
            .version(2L)
            .build();
    }

    private BlobPath getPath() {
        BlobPath indexRoutingPath = basePath.add(INDEX_ROUTING_TABLE);
        return RemoteStoreEnums.PathType.HASHED_PREFIX.path(
            RemoteStorePathStrategy.PathInput.builder().basePath(indexRoutingPath).indexUUID("uuid").build(),
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64
        );
    }

    public void testDeleteStaleIndexRoutingPaths() throws IOException {
        doNothing().when(blobContainer).deleteBlobsIgnoringIfNotExists(any());
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        List<String> stalePaths = Arrays.asList("path1", "path2");
        remoteRoutingTableService.doStart();
        remoteRoutingTableService.deleteStaleIndexRoutingPaths(stalePaths);
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(stalePaths);
    }

    public void testDeleteStaleIndexRoutingPathsThrowsIOException() throws IOException {
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        List<String> stalePaths = Arrays.asList("path1", "path2");
        // Simulate an IOException
        doThrow(new IOException("test exception")).when(blobContainer).deleteBlobsIgnoringIfNotExists(Mockito.anyList());

        remoteRoutingTableService.doStart();
        IOException thrown = assertThrows(IOException.class, () -> {
            remoteRoutingTableService.deleteStaleIndexRoutingPaths(stalePaths);
        });
        assertEquals("test exception", thrown.getMessage());
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(stalePaths);
    }

}
