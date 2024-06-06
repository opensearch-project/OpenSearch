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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteRoutingTableServiceTests extends OpenSearchTestCase {

    private RemoteRoutingTableService remoteRoutingTableService;
    private ClusterSettings clusterSettings;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;
    private BlobContainer blobContainer;
    private BlobPath basePath;
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

        blobStoreRepository = mock(BlobStoreRepository.class);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        blobStore = mock(BlobStore.class);
        blobContainer = mock(BlobContainer.class);
        when(repositoriesService.repository("routing_repository")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);

        basePath = BlobPath.cleanPath().add("base-path");

        remoteRoutingTableService = new RemoteRoutingTableService(
            repositoriesServiceSupplier,
            settings,
            threadPool
        );
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteRoutingTableService.close();
        threadPool.shutdown();
    }


    public void testFailInitializationWhenRemoteRoutingDisabled() {
        final Settings settings = Settings.builder().build();
        assertThrows(
            AssertionError.class,
            () -> new RemoteRoutingTableService(
                repositoriesServiceSupplier,
                settings,
                new ThreadPool(settings)
            )
        );
    }


    public void testFailStartWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("routing_repository")).thenReturn(filterRepository);
        assertThrows(AssertionError.class, () -> remoteRoutingTableService.start());
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

        assertEquals(0, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), state.getRoutingTable()).getUpserts().size());

        //Reversing order to check for equality without order.
        IndexRoutingTable indexRouting = routingTable.getIndicesRouting().get(indexName);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(indexRouting.getShards().get(0).replicaShards().get(0))
            .addShard(indexRouting.getShards().get(0).primaryShard()).build();
        ClusterState newState = ClusterState.builder(ClusterName.DEFAULT).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
        assertEquals(0, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), newState.getRoutingTable()).getUpserts().size());
    }

    public void testIndicesRoutingDiffWhenIndexDeleted() {

        ClusterState state = createIndices(randomIntBetween(1,100));
        RoutingTable routingTable = state.routingTable();

        List<String> allIndices = new ArrayList<>();
        routingTable.getIndicesRouting().forEach((k,v) -> allIndices.add(k));

        String indexNameToDelete = allIndices.get(randomIntBetween(0, allIndices.size()-1));
        RoutingTable updatedRoutingTable = RoutingTable.builder(routingTable).remove(indexNameToDelete).build();

        assertEquals(1, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().size());
        assertEquals(indexNameToDelete, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().get(0));
    }

    public void testIndicesRoutingDiffWhenIndexDeletedAndAdded() {

        ClusterState state = createIndices(randomIntBetween(1,100));
        RoutingTable routingTable = state.routingTable();

        List<String> allIndices = new ArrayList<>();
        routingTable.getIndicesRouting().forEach((k,v) -> allIndices.add(k));

        String indexNameToDelete = allIndices.get(randomIntBetween(0, allIndices.size()-1));
        RoutingTable.Builder updatedRoutingTableBuilder = RoutingTable.builder(routingTable).remove(indexNameToDelete);

        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable updatedRoutingTable =  updatedRoutingTableBuilder.addAsNew(indexMetadata).build();

        assertEquals(1, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().size());
        assertEquals(indexNameToDelete, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().get(0));

        assertEquals(1, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getUpserts().size());
        assertTrue(RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getUpserts().containsKey(indexName));
    }

    public void testGetAsyncIndexMetadataReadAction() throws Exception {
        String indexName = randomAlphaOfLength(randomIntBetween(1,50));
        ClusterState clusterState = createClusterState(indexName);
        String uploadedFileName = String.format("index-routing/" + indexName);
        Index index = new Index(indexName, "uuid-01");

        LatchedActionListener<IndexRoutingTable> listener = mock(LatchedActionListener.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobContainer.readBlob(indexName)).thenReturn(new IndexRoutingTableInputStream(clusterState.routingTable().getIndicesRouting().get(indexName)));
        remoteRoutingTableService.start();

        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getAsyncIndexMetadataReadAction(
            uploadedFileName, index, listener
        );
        assertNotNull(runnable);
        runnable.run();

        verify(blobContainer, times(1)).readBlob(any());
        assertBusy(() -> verify(listener, times(1)).onResponse(any(IndexRoutingTable.class)));
    }

    public void testGetAsyncIndexMetadataReadActionFailureForIncorrectIndex() throws Exception {
        String indexName = randomAlphaOfLength(randomIntBetween(1,50));
        ClusterState clusterState = createClusterState(indexName);
        String uploadedFileName = String.format("index-routing/" + indexName);
        Index index = new Index("incorrect-index", "uuid-01");

        LatchedActionListener<IndexRoutingTable> listener = mock(LatchedActionListener.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobContainer.readBlob(indexName)).thenReturn(new IndexRoutingTableInputStream(clusterState.routingTable().getIndicesRouting().get(indexName)));
        remoteRoutingTableService.start();

        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getAsyncIndexMetadataReadAction(
            uploadedFileName, index, listener
        );
        assertNotNull(runnable);
        runnable.run();

        verify(blobContainer, times(1)).readBlob(any());
        assertBusy(() -> verify(listener, times(1)).onFailure(any(Exception.class)));
    }

    public void testGetAsyncIndexMetadataReadActionFailureInBlobRepo() throws Exception {
        String indexName = randomAlphaOfLength(randomIntBetween(1,50));
        ClusterState clusterState = createClusterState(indexName);
        String uploadedFileName = String.format("index-routing/" + indexName);
        Index index = new Index(indexName, "uuid-01");

        LatchedActionListener<IndexRoutingTable> listener = mock(LatchedActionListener.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        doThrow(new IOException("testing failure")).when(blobContainer).readBlob(indexName);
        remoteRoutingTableService.start();

        CheckedRunnable<IOException> runnable = remoteRoutingTableService.getAsyncIndexMetadataReadAction(
            uploadedFileName, index, listener
        );
        assertNotNull(runnable);
        runnable.run();

        verify(blobContainer, times(1)).readBlob(any());
        assertBusy(() -> verify(listener, times(1)).onFailure(any(RemoteClusterStateService.RemoteStateTransferException.class)));
    }

    public void testGetUpdatedIndexRoutingTableMetadataWhenNoChange() {
        List<String> updatedIndicesRouting = new ArrayList<>();
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRouting = randomUploadedIndexMetadataList();
        List<ClusterMetadataManifest.UploadedIndexMetadata> updatedIndexMetadata = remoteRoutingTableService.getUpdatedIndexRoutingTableMetadata(updatedIndicesRouting, indicesRouting);
        assertEquals(0, updatedIndexMetadata.size());
    }

    public void testGetUpdatedIndexRoutingTableMetadataWhenIndexIsUpdated() {
        List<String> updatedIndicesRouting = new ArrayList<>();
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRouting = randomUploadedIndexMetadataList();
        ClusterMetadataManifest.UploadedIndexMetadata expectedIndexRouting = indicesRouting.get(randomIntBetween(0, indicesRouting.size()));
        updatedIndicesRouting.add(expectedIndexRouting.getIndexName());
        List<ClusterMetadataManifest.UploadedIndexMetadata> updatedIndexMetadata = remoteRoutingTableService.getUpdatedIndexRoutingTableMetadata(updatedIndicesRouting, indicesRouting);
        assertEquals(1, updatedIndexMetadata.size());
        assertEquals(expectedIndexRouting, updatedIndexMetadata.get(0));
    }

    private ClusterState createIndices(int numberOfIndices) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for(int i=0; i< numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
            final Index index = new Index(indexName, "uuid");
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
        ).numberOfShards(randomInt(1000)).numberOfReplicas(randomInt(10)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        return ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTable).build();
    }
}
