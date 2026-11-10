/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportRemoteStoreMetadataActionTests extends OpenSearchTestCase {

    private TransportRemoteStoreMetadataAction action;
    private ClusterService mockClusterService;
    private TransportService mockTransportService;
    private IndexNameExpressionResolver mockResolver;
    private ActionFilters mockActionFilters;
    private RepositoriesService mockRepositoriesService;
    private ThreadPool mockThreadPool;
    private RemoteStoreSettings mockRemoteStoreSettings;

    private static final String TEST_INDEX = "test-index";
    private static final String INDEX_UUID = "uuid-test";
    private static final String SEGMENT_REPOSITORY = "segment-repo";

    @Before
    public void setup() {
        mockClusterService = mock(ClusterService.class);
        mockTransportService = mock(TransportService.class);
        mockResolver = mock(IndexNameExpressionResolver.class);
        mockActionFilters = mock(ActionFilters.class);
        mockRepositoriesService = mock(RepositoriesService.class);
        mockThreadPool = mock(ThreadPool.class);
        mockRemoteStoreSettings = mock(RemoteStoreSettings.class);

        when(mockTransportService.getTaskManager()).thenReturn(null);

        action = new TransportRemoteStoreMetadataAction(
            mockClusterService,
            mockTransportService,
            mockActionFilters,
            mockResolver,
            mockRepositoriesService,
            mockThreadPool,
            mockRemoteStoreSettings
        );
    }

    public void testEmptyIndexReturnsEmptyResponse() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices("dummy");

        when(mockClusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[0]);

        AtomicReference<RemoteStoreMetadataResponse> responseRef = new AtomicReference<>();
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(RemoteStoreMetadataResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(0, responseRef.get().getTotalShards());
        assertEquals(0, responseRef.get().getSuccessfulShards());
        assertEquals(0, responseRef.get().getFailedShards());
    }

    public void testClusterBlocked() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices(TEST_INDEX);

        ClusterBlock block = new ClusterBlock(
            1,
            "metadata_read_block",
            true,
            true,
            false,
            null,
            EnumSet.of(ClusterBlockLevel.METADATA_READ)
        );
        ClusterBlocks blocks = ClusterBlocks.builder().addGlobalBlock(block).build();
        ClusterState blockedState = ClusterState.builder(new ClusterName("test")).blocks(blocks).build();

        when(mockClusterService.state()).thenReturn(blockedState);
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { TEST_INDEX });

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(RemoteStoreMetadataResponse response) {
                fail("Expected failure due to cluster block");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        });

        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get().getMessage().toLowerCase(Locale.ROOT).contains("blocked"));
    }

    public void testRemoteStoreDisabledIndex() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices(TEST_INDEX);
        request.shards("0");

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(TEST_INDEX)
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false)
                            )
                    )
            )
            .build();

        when(mockClusterService.state()).thenReturn(state);
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { TEST_INDEX });
        when(mockClusterService.getSettings()).thenReturn(Settings.EMPTY); // FIXED LINE

        AtomicReference<RemoteStoreMetadataResponse> responseRef = new AtomicReference<>();
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(RemoteStoreMetadataResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should handle as shard failure, not exception");
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(1, responseRef.get().getTotalShards());
        assertEquals(0, responseRef.get().getSuccessfulShards());
        assertEquals(1, responseRef.get().getFailedShards());
        assertTrue(responseRef.get().getShardFailures()[0].reason().contains("Remote store not enabled"));
    }

    /**
     * A {@code segments_only} index has no translog repository at all. Reading the repository setting through its own
     * {@link org.opensearch.common.settings.Setting} rejects that as a missing value, so the shard has to report an
     * empty translog section rather than fail outright.
     */
    public void testSegmentsOnlyIndexReportsNoTranslogMetadata() throws IOException {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices(TEST_INDEX);
        request.shards("0");

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(TEST_INDEX)
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                                    .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                                    .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, SEGMENT_REPOSITORY)
                            )
                    )
            )
            .build();

        when(mockClusterService.state()).thenReturn(state);
        when(mockClusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { TEST_INDEX });
        when(mockRemoteStoreSettings.getSegmentsPathFixedPrefix()).thenReturn("");
        stubEmptySegmentRepository();

        AtomicReference<RemoteStoreMetadataResponse> responseRef = new AtomicReference<>();
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(RemoteStoreMetadataResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("a segments_only shard must not fail metadata collection", e);
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(0, responseRef.get().getFailedShards());
        assertEquals(1, responseRef.get().getSuccessfulShards());
        RemoteStoreShardMetadata shardMetadata = responseRef.get().groupByIndexAndShards().get(TEST_INDEX).get(0).get(0);
        assertTrue("a local translog has no remote metadata files", shardMetadata.getTranslogMetadataFiles().isEmpty());
        assertNull(shardMetadata.getLatestTranslogMetadataFileName());
    }

    /** A repository holding no segment metadata at all, which is enough to get past segment collection. */
    private void stubEmptySegmentRepository() throws IOException {
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.basePath()).thenReturn(new BlobPath().add("base_path"));
        when(repository.blobStore()).thenReturn(blobStore);
        when(repository.blobStore(anyBoolean())).thenReturn(blobStore);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        doAnswer(invocation -> {
            invocation.getArgument(3, ActionListener.class).onResponse(List.of());
            return null;
        }).when(blobContainer).listBlobsByPrefixInSortedOrder(any(), anyInt(), any(), any(ActionListener.class));
        when(mockRepositoriesService.repository(SEGMENT_REPOSITORY)).thenReturn(repository);
    }
}
