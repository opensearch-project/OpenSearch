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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportRemoteStoreMetadataActionTests extends OpenSearchTestCase {

    private TransportRemoteStoreMetadataAction transportAction;
    private ClusterService mockClusterService;
    private TransportService mockTransportService;
    private IndicesService mockIndicesService;
    private IndexNameExpressionResolver mockResolver;
    private ActionFilters mockActionFilters;
    private RepositoriesService mockRepositoriesService;
    private ThreadPool mockThreadPool;
    private RemoteStoreSettings mockRemoteStoreSettings;

    private static final String TEST_INDEX = "test-index";
    private static final String INDEX_UUID = "test-uuid";

    @Before
    public void setup() {
        mockClusterService = mock(ClusterService.class);
        mockTransportService = mock(TransportService.class);
        mockIndicesService = mock(IndicesService.class);
        mockResolver = mock(IndexNameExpressionResolver.class);
        mockActionFilters = mock(ActionFilters.class);
        mockRepositoriesService = mock(RepositoriesService.class);
        mockThreadPool = mock(ThreadPool.class);
        mockRemoteStoreSettings = mock(RemoteStoreSettings.class);

        transportAction = new TransportRemoteStoreMetadataAction(
            mockClusterService,
            mockTransportService,
            mockIndicesService,
            mockActionFilters,
            mockResolver,
            mockRepositoriesService,
            mockThreadPool,
            mockRemoteStoreSettings
        );
    }

    public void testExecuteWithEmptyIndices() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices("nonexistent-index");

        when(mockClusterService.state()).thenReturn(createClusterState(false));
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[0]);

        AtomicReference<RemoteStoreMetadataResponse> responseRef = new AtomicReference<>();
        transportAction.doExecute(null, request, new ActionListener<>() {
            public void onResponse(RemoteStoreMetadataResponse response) {
                responseRef.set(response);
            }

            public void onFailure(Exception e) {
                fail("Unexpected failure: " + e.getMessage());
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(0, responseRef.get().getTotalShards());
        assertEquals(0, responseRef.get().getSuccessfulShards());
    }

    public void testExecuteWithNonRemoteStoreIndex() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices(TEST_INDEX);
        request.shards("0");

        when(mockClusterService.state()).thenReturn(createClusterState(false));
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { TEST_INDEX });

        AtomicReference<RemoteStoreMetadataResponse> responseRef = new AtomicReference<>();
        transportAction.doExecute(null, request, new ActionListener<>() {
            public void onResponse(RemoteStoreMetadataResponse response) {
                responseRef.set(response);
            }

            public void onFailure(Exception e) {
                fail("Unexpected failure: " + e.getMessage());
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(1, responseRef.get().getTotalShards());
        assertEquals(0, responseRef.get().getSuccessfulShards());
        assertEquals(1, responseRef.get().getFailedShards());
        assertTrue(responseRef.get().getShardFailures()[0].reason().contains("Remote store not enabled"));
    }

    public void testExecuteWithClusterBlock() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices(TEST_INDEX);

        when(mockClusterService.state()).thenReturn(createBlockedClusterState());
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { TEST_INDEX });

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        transportAction.doExecute(null, request, new ActionListener<>() {
            public void onResponse(RemoteStoreMetadataResponse response) {
                fail("Expected cluster block exception");
            }

            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        });

        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get().getMessage().toLowerCase(Locale.ROOT).contains("blocked"));
    }

    public void testExecuteWithFailure() {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices(TEST_INDEX);
        request.shards("0");

        when(mockClusterService.state()).thenReturn(createClusterState(true));
        when(mockResolver.concreteIndexNames(any(), any())).thenReturn(new String[] { TEST_INDEX });
        when(mockIndicesService.indexService(any())).thenReturn(null); // Simulate failure

        AtomicReference<RemoteStoreMetadataResponse> responseRef = new AtomicReference<>();
        transportAction.doExecute(null, request, new ActionListener<>() {
            public void onResponse(RemoteStoreMetadataResponse response) {
                responseRef.set(response);
            }

            public void onFailure(Exception e) {
                fail("Should not throw failure, expect partial shard failure response");
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(0, responseRef.get().getSuccessfulShards());
        assertEquals(1, responseRef.get().getFailedShards());
        assertTrue(responseRef.get().getShardFailures()[0].reason().toLowerCase(Locale.ROOT).contains("null"));
    }

    private ClusterState createClusterState(boolean remoteStoreEnabled) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, remoteStoreEnabled);

        if (remoteStoreEnabled) {
            settings.put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "test-repo")
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "test-repo");
        }

        IndexMetadata indexMetadata = IndexMetadata.builder(TEST_INDEX).settings(settings).numberOfShards(1).numberOfReplicas(0).build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, false)).build();
    }

    private ClusterState createBlockedClusterState() {
        ClusterBlock block = new ClusterBlock(1, "test block", true, true, false, null, EnumSet.of(ClusterBlockLevel.METADATA_READ));

        IndexMetadata indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(block))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .build();
    }
}
