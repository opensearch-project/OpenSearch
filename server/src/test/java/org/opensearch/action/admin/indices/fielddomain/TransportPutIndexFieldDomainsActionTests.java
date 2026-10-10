/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexFieldDomainService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.EnumSet;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutIndexFieldDomainsActionTests extends OpenSearchTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private MetadataIndexFieldDomainService fieldDomainService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ActionListener<AcknowledgedResponse> listener;
    @Mock
    private ClusterState clusterState;

    private TransportPutIndexFieldDomainsAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        action = new TransportPutIndexFieldDomainsAction(
            transportService,
            clusterService,
            threadPool,
            fieldDomainService,
            actionFilters,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );
    }

    public void testClusterManagerOperationDelegatesToMetadataService() {
        Index targetIndex = new Index("logs-000001", "index-uuid");
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(targetIndex).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer")
        );

        ArgumentCaptor<PutIndexFieldDomainsClusterStateUpdateRequest> updateRequestCaptor = ArgumentCaptor.forClass(
            PutIndexFieldDomainsClusterStateUpdateRequest.class
        );
        ArgumentCaptor<ActionListener<ClusterStateUpdateResponse>> updateListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(fieldDomainService).putFieldDomains(updateRequestCaptor.capture(), updateListenerCaptor.capture());

        action.clusterManagerOperation(request, clusterState, listener);

        PutIndexFieldDomainsClusterStateUpdateRequest updateRequest = updateRequestCaptor.getValue();
        assertThat(updateRequest.targetIndex(), sameInstance(targetIndex));
        assertThat(updateRequest.fieldDomainCustomData(), equalTo(request.fieldDomainCustomData()));
        assertThat(updateRequest.ackTimeout(), equalTo(request.timeout()));
        assertThat(updateRequest.clusterManagerNodeTimeout(), equalTo(request.clusterManagerNodeTimeout()));

        updateListenerCaptor.getValue().onResponse(new ClusterStateUpdateResponse(true));
        ArgumentCaptor<AcknowledgedResponse> responseCaptor = ArgumentCaptor.forClass(AcknowledgedResponse.class);
        verify(listener).onResponse(responseCaptor.capture());
        assertTrue(responseCaptor.getValue().isAcknowledged());
    }

    public void testClusterManagerOperationForwardsMetadataServiceFailure() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid")).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer")
        );
        RuntimeException failure = new RuntimeException("boom");

        ArgumentCaptor<ActionListener<ClusterStateUpdateResponse>> updateListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(fieldDomainService).putFieldDomains(any(), updateListenerCaptor.capture());

        action.clusterManagerOperation(request, clusterState, listener);

        updateListenerCaptor.getValue().onFailure(failure);
        verify(listener).onFailure(failure);
    }

    public void testCheckBlockAllowsUnblockedState() {
        Index targetIndex = new Index("logs-000001", "index-uuid");
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(targetIndex).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer")
        );

        assertNull(action.checkBlock(request, clusterState(targetIndex, ClusterBlocks.EMPTY_CLUSTER_BLOCK)));
    }

    public void testCheckBlockRejectsGlobalMetadataWriteBlock() {
        Index targetIndex = new Index("logs-000001", "index-uuid");
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(targetIndex).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer")
        );
        ClusterBlock metadataWriteBlock = metadataWriteBlock();
        ClusterState blockedState = clusterState(targetIndex, ClusterBlocks.builder().addGlobalBlock(metadataWriteBlock).build());

        ClusterBlockException exception = action.checkBlock(request, blockedState);

        assertNotNull(exception);
        assertTrue(exception.blocks().contains(metadataWriteBlock));
    }

    public void testCheckBlockRejectsIndexMetadataWriteBlock() {
        Index targetIndex = new Index("logs-000001", "index-uuid");
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(targetIndex).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer")
        );
        ClusterBlock metadataWriteBlock = metadataWriteBlock();
        ClusterState blockedState = clusterState(
            targetIndex,
            ClusterBlocks.builder().addIndexBlock(targetIndex.getName(), metadataWriteBlock).build()
        );

        ClusterBlockException exception = action.checkBlock(request, blockedState);

        assertNotNull(exception);
        assertTrue(exception.blocks().contains(metadataWriteBlock));
    }

    public void testResolveIndicesReturnsExactTargetIndex() {
        Index targetIndex = new Index("logs-000001", "index-uuid");
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(targetIndex).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer")
        );

        assertThat(action.resolveIndices(request), equalTo(ResolvedIndices.of(targetIndex)));
    }

    private static ClusterState clusterState(Index targetIndex, ClusterBlocks blocks) {
        IndexMetadata indexMetadata = IndexMetadata.builder(targetIndex.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, targetIndex.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, true)).blocks(blocks).build();
    }

    private static ClusterBlock metadataWriteBlock() {
        return new ClusterBlock(
            1,
            "metadata write block",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );
    }
}
