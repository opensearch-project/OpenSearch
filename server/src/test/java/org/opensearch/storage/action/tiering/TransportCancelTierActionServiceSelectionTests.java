/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import org.mockito.ArgumentCaptor;

import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportCancelTierAction#getTieringServiceForIndex} — the logic that picks
 * which tiering service handles a cancel. Verifies the in-memory tracking takes precedence, and that
 * when neither service tracks the index, the persisted INDEX_TIERING_STATE selects the service
 * (the failover fallback) or rejects the cancel for terminal states.
 * <p>
 * These drive the protected {@code clusterManagerOperation} (same package) and assert which service's
 * {@code cancelTiering} is invoked, since {@code getTieringServiceForIndex} itself is private.
 */
public class TransportCancelTierActionServiceSelectionTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "cancel-svc-idx";

    private TransportCancelTierAction action;
    private HotToWarmTieringService hotToWarmTieringService;
    private WarmToHotTieringService warmToHotTieringService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        hotToWarmTieringService = mock(HotToWarmTieringService.class);
        warmToHotTieringService = mock(WarmToHotTieringService.class);

        action = new TransportCancelTierAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            hotToWarmTieringService,
            warmToHotTieringService
        );
    }

    /**
     * Builds a cluster state containing the index with the given persisted tiering state, and wires the
     * index-name resolver to resolve INDEX_NAME to that index. Returns the resolved Index.
     */
    private ClusterState clusterStateWithTieringState(IndexModule.TieringState tieringState) {
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid-" + INDEX_NAME)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(INDEX_TIERING_STATE.getKey(), tieringState.toString())
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME).settings(idxSettings).build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        Index index = metadata.index(INDEX_NAME).getIndex();
        when(indexNameExpressionResolver.concreteIndices(any(ClusterState.class), any(), eq(INDEX_NAME))).thenReturn(new Index[] { index });
        return state;
    }

    private void invokeCancel(ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex(INDEX_NAME);
        action.clusterManagerOperation(request, state, listener);
    }

    /** Not tracked in memory + persisted HOT_TO_WARM → hot-to-warm service handles the cancel. */
    @SuppressWarnings("unchecked")
    public void testPersistedHotToWarm_SelectsHotToWarmService_WhenNotTracked() throws Exception {
        ClusterState state = clusterStateWithTieringState(IndexModule.TieringState.HOT_TO_WARM);
        // Neither service tracks it (mock default is false).
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        invokeCancel(state, listener);

        verify(hotToWarmTieringService).cancelTiering(any(CancelTieringRequest.class), any(), eq(state));
        verify(warmToHotTieringService, never()).cancelTiering(any(), any(), any());
    }

    /** Not tracked in memory + persisted WARM_TO_HOT → warm-to-hot service handles the cancel. */
    @SuppressWarnings("unchecked")
    public void testPersistedWarmToHot_SelectsWarmToHotService_WhenNotTracked() throws Exception {
        ClusterState state = clusterStateWithTieringState(IndexModule.TieringState.WARM_TO_HOT);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        invokeCancel(state, listener);

        verify(warmToHotTieringService).cancelTiering(any(CancelTieringRequest.class), any(), eq(state));
        verify(hotToWarmTieringService, never()).cancelTiering(any(), any(), any());
    }

    /** Not tracked in memory + terminal HOT → no service selected, cancel rejected. */
    @SuppressWarnings("unchecked")
    public void testTerminalHotNotTracked_RejectsCancel() throws Exception {
        ClusterState state = clusterStateWithTieringState(IndexModule.TieringState.HOT);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        invokeCancel(state, listener);

        verify(hotToWarmTieringService, never()).cancelTiering(any(), any(), any());
        verify(warmToHotTieringService, never()).cancelTiering(any(), any(), any());

        ArgumentCaptor<Exception> failure = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(failure.capture());
        assertTrue(failure.getValue() instanceof IllegalArgumentException);
        assertTrue(failure.getValue().getMessage().contains("is not currently undergoing tiering operation"));
    }

    /** In-memory tracking takes precedence over persisted state (even a terminal one). */
    @SuppressWarnings("unchecked")
    public void testInMemoryTrackingTakesPrecedence_OverPersistedState() throws Exception {
        // Persisted state is terminal WARM, but the H2W service still tracks it in memory → it wins.
        ClusterState state = clusterStateWithTieringState(IndexModule.TieringState.WARM);
        Index index = state.metadata().index(INDEX_NAME).getIndex();
        when(hotToWarmTieringService.isIndexBeingTiered(index)).thenReturn(true);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        invokeCancel(state, listener);

        verify(hotToWarmTieringService).cancelTiering(any(CancelTieringRequest.class), any(), eq(state));
        verify(warmToHotTieringService, never()).cancelTiering(any(), any(), any());
    }
}
