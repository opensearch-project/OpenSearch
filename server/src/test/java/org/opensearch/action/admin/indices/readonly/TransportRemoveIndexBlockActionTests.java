/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockResponse.RemoveBlockResult;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportRemoveIndexBlockActionTests extends OpenSearchTestCase {

    @BeforeClass
    public static void initializeIndexMetadataFirst() {
        // IndexMetadata and its nested APIBlock enum initialize each other and only work when IndexMetadata is initialized first, as it
        // always is on a running node. Do not let the first test that mentions APIBlock in this JVM decide.
        assertNotNull(IndexMetadata.INDEX_READ_ONLY_BLOCK);
    }

    @Mock
    private TransportService transportService;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private MetadataIndexStateService indexStateService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ActionListener<RemoveIndexBlockResponse> listener;

    private ClusterSettings clusterSettings;
    private TransportRemoveIndexBlockAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(DestructiveOperations.REQUIRES_NAME_SETTING));
        action = new TransportRemoveIndexBlockAction(
            transportService,
            clusterService,
            threadPool,
            indexStateService,
            actionFilters,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            new DestructiveOperations(Settings.EMPTY, clusterSettings)
        );
    }

    public void testActionName() {
        assertThat(RemoveIndexBlockAction.NAME, equalTo("indices:admin/block/remove"));
        assertThat(RemoveIndexBlockAction.INSTANCE.name(), equalTo(RemoveIndexBlockAction.NAME));
    }

    // --- block checks -------------------------------------------------------------------------------------------------------------

    public void testIndexOwnBlocksNeverStopTheRemovalOfABlock() {
        // https://github.com/opensearch-project/OpenSearch/issues/20293: with read_only and write set the settings API answers 403
        final ClusterState state = clusterState(
            ClusterBlocks.EMPTY_CLUSTER_BLOCK,
            APIBlock.READ_ONLY,
            APIBlock.READ,
            APIBlock.WRITE,
            APIBlock.METADATA
        );
        assertTrue(state.blocks().indexBlocked(ClusterBlockLevel.METADATA_WRITE, "test"));
        assertTrue(state.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test"));
        assertTrue(state.blocks().indexBlocked(ClusterBlockLevel.READ, "test"));
        for (APIBlock block : List.of(APIBlock.READ_ONLY, APIBlock.READ, APIBlock.WRITE, APIBlock.METADATA)) {
            assertNull(block.name(), action.checkBlock(new RemoveIndexBlockRequest(block, "test"), state));
        }
    }

    public void testClusterLevelMetadataWriteBlockStillApplies() {
        final ClusterState state = clusterState(ClusterBlocks.builder().addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK).build());
        final ClusterBlockException e = action.checkBlock(new RemoveIndexBlockRequest(APIBlock.WRITE, "test"), state);
        assertNotNull(e);
        assertThat(e.blocks(), hasSize(1));
        assertThat(e.blocks().iterator().next(), equalTo(Metadata.CLUSTER_READ_ONLY_BLOCK));
    }

    public void testClusterLevelBlocksWithoutMetadataWriteLevelDoNotApply() {
        final ClusterBlock readOnly = new ClusterBlock(
            99,
            "test global read block",
            true,
            false,
            false,
            RestStatus.SERVICE_UNAVAILABLE,
            EnumSet.of(ClusterBlockLevel.READ)
        );
        final ClusterState state = clusterState(ClusterBlocks.builder().addGlobalBlock(readOnly).build());
        assertNull(action.checkBlock(new RemoveIndexBlockRequest(APIBlock.WRITE, "test"), state));
    }

    public void testCheckBlockDoesNotResolveIndices() {
        // resolution problems (unknown index) are reported by the operation itself, not by the block check
        assertNull(
            action.checkBlock(new RemoveIndexBlockRequest(APIBlock.WRITE, "missing"), clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK))
        );
    }

    // --- cluster manager operation -----------------------------------------------------------------------------------------------

    public void testOperationDelegatesWithResolvedIndicesBlockAndTimeouts() throws Exception {
        final ClusterState state = clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE);
        final Index index = state.metadata().index("test").getIndex();
        final RemoveIndexBlockRequest request = new RemoveIndexBlockRequest(APIBlock.WRITE, "te*");
        request.timeout(TimeValue.timeValueSeconds(11));
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(12));
        final ArgumentCaptor<RemoveIndexBlockClusterStateUpdateRequest> updateRequest = ArgumentCaptor.forClass(
            RemoveIndexBlockClusterStateUpdateRequest.class
        );
        final ArgumentCaptor<ActionListener<RemoveIndexBlockResponse>> updateListener = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(indexStateService).removeIndexBlock(updateRequest.capture(), updateListener.capture());

        action.clusterManagerOperation(request, state, listener);

        assertThat(updateRequest.getValue().indices(), equalTo(new Index[] { index }));
        assertThat(updateRequest.getValue().getBlock(), equalTo(APIBlock.WRITE));
        assertThat(updateRequest.getValue().ackTimeout(), equalTo(TimeValue.timeValueSeconds(11)));
        assertThat(updateRequest.getValue().clusterManagerNodeTimeout(), equalTo(TimeValue.timeValueSeconds(12)));

        final RemoveIndexBlockResponse response = new RemoveIndexBlockResponse(true, List.of(new RemoveBlockResult(index)));
        updateListener.getValue().onResponse(response);
        verify(listener).onResponse(response);
    }

    public void testOperationForwardsFailures() throws Exception {
        final ClusterState state = clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE);
        final ArgumentCaptor<ActionListener<RemoveIndexBlockResponse>> updateListener = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(indexStateService).removeIndexBlock(any(), updateListener.capture());
        action.clusterManagerOperation(new RemoveIndexBlockRequest(APIBlock.WRITE, "test"), state, listener);

        final RuntimeException failure = new RuntimeException("boom");
        updateListener.getValue().onFailure(failure);

        verify(listener).onFailure(failure);
    }

    public void testOperationWithoutMatchingIndicesAcknowledgesWithoutTouchingTheClusterState() throws Exception {
        final ClusterState state = clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE);
        // wildcard that matches nothing is fine with the default allow_no_indices=true
        action.clusterManagerOperation(new RemoveIndexBlockRequest(APIBlock.WRITE, "nothing-*"), state, listener);

        final ArgumentCaptor<RemoveIndexBlockResponse> response = ArgumentCaptor.forClass(RemoveIndexBlockResponse.class);
        verify(listener).onResponse(response.capture());
        assertTrue(response.getValue().isAcknowledged());
        assertThat(response.getValue().getIndices(), hasSize(0));
        verifyNoInteractions(indexStateService);
    }

    public void testOperationFailsForUnknownIndexUnlessUnavailableIndicesAreIgnored() throws Exception {
        final ClusterState state = clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE);
        expectThrows(
            IndexNotFoundException.class,
            () -> action.clusterManagerOperation(new RemoveIndexBlockRequest(APIBlock.WRITE, "missing"), state, listener)
        );
        // wildcard without matches but allow_no_indices=false
        expectThrows(
            IndexNotFoundException.class,
            () -> action.clusterManagerOperation(
                new RemoveIndexBlockRequest(APIBlock.WRITE, "nothing-*").indicesOptions(
                    IndicesOptions.fromOptions(false, false, true, false)
                ),
                state,
                listener
            )
        );
        verifyNoInteractions(indexStateService);

        action.clusterManagerOperation(
            new RemoveIndexBlockRequest(APIBlock.WRITE, "missing").indicesOptions(IndicesOptions.fromOptions(true, true, true, false)),
            state,
            listener
        );
        final ArgumentCaptor<RemoveIndexBlockResponse> response = ArgumentCaptor.forClass(RemoveIndexBlockResponse.class);
        verify(listener).onResponse(response.capture());
        assertTrue(response.getValue().isAcknowledged());
        assertThat(response.getValue().getIndices(), hasSize(0));
        verifyNoInteractions(indexStateService);
    }

    public void testResolveIndicesForAuthorization() {
        final ClusterState state = clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE);
        when(clusterService.state()).thenReturn(state);

        final ResolvedIndices resolved = action.resolveIndices(new RemoveIndexBlockRequest(APIBlock.WRITE, "te*"));

        assertThat(resolved.local().names(), equalTo(Set.of("test")));
    }

    // --- mixed version clusters --------------------------------------------------------------------------------------------------

    public void testNodesOlderThanTheReleaseThatShipsTheActionAreRejected() {
        final DiscoveryNodes mixed = nodes(Version.CURRENT, Version.V_3_9_0);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportRemoveIndexBlockAction.ensureAllNodesSupportRemoveIndexBlock(mixed)
        );
        assertThat(e.getMessage(), containsString("indices:admin/block/remove"));
        assertThat(e.getMessage(), containsString(Version.V_3_11_0.toString()));
        assertThat(e.getMessage(), containsString(Version.V_3_9_0.toString()));
    }

    public void testClusterOfNewNodesIsAccepted() {
        TransportRemoveIndexBlockAction.ensureAllNodesSupportRemoveIndexBlock(nodes(Version.CURRENT));
        TransportRemoveIndexBlockAction.ensureAllNodesSupportRemoveIndexBlock(nodes(Version.V_3_11_0, Version.CURRENT));
    }

    public void testRequestIsNotForwardedInAMixedVersionCluster() {
        final ClusterState state = ClusterState.builder(clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE))
            .nodes(nodes(Version.CURRENT, Version.V_3_9_0))
            .build();
        when(clusterService.state()).thenReturn(state);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.doExecute(mock(Task.class), new RemoveIndexBlockRequest(APIBlock.WRITE, "test"), listener)
        );

        assertThat(e.getMessage(), containsString(Version.V_3_11_0.toString()));
        // nothing was started: the request never reaches the cluster manager, which could be a node that lacks the action
        verifyNoInteractions(indexStateService, listener);
    }

    // --- destructive operations guard, same as the add block API -----------------------------------------------------------------

    public void testWildcardsAreRejectedWhenDestructiveOperationsRequireNames() {
        final ClusterState state = ClusterState.builder(clusterState(ClusterBlocks.EMPTY_CLUSTER_BLOCK, APIBlock.WRITE))
            .nodes(nodes(Version.CURRENT))
            .build();
        when(clusterService.state()).thenReturn(state);
        clusterSettings.applySettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true).build());

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.doExecute(mock(Task.class), new RemoveIndexBlockRequest(APIBlock.WRITE, "*"), listener)
        );
        assertThat(e.getMessage(), containsString("Wildcard expressions or all indices are not allowed"));
        verifyNoInteractions(indexStateService);
    }

    public void testReadDeserializesTheResponse() throws Exception {
        final RemoveIndexBlockResponse response = new RemoveIndexBlockResponse(
            true,
            List.of(new RemoveBlockResult(new Index("test", "uuid")))
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            final RemoveIndexBlockResponse read = action.read(out.bytes().streamInput());
            assertTrue(read.isAcknowledged());
            assertThat(read.getIndices(), hasSize(1));
            assertThat(read.getIndices().get(0).getIndex(), equalTo(new Index("test", "uuid")));
        }
    }

    // --- helpers -----------------------------------------------------------------------------------------------------------------

    private static DiscoveryNodes nodes(Version... versions) {
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < versions.length; i++) {
            builder.add(
                new DiscoveryNode(
                    "node-" + i,
                    "node-" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.emptySet(),
                    versions[i]
                )
            );
        }
        return builder.build();
    }

    /** A cluster state with one open index "test" that has the given API blocks (settings and cluster blocks in sync). */
    private static ClusterState clusterState(ClusterBlocks globalBlocks, APIBlock... indexBlocks) {
        final Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        for (APIBlock block : indexBlocks) {
            settings.put(block.settingName(), true);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).build();
        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .blocks(ClusterBlocks.builder().blocks(globalBlocks).addBlocks(indexMetadata))
            .build();
    }
}
