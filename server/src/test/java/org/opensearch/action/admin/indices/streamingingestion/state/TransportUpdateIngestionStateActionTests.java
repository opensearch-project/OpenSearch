/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportUpdateIngestionStateActionTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private IndicesService indicesService;
    private ActionFilters actionFilters;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private TransportUpdateIngestionStateAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        clusterService = mock(ClusterService.class);
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, NoopTracer.INSTANCE);
        indicesService = mock(IndicesService.class);
        actionFilters = mock(ActionFilters.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        action = new TransportUpdateIngestionStateAction(
            clusterService,
            transportService,
            indicesService,
            actionFilters,
            indexNameExpressionResolver
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    public void testShards() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] { 0, 1 });
        ClusterState clusterState = mock(ClusterState.class);
        ShardsIterator shardsIterator = mock(ShardsIterator.class);
        when(clusterState.routingTable()).thenReturn(mock(org.opensearch.cluster.routing.RoutingTable.class));
        when(clusterState.routingTable().allShardsSatisfyingPredicate(any(), any())).thenReturn(shardsIterator);

        ShardsIterator result = action.shards(clusterState, request, new String[] { "test-index" });
        assertThat(result, equalTo(shardsIterator));
    }

    public void testCheckGlobalBlock() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] {});
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.blocks()).thenReturn(mock(org.opensearch.cluster.block.ClusterBlocks.class));
        when(clusterState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)).thenReturn(null);

        ClusterBlockException result = action.checkGlobalBlock(clusterState, request);
        assertThat(result, equalTo(null));
    }

    public void testCheckRequestBlock() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] {});
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.blocks()).thenReturn(mock(org.opensearch.cluster.block.ClusterBlocks.class));
        when(clusterState.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.indices())).thenReturn(null);

        ClusterBlockException result = action.checkRequestBlock(clusterState, request, new String[] { "test-index" });
        assertThat(result, equalTo(null));
    }

    public void testShardOperation() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] { 0 });
        request.setIngestionPaused(true);
        ShardRouting shardRouting = mock(ShardRouting.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        ShardIngestionState expectedState = new ShardIngestionState("test-index", 0, "PAUSED", "DROP", true);

        when(shardRouting.shardId()).thenReturn(mock(ShardId.class));
        when(shardRouting.shardId().getIndex()).thenReturn(mock(Index.class));
        when(shardRouting.shardId().id()).thenReturn(0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(mock(org.opensearch.cluster.routing.ShardRouting.class));
        when(indexShard.getIngestionState()).thenReturn(expectedState);

        ShardIngestionState result = action.shardOperation(request, shardRouting);
        assertThat(result, equalTo(expectedState));
    }

    public void testShardOperationWithShardNotFoundException() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] { 0 });
        ShardRouting shardRouting = mock(ShardRouting.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);

        when(shardRouting.shardId()).thenReturn(mock(ShardId.class));
        when(shardRouting.shardId().getIndex()).thenReturn(mock(Index.class));
        when(shardRouting.shardId().id()).thenReturn(0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(null);

        expectThrows(ShardNotFoundException.class, () -> action.shardOperation(request, shardRouting));
    }

    public void testShardOperationWithAlreadyClosedException() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] { 0 });
        ShardRouting shardRouting = mock(ShardRouting.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);

        when(shardRouting.shardId()).thenReturn(mock(ShardId.class));
        when(shardRouting.shardId().getIndex()).thenReturn(mock(Index.class));
        when(shardRouting.shardId().id()).thenReturn(0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(mock(org.opensearch.cluster.routing.ShardRouting.class));
        when(indexShard.getIngestionState()).thenThrow(new AlreadyClosedException("shard is closed"));

        expectThrows(ShardNotFoundException.class, () -> action.shardOperation(request, shardRouting));
    }

    public void testNewResponse() {
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(new String[] { "test-index" }, new int[] { 0 });
        List<ShardIngestionState> responses = Collections.singletonList(new ShardIngestionState("test-index", 0, "PAUSED", "DROP", true));
        List<DefaultShardOperationFailedException> shardFailures = Collections.emptyList();
        ClusterState clusterState = mock(ClusterState.class);

        UpdateIngestionStateResponse response = action.newResponse(request, 1, 1, 0, responses, shardFailures, clusterState);

        assertThat(response.isAcknowledged(), equalTo(true));
        assertThat(response.getTotalShards(), equalTo(1));
        assertThat(response.getSuccessfulShards(), equalTo(1));
        assertThat(response.getFailedShards(), equalTo(0));
    }
}
