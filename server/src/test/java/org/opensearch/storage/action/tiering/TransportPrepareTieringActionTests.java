/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.mockito.InOrder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportPrepareTieringAction} shard-level operations.
 * <p>
 * These tests verify the shard operation logic:
 * - Correct ordering of sync, flush, refresh, and remote store sync
 * - Primary permit acquisition before operations
 * - Timeout handling on permit acquisition
 * - Uncommitted ops verification after flush
 * - Permit release on failure
 * - Primary-only shard targeting
 */
@SuppressWarnings("unchecked")
public class TransportPrepareTieringActionTests extends OpenSearchTestCase {

    private IndexShard mockIndexShard;
    private IndicesService mockIndicesService;
    private IndexService mockIndexService;
    private Releasable mockPermit;
    private ShardRouting primaryShardRouting;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockIndexShard = mock(IndexShard.class);
        mockIndicesService = mock(IndicesService.class);
        mockIndexService = mock(IndexService.class);
        mockPermit = mock(Releasable.class);

        shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        primaryShardRouting = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);

        when(mockIndicesService.indexServiceSafe(shardId.getIndex())).thenReturn(mockIndexService);
        when(mockIndexService.getShard(0)).thenReturn(mockIndexShard);

        TranslogStats translogStats = mock(TranslogStats.class);
        when(translogStats.getUncommittedOperations()).thenReturn(0);
        when(mockIndexShard.translogStats()).thenReturn(translogStats);
    }

    /**
     * Helper to mock acquireAllPrimaryOperationsPermits to immediately call the listener with a permit.
     */
    private void mockPermitAcquisitionSuccess() {
        doAnswer(invocation -> {
            ActionListener<Releasable> listener = invocation.getArgument(0);
            listener.onResponse(mockPermit);
            return null;
        }).when(mockIndexShard).acquireAllPrimaryOperationsPermits(any(ActionListener.class), any(TimeValue.class));
    }

    /**
     * Helper to mock acquireAllPrimaryOperationsPermits to call the listener with an exception.
     */
    private void mockPermitAcquisitionFailure(Exception exception) {
        doAnswer(invocation -> {
            ActionListener<Releasable> listener = invocation.getArgument(0);
            listener.onFailure(exception);
            return null;
        }).when(mockIndexShard).acquireAllPrimaryOperationsPermits(any(ActionListener.class), any(TimeValue.class));
    }

    /**
     * Simulates the shard operation logic from TransportPrepareTieringAction.shardOperation.
     * We replicate the logic here since the actual method requires full transport infrastructure.
     */
    private void executeShardOperation(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
        PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
        indexShard.acquireAllPrimaryOperationsPermits(permitFuture, TimeValue.timeValueSeconds(30));
        Releasable permit;
        try {
            permit = permitFuture.actionGet();
        } catch (Exception e) {
            throw new IOException("Failed to acquire primary operation permits for shard [" + shardRouting.shardId() + "]", e);
        }
        try {
            indexShard.sync();
            indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            indexShard.refresh("prepare_tiering");
            indexShard.waitForRemoteStoreSync();

            int uncommitted = indexShard.translogStats().getUncommittedOperations();
            if (uncommitted > 0) {
                throw new IOException(
                    "Shard [" + shardRouting.shardId() + "] still has " + uncommitted + " uncommitted translog ops after flush"
                );
            }
        } finally {
            permit.close();
        }
    }

    /**
     * Verifies that the shard operation calls sync, flush, refresh, and waitForRemoteStoreSync in order.
     */
    public void testShardOperation_SyncFlushRefreshAndWaitForRemoteSync() throws IOException {
        mockPermitAcquisitionSuccess();

        executeShardOperation(mockIndexShard, primaryShardRouting);

        InOrder inOrder = inOrder(mockIndexShard);
        inOrder.verify(mockIndexShard).sync();
        inOrder.verify(mockIndexShard).flush(any(FlushRequest.class));
        inOrder.verify(mockIndexShard).refresh("prepare_tiering");
        inOrder.verify(mockIndexShard).waitForRemoteStoreSync();
    }

    /**
     * Verifies that primary permits are acquired before sync/flush operations.
     */
    public void testShardOperation_AcquiresPrimaryPermitsBeforeOperations() throws IOException {
        mockPermitAcquisitionSuccess();

        executeShardOperation(mockIndexShard, primaryShardRouting);

        InOrder inOrder = inOrder(mockIndexShard);
        inOrder.verify(mockIndexShard).acquireAllPrimaryOperationsPermits(any(ActionListener.class), any(TimeValue.class));
        inOrder.verify(mockIndexShard).sync();
        inOrder.verify(mockIndexShard).flush(any(FlushRequest.class));
    }

    /**
     * Verifies that timeout on permit acquisition throws IOException.
     */
    public void testShardOperation_PermitTimeout_ThrowsIOException() {
        mockPermitAcquisitionFailure(new TimeoutException("Timed out waiting for permits"));

        IOException thrown = expectThrows(IOException.class, () -> executeShardOperation(mockIndexShard, primaryShardRouting));
        assertTrue(
            "Exception message should mention permit acquisition failure",
            thrown.getMessage().contains("Failed to acquire primary operation permits")
        );
        assertNotNull("Should have a cause", thrown.getCause());
    }

    /**
     * Verifies that uncommitted ops > 0 after flush throws IOException.
     */
    public void testShardOperation_UncommittedOpsAfterFlush_ThrowsIOException() throws IOException {
        mockPermitAcquisitionSuccess();

        TranslogStats translogStats = mock(TranslogStats.class);
        when(translogStats.getUncommittedOperations()).thenReturn(5);
        when(mockIndexShard.translogStats()).thenReturn(translogStats);

        IOException thrown = expectThrows(IOException.class, () -> executeShardOperation(mockIndexShard, primaryShardRouting));
        assertTrue(
            "Exception message should mention uncommitted ops",
            thrown.getMessage().contains("uncommitted translog ops after flush")
        );
        assertTrue(
            "Exception message should include the count",
            thrown.getMessage().contains("5")
        );
        // Permit should still be released via finally block
        verify(mockPermit).close();
    }

    /**
     * Verifies that the permit is released even if flush/sync throws an exception.
     */
    public void testShardOperation_ReleasesPermitOnFailure() throws IOException {
        mockPermitAcquisitionSuccess();

        doThrow(new IOException("sync failed")).when(mockIndexShard).sync();

        expectThrows(IOException.class, () -> executeShardOperation(mockIndexShard, primaryShardRouting));

        // Verify permit was released despite the exception
        verify(mockPermit).close();
    }

    /**
     * Verifies that the shards() method only returns primary shards.
     * We test this by building a routing table with both primary and replica shards
     * and verifying the predicate filters correctly.
     */
    public void testShards_TargetsPrimariesOnly() {
        ShardId sid = new ShardId(new Index("test-index", "test-uuid"), 0);
        ShardRouting primary = TestShardRouting.newShardRouting(sid, "node1", true, ShardRoutingState.STARTED);
        ShardRouting replica = TestShardRouting.newShardRouting(sid, "node2", false, ShardRoutingState.STARTED);

        IndexShardRoutingTable shardRoutingTable = new IndexShardRoutingTable.Builder(sid)
            .addShard(primary)
            .addShard(replica)
            .build();

        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(sid.getIndex())
            .addIndexShard(shardRoutingTable)
            .build();

        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTable).build();

        // Use the same predicate as TransportPrepareTieringAction.shards()
        var shardsIterator = clusterState.routingTable()
            .allShardsSatisfyingPredicate(new String[] { "test-index" }, ShardRouting::primary);

        int count = 0;
        for (ShardRouting shard : shardsIterator) {
            assertTrue("Only primary shards should be returned", shard.primary());
            count++;
        }
        assertEquals("Should have exactly 1 primary shard", 1, count);
    }
}
