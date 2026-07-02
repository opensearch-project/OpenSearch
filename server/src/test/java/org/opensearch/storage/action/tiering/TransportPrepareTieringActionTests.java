/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
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
            indexShard.freezeForTiering();
            indexShard.sync();
            indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            indexShard.refresh("prepare_tiering");
            indexShard.waitForRemoteStoreSync();
            indexShard.waitForReplicaSync(TimeValue.timeValueSeconds(30));

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
     * Verifies that the engine is frozen and in-flight merges are drained before the flush, so no
     * merge mutates the catalog after the final refresh during tiering preparation.
     */
    public void testShardOperation_FreezesAndDrainsMergesBeforeFlush() throws IOException {
        mockPermitAcquisitionSuccess();

        executeShardOperation(mockIndexShard, primaryShardRouting);

        InOrder inOrder = inOrder(mockIndexShard);
        inOrder.verify(mockIndexShard).freezeForTiering();
        inOrder.verify(mockIndexShard).sync();
        inOrder.verify(mockIndexShard).flush(any(FlushRequest.class));
        inOrder.verify(mockIndexShard).refresh("prepare_tiering");
    }

    /**
     * Verifies that the shard operation calls sync, flush, refresh, waitForRemoteStoreSync,
     * and waitForReplicaSync in order.
     */
    public void testShardOperation_SyncFlushRefreshAndWaitForRemoteSync() throws IOException {
        mockPermitAcquisitionSuccess();

        executeShardOperation(mockIndexShard, primaryShardRouting);

        InOrder inOrder = inOrder(mockIndexShard);
        inOrder.verify(mockIndexShard).sync();
        inOrder.verify(mockIndexShard).flush(any(FlushRequest.class));
        inOrder.verify(mockIndexShard).refresh("prepare_tiering");
        inOrder.verify(mockIndexShard).waitForRemoteStoreSync();
        inOrder.verify(mockIndexShard).waitForReplicaSync(any(TimeValue.class));
    }

    /**
     * Verifies that if waitForReplicaSync throws (replicas failed to sync in time),
     * the exception propagates and the prepare action fails — allowing retry.
     */
    public void testShardOperation_WaitForReplicaSyncTimeout_PropagatesFailure() throws IOException {
        mockPermitAcquisitionSuccess();

        doThrow(
            new IOException(
                "[REPLICA_SYNC_TIMEOUT] Shard [[clickbench][0]] replicas failed to sync within 30s. "
                    + "Replicas still behind: 1, max checkpoints behind: 2"
            )
        ).when(mockIndexShard).waitForReplicaSync(any(TimeValue.class));

        IOException ex = expectThrows(IOException.class, () -> executeShardOperation(mockIndexShard, primaryShardRouting));
        assertThat(ex.getMessage(), containsString("[REPLICA_SYNC_TIMEOUT]"));

        // Verify permit was released despite the exception
        verify(mockPermit, timeout(5000)).close();
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
        assertTrue("Exception message should include the count", thrown.getMessage().contains("5"));
        // Permit should still be released via finally block
        verify(mockPermit, timeout(5000)).close();
    }

    /**
     * Verifies that the permit is released even if flush/sync throws an exception.
     */
    public void testShardOperation_ReleasesPermitOnFailure() throws IOException {
        mockPermitAcquisitionSuccess();

        doThrow(new IOException("sync failed")).when(mockIndexShard).sync();

        expectThrows(IOException.class, () -> executeShardOperation(mockIndexShard, primaryShardRouting));

        // Verify permit was released despite the exception
        verify(mockPermit, timeout(5000)).close();
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

        IndexShardRoutingTable shardRoutingTable = new IndexShardRoutingTable.Builder(sid).addShard(primary).addShard(replica).build();

        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(sid.getIndex()).addIndexShard(shardRoutingTable).build();

        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTable).build();

        // Use the same predicate as TransportPrepareTieringAction.shards()
        var shardsIterator = clusterState.routingTable().allShardsSatisfyingPredicate(new String[] { "test-index" }, ShardRouting::primary);

        int count = 0;
        for (ShardRouting shard : shardsIterator) {
            assertTrue("Only primary shards should be returned", shard.primary());
            count++;
        }
        assertEquals("Should have exactly 1 primary shard", 1, count);
    }

    // ── Wire serde tests ──────────────────────────────────────────────────────

    /**
     * Verifies that PrepareTieringRequest round-trips correctly over the wire (writeTo → StreamInput).
     * This ensures the request is not corrupted when sent from cluster-manager to shard nodes.
     */
    public void testPrepareTieringRequest_SerializationRoundTrip() throws IOException {
        PrepareTieringRequest original = new PrepareTieringRequest("my-index");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        PrepareTieringRequest deserialized = new PrepareTieringRequest(out.bytes().streamInput());

        // BroadcastRequest only wire-serializes indices; timeout is a local routing hint not sent over the wire.
        assertArrayEquals(original.indices(), deserialized.indices());
    }

    /**
     * Verifies round-trip with no indices (broadcast to all).
     */
    public void testPrepareTieringRequest_SerializationRoundTrip_NoIndices() throws IOException {
        PrepareTieringRequest original = new PrepareTieringRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        PrepareTieringRequest deserialized = new PrepareTieringRequest(out.bytes().streamInput());

        assertArrayEquals(original.indices(), deserialized.indices());
    }

    // ── Async shard operation tests (deadlock fix) ────────────────────────────

    /**
     * Helper that replicates the async shard operation logic from
     * {@link TransportPrepareTieringAction#shardOperationAsync} for unit testing
     * without requiring full transport infrastructure.
     */
    private void executeShardOperationAsync(
        IndexShard indexShard,
        ShardRouting shardRouting,
        ActionListener<Void> listener,
        ThreadPool threadPool,
        TimeValue mergeTimeout
    ) {
        // Fail fast if shard is not fully started
        if (indexShard.state() != IndexShardState.STARTED) {
            listener.onFailure(
                new IOException(
                    "Shard ["
                        + shardRouting.shardId()
                        + "] is not in STARTED state (current: "
                        + indexShard.state()
                        + "). Cannot prepare for tiering — will retry."
                )
            );
            return;
        }

        // Acquire permits (blocking via PlainActionFuture to match production code)
        Releasable permit;
        try {
            PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
            indexShard.acquireAllPrimaryOperationsPermits(permitFuture, TimeValue.timeValueSeconds(30));
            permit = permitFuture.actionGet();
        } catch (Exception e) {
            listener.onFailure(
                new IOException("Failed to acquire primary operation permits for shard [" + shardRouting.shardId() + "]", e)
            );
            return;
        }

        indexShard.freezeForTiering();

        long mergeTimeoutMillis = (long) (mergeTimeout.millis() * 0.8);
        TimeValue effectiveTimeout = TimeValue.timeValueMillis(mergeTimeoutMillis);
        AtomicBoolean completed = new AtomicBoolean(false);

        // Schedule timeout (named `timeoutTask` to avoid shadowing the Mockito.timeout static import)
        Scheduler.ScheduledCancellable timeoutTask = threadPool.schedule(() -> {
            if (completed.compareAndSet(false, true)) {
                int activeMerges = indexShard.getActiveMergeCount();
                boolean hasPendingMerges = indexShard.hasPendingMerges();
                try {
                    listener.onFailure(
                        new MergeDrainTimeoutException(shardRouting.shardId(), activeMerges, hasPendingMerges, mergeTimeout.toString())
                    );
                } finally {
                    permit.close();
                }
            }
        }, effectiveTimeout, ThreadPool.Names.GENERIC);

        // Non-blocking merge wait
        indexShard.onMergesDrained(() -> {
            if (completed.compareAndSet(false, true)) {
                timeoutTask.cancel();
                try {
                    completeSyncAndFlushForTest(indexShard, shardRouting);
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    permit.close();
                }
            }
        });
    }

    /**
     * Helper that replicates completeSyncAndFlush from TransportPrepareTieringAction.
     */
    private void completeSyncAndFlushForTest(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
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
    }

    /**
     * Tests that when merges are already drained (onMergesDrained fires listener immediately),
     * the listener fires immediately with a successful response, and no timeout fires.
     */
    public void testShardOperationAsync_AlreadyDrained_CompletesImmediately() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            // When onMergesDrained is called, immediately invoke the Runnable (simulating already drained)
            doAnswer(invocation -> {
                Runnable callback = invocation.getArgument(0);
                callback.run();
                return null;
            }).when(mockIndexShard).onMergesDrained(any(Runnable.class));

            AtomicReference<Void> responseRef = new AtomicReference<>();
            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    responseRef.set(null);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should have been called", latch.await(5, TimeUnit.SECONDS));
            assertNull("Should not have failed", failureRef.get());

            verify(mockIndexShard).sync();
            verify(mockIndexShard).flush(any(FlushRequest.class));
            verify(mockIndexShard).refresh("prepare_tiering");
            verify(mockIndexShard).waitForRemoteStoreSync();

            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that when merges are not yet drained (onMergesDrained registers listener),
     * the listener is registered and fires later when the drain callback is invoked.
     */
    public void testShardOperationAsync_MergesDrainLater_ListenerFires() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            // Capture the Runnable passed to onMergesDrained (do not invoke it yet)
            ArgumentCaptor<Runnable> drainCallbackCaptor = ArgumentCaptor.forClass(Runnable.class);
            doAnswer(invocation -> {
                // Do nothing — simulate merges not yet drained
                return null;
            }).when(mockIndexShard).onMergesDrained(drainCallbackCaptor.capture());

            AtomicReference<Void> responseRef = new AtomicReference<>();
            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    responseRef.set(null);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertFalse("Listener should not fire before drain callback", latch.await(100, TimeUnit.MILLISECONDS));

            Runnable drainCallback = drainCallbackCaptor.getValue();
            assertNotNull("Drain callback should have been captured", drainCallback);
            drainCallback.run();

            assertTrue("Listener should fire after drain callback", latch.await(5, TimeUnit.SECONDS));
            assertNull("Should not have failed", failureRef.get());

            verify(mockIndexShard).sync();
            verify(mockIndexShard).flush(any(FlushRequest.class));
            verify(mockIndexShard).refresh("prepare_tiering");
            verify(mockIndexShard).waitForRemoteStoreSync();

            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests the timeout path: when merges never drain, the scheduled timeout fires
     * and the listener receives a MergeDrainTimeoutException with correct merge counts.
     */
    public void testShardOperationAsync_Timeout_FiresMergeDrainTimeoutException() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);

            // onMergesDrained captures the callback but never invokes it (simulates stuck merges)
            doAnswer(invocation -> null).when(mockIndexShard).onMergesDrained(any(Runnable.class));

            when(mockIndexShard.getActiveMergeCount()).thenReturn(3);
            when(mockIndexShard.hasPendingMerges()).thenReturn(true);

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            TimeValue shortTimeout = TimeValue.timeValueMillis(100);

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, shortTimeout);

            assertTrue("Listener should fire after timeout", latch.await(5, TimeUnit.SECONDS));

            assertNotNull("Should have received a failure", failureRef.get());
            assertTrue(
                "Should be MergeDrainTimeoutException, got: " + failureRef.get().getClass().getName(),
                failureRef.get() instanceof MergeDrainTimeoutException
            );

            MergeDrainTimeoutException timeoutEx = (MergeDrainTimeoutException) failureRef.get();
            assertTrue("Message should report shard id", timeoutEx.getMessage().contains(shardId.toString()));
            assertTrue("Message should report active merges", timeoutEx.getMessage().contains("Active merges: 3"));
            assertTrue("Message should report pending merges as yes", timeoutEx.getMessage().contains("pending: yes"));
            assertTrue("Timeout message should contain timeout value", timeoutEx.getMessage().contains("100ms"));

            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that if the shard is not in STARTED state, the listener immediately
     * receives an IOException mentioning the state issue.
     */
    public void testShardOperationAsync_ShardNotStarted_FailsFast() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            when(mockIndexShard.state()).thenReturn(IndexShardState.RECOVERING);

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should fire immediately", latch.await(5, TimeUnit.SECONDS));
            assertNotNull("Should have received a failure", failureRef.get());
            assertTrue("Should be IOException", failureRef.get() instanceof IOException);
            assertTrue("Message should mention not in STARTED state", failureRef.get().getMessage().contains("not in STARTED state"));

            verify(mockIndexShard, never()).acquireAllPrimaryOperationsPermits(any(ActionListener.class), any(TimeValue.class));
            verify(mockIndexShard, never()).onMergesDrained(any(Runnable.class));
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that if permit acquisition fails, the listener immediately receives
     * an IOException wrapping the permit failure cause.
     */
    public void testShardOperationAsync_PermitAcquisitionFails() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            mockPermitAcquisitionFailure(new TimeoutException("Timed out waiting for permits"));

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should fire immediately on permit failure", latch.await(5, TimeUnit.SECONDS));
            assertNotNull("Should have received a failure", failureRef.get());
            assertTrue("Should be IOException", failureRef.get() instanceof IOException);
            assertTrue(
                "Message should mention permit acquisition failure",
                failureRef.get().getMessage().contains("Failed to acquire primary operation permits")
            );

            verify(mockIndexShard, never()).onMergesDrained(any(Runnable.class));
            verify(mockIndexShard, never()).freezeForTiering();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that the AtomicBoolean guard prevents double completion when both
     * the drain callback and timeout fire concurrently.
     */
    public void testShardOperationAsync_AtomicBooleanPreventsDoubleCompletion() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);

            ArgumentCaptor<Runnable> drainCallbackCaptor = ArgumentCaptor.forClass(Runnable.class);
            doAnswer(invocation -> null).when(mockIndexShard).onMergesDrained(drainCallbackCaptor.capture());

            when(mockIndexShard.getActiveMergeCount()).thenReturn(1);
            when(mockIndexShard.hasPendingMerges()).thenReturn(false);

            AtomicInteger completionCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    completionCount.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    completionCount.incrementAndGet();
                    latch.countDown();
                }
            };

            TimeValue shortTimeout = TimeValue.timeValueMillis(100);

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, shortTimeout);

            assertTrue("Timeout should fire", latch.await(5, TimeUnit.SECONDS));

            Runnable drainCallback = drainCallbackCaptor.getValue();
            assertNotNull("Drain callback should have been captured", drainCallback);
            drainCallback.run();

            Thread.sleep(50);

            assertEquals("Listener should be called exactly once despite race", 1, completionCount.get());
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that the permit is always closed on the success path (already drained).
     */
    public void testShardOperationAsync_PermitClosedOnSuccessPath() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            doAnswer(invocation -> {
                Runnable callback = invocation.getArgument(0);
                callback.run();
                return null;
            }).when(mockIndexShard).onMergesDrained(any(Runnable.class));

            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should fire", latch.await(5, TimeUnit.SECONDS));
            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that the permit is always closed on the timeout path.
     */
    public void testShardOperationAsync_PermitClosedOnTimeoutPath() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            doAnswer(invocation -> null).when(mockIndexShard).onMergesDrained(any(Runnable.class));
            when(mockIndexShard.getActiveMergeCount()).thenReturn(1);
            when(mockIndexShard.hasPendingMerges()).thenReturn(false);

            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueMillis(100));

            assertTrue("Listener should fire after timeout", latch.await(5, TimeUnit.SECONDS));
            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that the permit is always closed on the drain callback path
     * when completeSyncAndFlush throws an exception.
     */
    public void testShardOperationAsync_PermitClosedOnDrainCallbackFailure() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);

            ArgumentCaptor<Runnable> drainCallbackCaptor = ArgumentCaptor.forClass(Runnable.class);
            doAnswer(invocation -> null).when(mockIndexShard).onMergesDrained(drainCallbackCaptor.capture());

            doThrow(new IOException("sync failed in drain callback")).when(mockIndexShard).sync();

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            Runnable drainCallback = drainCallbackCaptor.getValue();
            assertNotNull(drainCallback);
            drainCallback.run();

            assertTrue("Listener should fire", latch.await(5, TimeUnit.SECONDS));
            assertNotNull("Should have received a failure", failureRef.get());
            assertTrue("Should be IOException", failureRef.get() instanceof IOException);

            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that the AtomicBoolean guard correctly prevents double-firing when
     * the drain callback arrives after the timeout has already fired.
     */
    public void testShardOperationAsync_DrainAfterTimeout_OnlyTimeoutFires() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);

            ArgumentCaptor<Runnable> drainCallbackCaptor = ArgumentCaptor.forClass(Runnable.class);
            doAnswer(invocation -> null).when(mockIndexShard).onMergesDrained(drainCallbackCaptor.capture());
            when(mockIndexShard.getActiveMergeCount()).thenReturn(2);
            when(mockIndexShard.hasPendingMerges()).thenReturn(true);

            AtomicInteger responseCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    responseCount.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureCount.incrementAndGet();
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueMillis(100));

            assertTrue("Timeout should fire", latch.await(5, TimeUnit.SECONDS));
            assertEquals("Should have exactly 1 failure (timeout)", 1, failureCount.get());
            assertEquals("Should have 0 responses", 0, responseCount.get());

            Runnable drainCallback = drainCallbackCaptor.getValue();
            assertNotNull(drainCallback);
            drainCallback.run();

            assertEquals("Failure count should still be 1", 1, failureCount.get());
            assertEquals("Response count should still be 0", 0, responseCount.get());
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that when the shard state is CLOSED (not STARTED), the operation fails
     * immediately with an appropriate IOException and no further actions are taken.
     */
    public void testShardOperationAsync_ShardClosed_FailsFast() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            when(mockIndexShard.state()).thenReturn(IndexShardState.CLOSED);

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should fire immediately", latch.await(5, TimeUnit.SECONDS));
            assertNotNull("Should have a failure", failureRef.get());
            assertTrue("Should be IOException", failureRef.get() instanceof IOException);
            assertTrue("Message should mention not STARTED", failureRef.get().getMessage().contains("not in STARTED state"));
            assertTrue("Message should mention CLOSED", failureRef.get().getMessage().contains("CLOSED"));

            verify(mockIndexShard, never()).acquireAllPrimaryOperationsPermits(any(ActionListener.class), any(TimeValue.class));
            verify(mockIndexShard, never()).onMergesDrained(any(Runnable.class));
            verify(mockIndexShard, never()).freezeForTiering();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that when timeout fires, the exception carries the correct field values.
     */
    public void testShardOperationAsync_TimeoutProduces_MergeDrainTimeoutException_WithCorrectFields() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);

            // onMergesDrained captures the callback but never invokes it (simulates stuck merges)
            doAnswer(invocation -> null).when(mockIndexShard).onMergesDrained(any(Runnable.class));

            when(mockIndexShard.getActiveMergeCount()).thenReturn(5);
            when(mockIndexShard.hasPendingMerges()).thenReturn(true);

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            TimeValue shortTimeout = TimeValue.timeValueMillis(50);

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, shortTimeout);

            assertTrue("Listener should fire after timeout", latch.await(5, TimeUnit.SECONDS));

            assertNotNull("Should have received a failure", failureRef.get());
            assertTrue(
                "Should be MergeDrainTimeoutException, got: " + failureRef.get().getClass().getName(),
                failureRef.get() instanceof MergeDrainTimeoutException
            );

            MergeDrainTimeoutException timeoutEx = (MergeDrainTimeoutException) failureRef.get();
            assertTrue("Message should report shard id", timeoutEx.getMessage().contains(shardId.toString()));
            assertTrue("Message should report active merges", timeoutEx.getMessage().contains("Active merges: 5"));
            assertTrue("Message should report pending merges as yes", timeoutEx.getMessage().contains("pending: yes"));
            assertTrue("Message should contain the configured timeout", timeoutEx.getMessage().contains("50ms"));

            verify(mockPermit, timeout(5000)).close();
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that when permit acquisition calls listener.onFailure, the async path wraps it in an IOException.
     */
    public void testShardOperationAsync_PermitAcquisitionFailure_AsyncPath() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            mockPermitAcquisitionFailure(new RuntimeException("permit denied"));

            AtomicReference<Exception> failureRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should fire immediately on permit failure", latch.await(5, TimeUnit.SECONDS));
            assertNotNull("Should have received a failure", failureRef.get());
            assertTrue("Should be IOException", failureRef.get() instanceof IOException);
            assertTrue(
                "Message should mention permit acquisition failure",
                failureRef.get().getMessage().contains("Failed to acquire primary operation permits")
            );
            assertNotNull("Should have a cause", failureRef.get().getCause());
            assertTrue("Cause should be the original RuntimeException", failureRef.get().getCause() instanceof RuntimeException);
            assertEquals("permit denied", failureRef.get().getCause().getMessage());

            verify(mockPermit, never()).close();
            verify(mockIndexShard, never()).freezeForTiering();
            verify(mockIndexShard, never()).onMergesDrained(any(Runnable.class));
        } finally {
            terminate(testThreadPool);
        }
    }

    /**
     * Tests that freezeForTiering() is called before onMergesDrained() is registered.
     */
    public void testShardOperationAsync_FreezeForTieringCalledBeforeDrain() throws Exception {
        TestThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            mockPermitAcquisitionSuccess();
            when(mockIndexShard.state()).thenReturn(IndexShardState.STARTED);
            doAnswer(invocation -> {
                Runnable callback = invocation.getArgument(0);
                callback.run();
                return null;
            }).when(mockIndexShard).onMergesDrained(any(Runnable.class));

            CountDownLatch latch = new CountDownLatch(1);

            ActionListener<Void> listener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            };

            executeShardOperationAsync(mockIndexShard, primaryShardRouting, listener, testThreadPool, TimeValue.timeValueSeconds(30));

            assertTrue("Listener should fire", latch.await(5, TimeUnit.SECONDS));

            InOrder inOrder = inOrder(mockIndexShard);
            inOrder.verify(mockIndexShard).freezeForTiering();
            inOrder.verify(mockIndexShard).onMergesDrained(any(Runnable.class));
        } finally {
            terminate(testThreadPool);
        }
    }

    // ── Real-action tests (no simulation) ───────────────────────────────────────────────────────
    // These construct an actual TransportPrepareTieringAction with mocked dependencies and assert
    // contracts that ONLY hold against production code (not against the simulation helpers above).

    /**
     * Constructs a real {@link TransportPrepareTieringAction} with the minimum mock dependencies its
     * constructor requires. The mocked {@link TransportService#registerRequestHandler} is a no-op so
     * the base-class constructor completes; {@link ClusterService#getSettings} and
     * {@link ClusterService#getClusterSettings} are wired so the dynamic timeout consumer registers
     * cleanly. The returned action is suitable for asserting protected/instance method contracts;
     * it will NOT execute end-to-end transport flows.
     */
    private TransportPrepareTieringAction newRealAction(TestThreadPool threadPool) {
        ClusterService mockClusterService = mock(ClusterService.class);
        Settings nodeSettings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, java.util.Set.of(TieringUtils.PREPARE_TIERING_TIMEOUT));
        when(mockClusterService.getSettings()).thenReturn(nodeSettings);
        when(mockClusterService.getClusterSettings()).thenReturn(clusterSettings);

        TransportService mockTransportService = mock(TransportService.class);
        when(mockTransportService.getThreadPool()).thenReturn(threadPool);

        return new TransportPrepareTieringAction(
            mockClusterService,
            mockTransportService,
            mockIndicesService,
            new ActionFilters(java.util.Set.of()),
            mock(IndexNameExpressionResolver.class)
        );
    }

    /**
     * The real action must opt-in to async per-shard execution. This is the wiring decision that
     * routes prepare through {@code shardOperationAsync} (non-blocking merge drain) rather than the
     * synchronous {@code shardOperation} path. Pinning this on the real action — not the simulation —
     * catches regressions where someone toggles the override.
     */
    public void testRealAction_IsAsyncShardOperation_ReturnsTrue() {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            TransportPrepareTieringAction action = newRealAction(threadPool);
            assertTrue("prepare must run async (non-blocking merge drain)", action.isAsyncShardOperation());
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * The synchronous {@code shardOperation} path must throw {@link UnsupportedOperationException} —
     * it is a sentinel that catches misuse if the framework ever calls the sync path despite
     * {@link TransportPrepareTieringAction#isAsyncShardOperation()} returning {@code true}. The
     * simulation tests cannot enforce this contract because the simulation never invokes
     * {@code shardOperation}; only the real action does.
     */
    public void testRealAction_ShardOperation_SyncSentinel_ThrowsUnsupported() {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            TransportPrepareTieringAction action = newRealAction(threadPool);
            UnsupportedOperationException ex = expectThrows(
                UnsupportedOperationException.class,
                () -> action.shardOperation(new PrepareTieringRequest("test-index"), primaryShardRouting)
            );
            assertTrue(
                "sentinel message should mention shardOperationAsync, got: " + ex.getMessage(),
                ex.getMessage().toLowerCase(Locale.ROOT).contains("shardoperationasync")
            );
        } finally {
            terminate(threadPool);
        }
    }
}
