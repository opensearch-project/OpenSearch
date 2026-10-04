/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Deterministic unit tests for {@link TranslogBatchScope}, the bounded translog batch shared by the remote-store
 * segment-replication engines. These exercise the scope directly (no engine) so the failure/abort semantics are
 * verified in isolation: a failed append must fail every pending location, fence the engine, advance no checkpoint,
 * and keep rethrowing the same failure; an abort/close must release every pending location without hanging.
 */
public class TranslogBatchScopeTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId("index", "_na_", 0);

    /** A stub operation whose only relevant behaviour here is a fixed size estimate. */
    private static Translog.Operation op(long sizeBytes) {
        final Translog.Operation operation = mock(Translog.Operation.class);
        when(operation.estimateSize()).thenReturn(sizeBytes);
        return operation;
    }

    private static Engine.IndexResult indexResult(long seqNo) {
        return new Engine.IndexResult(1L, 1L, seqNo, true);
    }

    /**
     * When the underlying batched translog append throws, the scope must: complete every pending location
     * exceptionally with the same failure, invoke failEngine exactly once, advance no checkpoint, surface no max
     * location, and keep rethrowing the identical failure on any later flush()/finish().
     */
    public void testAppendFailureFailsEveryPendingAndFencesEngine() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final RuntimeException appendFailure = new RuntimeException("disk full");
        when(translogManager.add(anyList())).thenThrow(appendFailure);

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final AtomicInteger failEngineCalls = new AtomicInteger();
        final AtomicReference<Exception> failEngineCause = new AtomicReference<>();
        final AtomicInteger onFinishedCalls = new AtomicInteger();

        final TranslogBatchScope scope = new TranslogBatchScope(translogManager, tracker, SHARD_ID, (reason, ex) -> {
            failEngineCalls.incrementAndGet();
            failEngineCause.set(ex);
        }, batch -> onFinishedCalls.incrementAndGet());

        final IndexVersionValue.PendingLocation firstPending = new IndexVersionValue.PendingLocation(scope);
        final IndexVersionValue.PendingLocation secondPending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(16), indexResult(0L), firstPending, 0L);
        scope.add(op(16), indexResult(1L), secondPending, 1L);

        // flush() triggers the (failing) append and must rethrow the wrapped failure.
        final EngineException thrown = expectThrows(EngineException.class, scope::flush);
        assertThat(thrown.getCause(), sameInstance(appendFailure));

        // Every pending location was completed exceptionally with that very EngineException, so resolve() rethrows it
        // without hanging (the latch is already counted down).
        final EngineException firstResolved = expectThrows(EngineException.class, firstPending::resolve);
        final EngineException secondResolved = expectThrows(EngineException.class, secondPending::resolve);
        assertThat(firstResolved, sameInstance(thrown));
        assertThat(secondResolved, sameInstance(thrown));

        // The engine was fenced exactly once with the original cause, and the scope reported itself finished once.
        assertThat(failEngineCalls.get(), equalTo(1));
        assertThat(failEngineCause.get(), sameInstance(appendFailure));
        assertThat(onFinishedCalls.get(), equalTo(1));

        // No checkpoint advanced: nothing was durably appended.
        assertThat(tracker.getProcessedCheckpoint(), equalTo(NO_OPS_PERFORMED));

        // Subsequent flush() and finish() keep rethrowing the identical failure and do not re-invoke the translog or
        // re-fence the engine.
        final EngineException onFlushAgain = expectThrows(EngineException.class, scope::flush);
        assertThat(onFlushAgain, sameInstance(thrown));
        final EngineException onFinishAgain = expectThrows(EngineException.class, scope::finish);
        assertThat(onFinishAgain, sameInstance(thrown));
        assertThat(failEngineCalls.get(), equalTo(1));
    }

    /**
     * A non-{@link EngineException} thrown by the translog is wrapped in an {@link EngineException} that carries the
     * shard id and the original cause, and that wrapper is what every pending location observes.
     */
    public void testAppendFailureWrapsNonEngineExceptionWithShardId() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final IllegalStateException raw = new IllegalStateException("boom");
        when(translogManager.add(anyList())).thenThrow(raw);

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final TranslogBatchScope scope = new TranslogBatchScope(translogManager, tracker, SHARD_ID, (reason, ex) -> {}, batch -> {});

        final IndexVersionValue.PendingLocation pending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(8), indexResult(0L), pending, 0L);

        final EngineException thrown = expectThrows(EngineException.class, scope::finish);
        assertThat(thrown.getCause(), sameInstance(raw));
        assertThat(thrown.getShardId(), equalTo(SHARD_ID));

        final EngineException resolved = expectThrows(EngineException.class, pending::resolve);
        assertThat(resolved, sameInstance(thrown));
    }

    /**
     * abort() completes every pending location exceptionally with the supplied close failure, marks the scope
     * finished, and must not touch the translog. A reader resolving a pending location after abort observes the
     * failure immediately without blocking on the latch.
     */
    public void testAbortCompletesPendingWithoutHanging() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final AtomicInteger onFinishedCalls = new AtomicInteger();

        final TranslogBatchScope scope = new TranslogBatchScope(
            translogManager,
            tracker,
            SHARD_ID,
            (reason, ex) -> {},
            batch -> onFinishedCalls.incrementAndGet()
        );

        final IndexVersionValue.PendingLocation firstPending = new IndexVersionValue.PendingLocation(scope);
        final IndexVersionValue.PendingLocation secondPending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(16), indexResult(0L), firstPending, 0L);
        scope.add(op(16), indexResult(1L), secondPending, 1L);

        final EngineException closeFailure = new EngineException(SHARD_ID, "engine closed with pending translog batches");

        // Resolve each pending location from another thread with a bounded wait to prove abort() releases the latch
        // (no hang). The resolver must observe exactly the close failure.
        final CountDownLatch firstResolverDone = new CountDownLatch(1);
        final AtomicReference<Throwable> firstResolverError = new AtomicReference<>();
        final Thread resolver = new Thread(() -> {
            try {
                final EngineException observed = expectThrows(EngineException.class, firstPending::resolve);
                assertThat(observed, sameInstance(closeFailure));
            } catch (Throwable t) {
                firstResolverError.set(t);
            } finally {
                firstResolverDone.countDown();
            }
        }, "pending-resolver");

        scope.abort(closeFailure);
        resolver.start();

        assertTrue("resolver thread must not hang after abort", firstResolverDone.await(10, TimeUnit.SECONDS));
        assertThat(firstResolverError.get(), nullValue());

        // The second pending location, resolved inline, also observes the close failure without blocking.
        final EngineException secondObserved = expectThrows(EngineException.class, secondPending::resolve);
        assertThat(secondObserved, sameInstance(closeFailure));

        // abort() reported the scope finished once and never advanced the checkpoint.
        assertThat(onFinishedCalls.get(), equalTo(1));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(NO_OPS_PERFORMED));

        // The translog was never consulted by an aborted scope.
        verifyNoInteractions(translogManager);

        // A later flush()/finish() on an aborted scope keeps rethrowing the same close failure.
        final EngineException flushAgain = expectThrows(EngineException.class, scope::flush);
        assertThat(flushAgain, sameInstance(closeFailure));
        final EngineException finishAgain = expectThrows(EngineException.class, scope::finish);
        assertThat(finishAgain, sameInstance(closeFailure));
    }

    /**
     * abort() on a scope that already finished successfully is a no-op: it must not overwrite the committed max
     * location with a failure nor re-invoke the onFinished callback.
     */
    public void testAbortAfterSuccessfulFinishIsNoOp() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final Translog.Location location = new Translog.Location(1L, 0L, 16);
        when(translogManager.add(anyList())).thenReturn(new Translog.Location[] { location });

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final AtomicInteger onFinishedCalls = new AtomicInteger();
        final TranslogBatchScope scope = new TranslogBatchScope(
            translogManager,
            tracker,
            SHARD_ID,
            (reason, ex) -> {},
            batch -> onFinishedCalls.incrementAndGet()
        );

        final IndexVersionValue.PendingLocation pending = new IndexVersionValue.PendingLocation(scope);
        final Engine.IndexResult result = indexResult(0L);
        scope.add(op(16), result, pending, 0L);

        final Translog.Location maxLocation = scope.finish();
        assertThat(maxLocation, sameInstance(location));
        assertThat(onFinishedCalls.get(), equalTo(1));
        assertThat(result.getTranslogLocation(), sameInstance(location));
        assertThat(pending.resolve(), sameInstance(location));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(0L));

        // abort() after a successful finish must not change anything.
        scope.abort(new EngineException(SHARD_ID, "late abort"));
        assertThat(onFinishedCalls.get(), equalTo(1));
        assertThat(scope.finish(), sameInstance(location));
        assertThat(pending.resolve(), sameInstance(location));
    }

    /**
     * A successful finish appends through the batch path, assigns every result its location, advances the processed
     * checkpoint for each seqNo, and returns the greatest location. Confirms the happy path the failure tests invert.
     */
    public void testFinishAppendsBatchAndAdvancesCheckpoint() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final Translog.Location first = new Translog.Location(1L, 0L, 16);
        final Translog.Location second = new Translog.Location(1L, 16L, 16);
        when(translogManager.add(anyList())).thenReturn(new Translog.Location[] { first, second });

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final TranslogBatchScope scope = new TranslogBatchScope(translogManager, tracker, SHARD_ID, (reason, ex) -> {}, batch -> {});

        final Engine.IndexResult firstResult = indexResult(0L);
        final Engine.IndexResult secondResult = indexResult(1L);
        final IndexVersionValue.PendingLocation firstPending = new IndexVersionValue.PendingLocation(scope);
        final IndexVersionValue.PendingLocation secondPending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(16), firstResult, firstPending, 0L);
        scope.add(op(16), secondResult, secondPending, 1L);

        final Translog.Location maxLocation = scope.finish();

        assertThat(maxLocation, notNullValue());
        assertThat(maxLocation, sameInstance(second));
        assertThat(firstResult.getTranslogLocation(), sameInstance(first));
        assertThat(secondResult.getTranslogLocation(), sameInstance(second));
        assertThat(firstPending.resolve(), sameInstance(first));
        assertThat(secondPending.resolve(), sameInstance(second));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(1L));
    }

    /**
     * Using the scope after a successful finish is a programming error: it must surface as an
     * {@link IllegalStateException} rather than silently appending.
     */
    public void testAddAfterFinishThrowsIllegalState() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        when(translogManager.add(anyList())).thenReturn(new Translog.Location[] { new Translog.Location(1L, 0L, 16) });

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final TranslogBatchScope scope = new TranslogBatchScope(translogManager, tracker, SHARD_ID, (reason, ex) -> {}, batch -> {});

        final IndexVersionValue.PendingLocation pending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(16), indexResult(0L), pending, 0L);
        scope.finish();

        final Exception e = expectThrows(
            IllegalStateException.class,
            () -> scope.add(op(16), indexResult(1L), new IndexVersionValue.PendingLocation(scope), 1L)
        );
        assertThat(e, instanceOf(IllegalStateException.class));
    }
}
