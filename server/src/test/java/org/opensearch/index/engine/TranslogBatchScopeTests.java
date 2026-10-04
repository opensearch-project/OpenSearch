/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.support.TransportActions;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
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
     * The failure contract is the per-operation one: when the batched translog append throws, the scope completes every
     * pending location exceptionally with the same exception the single-operation path would have thrown, consults
     * maybeFailEngine exactly once with the raw exception (so the engine fails only if that exception is the translog's
     * tragic event), advances no checkpoint, surfaces no max location, and keeps rethrowing the identical failure on any
     * later flush()/finish(). A RuntimeException from the translog (here the TranslogException the translog itself uses
     * for a non-IO write failure) propagates as the very same instance.
     */
    public void testAppendFailureFailsEveryPendingAndConsultsMaybeFailEngineOnce() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final TranslogException appendFailure = new TranslogException(SHARD_ID, "Failed to write batch", new RuntimeException("boom"));
        when(translogManager.add(anyList())).thenThrow(appendFailure);

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final AtomicInteger maybeFailEngineCalls = new AtomicInteger();
        final AtomicReference<Exception> maybeFailEngineCause = new AtomicReference<>();
        final AtomicInteger onFinishedCalls = new AtomicInteger();

        final TranslogBatchScope scope = new TranslogBatchScope(translogManager, tracker, SHARD_ID, (reason, ex) -> {
            maybeFailEngineCalls.incrementAndGet();
            maybeFailEngineCause.set(ex);
        }, batch -> onFinishedCalls.incrementAndGet());

        final IndexVersionValue.PendingLocation firstPending = new IndexVersionValue.PendingLocation(scope);
        final IndexVersionValue.PendingLocation secondPending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(16), indexResult(0L), firstPending, 0L);
        scope.add(op(16), indexResult(1L), secondPending, 1L);

        // flush() triggers the (failing) append and rethrows the translog's own exception, unwrapped.
        final TranslogException thrown = expectThrows(TranslogException.class, scope::flush);
        assertThat(thrown, sameInstance(appendFailure));

        // Every pending location was completed exceptionally with that same instance, so resolve() rethrows it without
        // hanging (the latch is already counted down).
        assertThat(expectThrows(TranslogException.class, firstPending::resolve), sameInstance(appendFailure));
        assertThat(expectThrows(TranslogException.class, secondPending::resolve), sameInstance(appendFailure));

        // The engine was consulted exactly once with the raw exception, and the scope reported itself finished once.
        assertThat(maybeFailEngineCalls.get(), equalTo(1));
        assertThat(maybeFailEngineCause.get(), sameInstance(appendFailure));
        assertThat(onFinishedCalls.get(), equalTo(1));

        // No checkpoint advanced: nothing was durably appended.
        assertThat(tracker.getProcessedCheckpoint(), equalTo(NO_OPS_PERFORMED));

        // Subsequent flush() and finish() keep rethrowing the identical failure and do not re-invoke the translog or
        // consult the engine again.
        assertThat(expectThrows(TranslogException.class, scope::flush), sameInstance(appendFailure));
        assertThat(expectThrows(TranslogException.class, scope::finish), sameInstance(appendFailure));
        assertThat(maybeFailEngineCalls.get(), equalTo(1));
    }

    /**
     * The one case that needs a carrier: a checked {@link IOException} from the translog cannot cross the unchecked
     * batch interface, so it is wrapped in the translog's own {@link TranslogException} carrying the shard id, exactly
     * as {@code Translog#add} wraps its non-IO failures. The engine still sees the raw IOException, which is what it
     * compares against the translog's tragic exception.
     */
    public void testCheckedAppendFailureIsCarriedByTranslogException() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final IOException raw = new IOException("disk full");
        when(translogManager.add(anyList())).thenThrow(raw);

        final AtomicReference<Exception> maybeFailEngineCause = new AtomicReference<>();
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final TranslogBatchScope scope = new TranslogBatchScope(
            translogManager,
            tracker,
            SHARD_ID,
            (reason, ex) -> maybeFailEngineCause.set(ex),
            batch -> {}
        );

        final IndexVersionValue.PendingLocation pending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(8), indexResult(0L), pending, 0L);

        final TranslogException thrown = expectThrows(TranslogException.class, scope::finish);
        assertThat(thrown.getCause(), sameInstance(raw));
        assertThat(thrown.getShardId(), equalTo(SHARD_ID));
        assertThat(expectThrows(TranslogException.class, pending::resolve), sameInstance(thrown));
        assertThat(maybeFailEngineCause.get(), sameInstance(raw));
    }

    /**
     * An {@link AlreadyClosedException} from the translog (closed by a tragic event such as a fenced remote upload, or
     * by an engine close) must surface as itself, not wrapped: {@code TransportActions#isShardNotAvailableException}
     * recognises the bare exception and the coordinating node then retries the bulk on the re-promoted primary, exactly
     * as it does when a per-operation {@code Translog#add} throws it. Every pending location observes the same instance
     * and the engine callback receives it so maybeFailEngine can fail the engine on the underlying tragic event.
     */
    public void testAlreadyClosedAppendFailureIsNotWrapped() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final AlreadyClosedException closed = new AlreadyClosedException("translog is already closed", new IOException("fenced"));
        when(translogManager.add(anyList())).thenThrow(closed);

        final AtomicReference<Exception> maybeFailEngineCause = new AtomicReference<>();
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final TranslogBatchScope scope = new TranslogBatchScope(
            translogManager,
            tracker,
            SHARD_ID,
            (reason, ex) -> maybeFailEngineCause.set(ex),
            batch -> {}
        );

        final IndexVersionValue.PendingLocation pending = new IndexVersionValue.PendingLocation(scope);
        scope.add(op(8), indexResult(0L), pending, 0L);

        final AlreadyClosedException thrown = expectThrows(AlreadyClosedException.class, scope::finish);
        assertThat(thrown, sameInstance(closed));
        assertTrue(TransportActions.isShardNotAvailableException(thrown));

        assertThat(expectThrows(AlreadyClosedException.class, pending::resolve), sameInstance(closed));
        assertThat(maybeFailEngineCause.get(), sameInstance(closed));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(NO_OPS_PERFORMED));

        // The scope stays failed with that same exception.
        assertThat(expectThrows(AlreadyClosedException.class, scope::finish), sameInstance(closed));
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

    /**
     * The chunk caps are per-scope values supplied by the engine from index settings, not the compiled-in defaults.
     * With an operation cap of 3 the fourth add must already have forced an append of the first three, and with a
     * byte cap smaller than two operations every add after the first closes the previous chunk.
     */
    public void testConfiguredCapsCloseChunks() throws Exception {
        final TranslogManager translogManager = mock(TranslogManager.class);
        final AtomicInteger appends = new AtomicInteger();
        final AtomicInteger appendedOps = new AtomicInteger();
        when(translogManager.add(anyList())).thenAnswer(invocation -> {
            final List<?> ops = invocation.getArgument(0);
            appends.incrementAndGet();
            final Translog.Location[] locations = new Translog.Location[ops.size()];
            for (int i = 0; i < ops.size(); i++) {
                locations[i] = new Translog.Location(1L, appendedOps.getAndIncrement() * 16L, 16);
            }
            return locations;
        });

        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        final TranslogBatchScope byCount = new TranslogBatchScope(
            translogManager,
            tracker,
            SHARD_ID,
            (reason, ex) -> {},
            batch -> {},
            3,
            Long.MAX_VALUE
        );
        for (long seqNo = 0; seqNo < 3; seqNo++) {
            byCount.add(op(16), indexResult(seqNo), new IndexVersionValue.PendingLocation(byCount), seqNo);
        }
        // Reaching the cap appends eagerly: three ops in, one append out, well below the 1,000 default.
        assertThat(appends.get(), equalTo(1));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(2L));
        byCount.add(op(16), indexResult(3L), new IndexVersionValue.PendingLocation(byCount), 3L);
        assertThat(appends.get(), equalTo(1));
        byCount.finish();
        assertThat(appends.get(), equalTo(2));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(3L));

        appends.set(0);
        final TranslogBatchScope byBytes = new TranslogBatchScope(
            translogManager,
            tracker,
            SHARD_ID,
            (reason, ex) -> {},
            batch -> {},
            Integer.MAX_VALUE,
            24
        );
        byBytes.add(op(16), indexResult(4L), new IndexVersionValue.PendingLocation(byBytes), 4L);
        assertThat(appends.get(), equalTo(0));
        // 16 + 16 > 24: the second add closes the first chunk before joining a new one.
        byBytes.add(op(16), indexResult(5L), new IndexVersionValue.PendingLocation(byBytes), 5L);
        assertThat(appends.get(), equalTo(1));
        byBytes.finish();
        assertThat(appends.get(), equalTo(2));
        assertThat(tracker.getProcessedCheckpoint(), equalTo(5L));
    }
}
