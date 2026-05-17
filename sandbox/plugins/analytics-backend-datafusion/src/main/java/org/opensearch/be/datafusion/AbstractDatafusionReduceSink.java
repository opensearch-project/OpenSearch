/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Shared lifecycle skeleton for coordinator-side {@link ExchangeSink}s backed by a native
 * DataFusion local session. Subclasses customise per-batch handling and the close-time
 * native handoff via {@link #feedBatchUnderLock} and {@link #closeUnderLock}.
 *
 * <p>Lifecycle invariants enforced by this base:
 * <ul>
 *   <li>{@link #feed} synchronises on {@link #feedLock}, short-circuits when {@link #closed},
 *       and always closes the supplied {@link VectorSchemaRoot} in {@code finally} regardless
 *       of whether {@link #feedBatchUnderLock} succeeds.</li>
 *   <li>{@link #close} flips {@link #closed} once under {@link #feedLock}, runs the
 *       subclass-specific {@link #closeUnderLock} hook, and rethrows any accumulated
 *       failure. Subclasses must close {@link #session} themselves inside
 *       {@link #closeUnderLock} (typically last, after any owned native streams).</li>
 *   <li>The downstream from {@link ExchangeSinkContext#downstream()} is intentionally NOT
 *       closed here — it accumulates drained results consumed by the walker after the
 *       sink is done.</li>
 * </ul>
 *
 * <p>Multi-input shapes (Union, future Join) are supported at this base by exposing
 * {@link #childInputs} (childStageId → producer-side plan bytes) for subclasses to register
 * one native partition per child stage. The native call returns the IPC-encoded schema the
 * session settled on after lowering; subclasses populate {@link #childSchemas} from those
 * returns so the {@code typesMatch} tripwire validates batches against the same schema the
 * native session is registered with. The {@link #INPUT_ID} constant remains as the
 * conventional name for the single-input case (childStageId=0); the per-child id is
 * computed via {@link #inputIdFor(int)}.
 *
 * @opensearch.internal
 */
abstract class AbstractDatafusionReduceSink implements ExchangeSink {

    /**
     * Substrait/DataFusion table name used for the single-input case (childStageId=0).
     * For multi-input shapes use {@link #inputIdFor(int)} instead.
     */
    static final String INPUT_ID = "input-0";

    protected final ExchangeSinkContext ctx;
    protected final NativeRuntimeHandle runtimeHandle;
    protected final DatafusionLocalSession session;
    /**
     * Non-null when this sink was constructed with a pre-prepared FINAL-aggregate plan
     * from the FinalAggregateInstructionHandler. When present, the handler already created
     * the session, registered the input partitions, and called {@code prepareFinalPlan} on
     * the Rust side; the sink only needs to drive {@code executeLocalPreparedPlan} and feed
     * batches. When null, the sink falls back to the legacy path (create its own session,
     * register its own partitions, call {@code executeLocalPlan}).
     *
     * <p>Close ownership: when {@code preparedState != null} the state owns session +
     * senders and {@link #close} skips re-closing them (avoids double-close on the native
     * side). When {@code preparedState == null} the base class closes the session itself.
     */
    protected final DataFusionReduceState preparedState;
    /**
     * Per-child producer-side plan bytes, keyed by childStageId. Iteration order matches
     * the order of {@code ctx.childInputs()} so subclasses get deterministic registration.
     * Subclasses pass each entry to the native registration call, which lowers the plan
     * and returns the schema the session settled on.
     */
    protected final Map<Integer, byte[]> childInputs;

    /**
     * Declared Arrow {@link org.apache.arrow.vector.types.pojo.Schema} per childStageId.
     * Populated lazily by subclasses from the IPC bytes the native registration call
     * returns — i.e. the schema the native session itself derived from the producer plan.
     * Used by sinks to validate incoming batches via the {@code typesMatch} tripwire.
     */
    protected final Map<Integer, Schema> childSchemas;

    /** Guards {@link #closed} and serialises {@link #feed}/{@link #close} against producers. */
    protected final Object feedLock = new Object();

    /** Set once in {@link #close} under {@link #feedLock}. Visible to all threads via volatile. */
    protected volatile boolean closed;

    protected AbstractDatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        this(ctx, runtimeHandle, null);
    }

    protected AbstractDatafusionReduceSink(
        ExchangeSinkContext ctx,
        NativeRuntimeHandle runtimeHandle,
        DataFusionReduceState preparedState
    ) {
        this.ctx = ctx;
        this.runtimeHandle = runtimeHandle;
        this.preparedState = preparedState;
        this.session = preparedState != null ? preparedState.session() : new DatafusionLocalSession(runtimeHandle.get());
        Map<Integer, byte[]> inputs = new LinkedHashMap<>(ctx.childInputs().size());
        for (ExchangeSinkContext.ChildInput child : ctx.childInputs()) {
            inputs.put(child.childStageId(), child.producerPlanBytes());
        }
        this.childInputs = inputs;
        this.childSchemas = new LinkedHashMap<>(ctx.childInputs().size());
    }

    /** DataFusion table name for an input partition associated with the given child stage id. */
    protected static String inputIdFor(int childStageId) {
        return "input-" + childStageId;
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        synchronized (feedLock) {
            if (closed) {
                batch.close();
                return;
            }
            try {
                feedBatchUnderLock(batch);
            } finally {
                batch.close();
            }
        }
    }

    @Override
    public final void close() {
        synchronized (feedLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        Throwable failure = null;
        try {
            failure = closeUnderLock();
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        } finally {
            // If a preparedState owns the session/senders, let the state's close handle
            // them (invoked by the orchestrator). Otherwise close the session we created.
            if (preparedState == null) {
                try {
                    session.close();
                } catch (Throwable t) {
                    failure = accumulate(failure, t);
                }
            }
        }
        rethrow(failure);
    }

    /**
     * Per-batch hook. Called inside {@code synchronized(feedLock)} after {@code closed} is
     * verified false. Implementations export and hand off (or buffer) {@code batch} via the
     * native bridge. Implementations MUST NOT close {@code batch} — the base class does that
     * in {@code finally}.
     */
    protected abstract void feedBatchUnderLock(VectorSchemaRoot batch);

    /**
     * Subclass-specific shutdown. Runs after {@link #closed} is set. Implementations must
     * close all owned native resources including {@link #session} — close owned streams
     * before the session.
     *
     * @return the first failure encountered (use {@link #accumulate(Throwable, Throwable)}
     *         when multiple steps may fail), or {@code null} on clean shutdown.
     */
    protected abstract Throwable closeUnderLock();

    /**
     * Drains a native output stream into {@link ExchangeSinkContext#downstream()},
     * importing each native batch into a fresh {@link VectorSchemaRoot}.
     *
     * <p>Uses {@link DatafusionResultStream.BatchIterator} directly (instead of
     * {@link DatafusionResultStream}) so the caller retains ownership of {@code outStream} —
     * the iterator manages schema, dictionary provider, and per-batch allocation, but
     * does not close the underlying stream handle.
     */
    protected final void drainOutputIntoDownstream(StreamHandle outStream) {
        BufferAllocator alloc = ctx.allocator();
        try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
            DatafusionResultStream.BatchIterator it = new DatafusionResultStream.BatchIterator(outStream, alloc, dictProvider);
            while (it.hasNext()) {
                ctx.downstream().feed(it.next().getArrowRoot());
            }
        }
    }

    /**
     * Synchronously awaits the result of an async native call expressed as a
     * {@code Consumer<ActionListener<Long>>}. Restores interrupt state on
     * {@link InterruptedException} and unwraps {@link ExecutionException} to surface the
     * original cause.
     */
    protected static long asyncCall(Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(ActionListener.wrap(future::complete, future::completeExceptionally));
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        }
    }

    /** Returns {@code t} if {@code acc} is null; otherwise adds {@code t} as a suppressed of {@code acc}. */
    protected static Throwable accumulate(Throwable acc, Throwable t) {
        if (acc == null) {
            return t;
        }
        acc.addSuppressed(t);
        return acc;
    }

    private static void rethrow(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException re) {
            throw re;
        }
        if (failure instanceof Error err) {
            throw err;
        }
        throw new RuntimeException(failure);
    }
}
