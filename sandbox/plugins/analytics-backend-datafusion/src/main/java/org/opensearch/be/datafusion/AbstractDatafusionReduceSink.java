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
import org.opensearch.analytics.spi.CancellableExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ReducingExchangeSink;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared lifecycle skeleton for coordinator-side {@link ReducingExchangeSink}s backed by a
 * native DataFusion local session. Subclasses customise per-batch handling, the reduce verb,
 * and native cleanup via {@link #feedBatchUnderLock}, {@link #reduce}, {@link #closeImpl}.
 *
 * <p>Invariants: {@link #feed} runs under {@link #feedLock} and always closes the batch in
 * {@code finally}; {@link #close} flips {@link #closed} under the lock then delegates to
 * {@link #closeImpl}. Subclasses own the full teardown sequence in {@link #closeImpl} —
 * including {@link #session} and any sender/stream handles. The base used to auto-close
 * {@code session} after {@code closeImpl} returned, but that double-closed (every concrete
 * subclass already closed it) and tangled session ownership with the close-state machine
 * the streaming subclass needs to manage.
 * The {@link ExchangeSinkContext#downstream()} sink is NOT closed here — its lifecycle is the
 * walker's.
 *
 * <p>Multi-input is exposed via {@link #childInputs} (childStageId → producer plan bytes) and
 * {@link #childSchemas} (lazily populated by subclasses from native registration replies).
 * Use {@link #inputIdFor(int)} for the per-child table id; {@link #INPUT_ID} is the
 * single-input shortcut.
 *
 * @opensearch.internal
 */
abstract class AbstractDatafusionReduceSink implements ReducingExchangeSink, CancellableExchangeSink {

    /** Single-input shortcut for the per-child table id; multi-input uses {@link #inputIdFor(int)}. */
    static final String INPUT_ID = "input-0";

    protected final ExchangeSinkContext ctx;
    protected final NativeRuntimeHandle runtimeHandle;
    protected final DatafusionLocalSession session;
    /** Execution metrics + physical plan JSON, populated after reduce drains. */
    protected volatile byte[] executionMetrics;

    /**
     * Non-null when constructed from a pre-prepared plan (FinalAggregateInstructionHandler):
     * session + senders are owned by the state and {@link #close} skips re-closing them.
     */
    protected final DataFusionReduceState preparedState;

    /** Per-child producer plan bytes (childStageId → bytes), iteration order = ctx.childInputs(). */
    protected final Map<Integer, byte[]> childInputs;

    /** Per-child declared schema, populated lazily from native registration replies. */
    protected final Map<Integer, Schema> childSchemas;

    /** Serialises {@link #feed}/{@link #close} against producers. */
    protected final Object feedLock = new Object();

    /** Set once under {@link #feedLock} in {@link #close}. */
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
        this.session = preparedState != null ? preparedState.session() : new DatafusionLocalSession(runtimeHandle.get(), ctx.taskId());
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

    /**
     * Fires the Rust CancellationToken for this query so that {@code stream_next}
     * returns the sentinel {@code 0} immediately and the drain unblocks without
     * waiting for DataFusion to finish naturally. No-op when {@code taskId} is 0
     * (no context registered) or when already closed.
     */
    @Override
    public final void cancel() {
        long taskId = ctx.taskId();
        if (taskId != 0L) {
            NativeBridge.cancelQuery(taskId);
        }
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        synchronized (feedLock) {
            if (closed) {
                batch.close();
                return;
            }
            try (batch) {
                feedBatchUnderLock(batch);
            }
        }
    }

    @Override
    public void close() {
        synchronized (feedLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        Exception failure = null;
        try {
            failure = closeImpl();
        } catch (Exception t) {
            failure = accumulate(failure, t);
        }
        rethrow(failure);
    }

    /**
     * Per-batch hook. Called under {@link #feedLock} after the closed-check. MUST NOT
     * close {@code batch} — the base does it in {@code finally}.
     */
    protected abstract void feedBatchUnderLock(VectorSchemaRoot batch);

    /** Returns execution metrics JSON (including physical_plan) captured after reduce, or null. */
    public byte[] getExecutionMetrics() {
        return executionMetrics;
    }

    /**
     * Subclass shutdown. Despite the historical name, this is NOT called under {@link #feedLock} —
     * the lock is released after {@link #closed} is flipped. Subclasses own the full teardown
     * including {@link #session} (gate on {@link #preparedState} == null when the session is
     * owned externally). Return the first failure, or null.
     */
    protected abstract Exception closeImpl();

    /**
     * Imports each batch from {@code outStream} into a fresh {@link VectorSchemaRoot} and
     * feeds it downstream. Caller retains ownership of {@code outStream}.
     */
    protected final void drainOutputIntoDownstream(StreamHandle outStream) {
        BufferAllocator alloc = ctx.allocator();
        try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
            DatafusionResultStream.BatchIterator it = new DatafusionResultStream.BatchIterator(outStream, alloc, dictProvider);
            while (it.hasNext()) {
                // next() transfers ownership of the imported VSR to us. feed() takes ownership only
                // on success; if it throws (e.g. the downstream sink was torn down on a concurrent
                // cancel), the imported batch would otherwise leak in the per-query allocator —
                // close it ourselves on the failure path.
                VectorSchemaRoot batch = it.next().getArrowRoot();
                boolean fed = false;
                try {
                    ctx.downstream().feed(batch);
                    fed = true;
                } finally {
                    if (!fed) {
                        batch.close();
                    }
                }
            }
        }
    }

    /** Returns {@code t} if {@code acc} is null; otherwise adds {@code t} as a suppressed of {@code acc}. */
    protected static Exception accumulate(Exception acc, Exception t) {
        if (acc == null) {
            return t;
        }
        acc.addSuppressed(t);
        return acc;
    }

    private static void rethrow(Exception failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException re) {
            throw re;
        }
        throw new RuntimeException(failure);
    }
}
