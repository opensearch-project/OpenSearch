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
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ReducingExchangeSink;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared lifecycle skeleton for coordinator-side {@link ReducingExchangeSink}s backed by a
 * native DataFusion local session. Subclasses customise per-batch handling, the reduce verb,
 * and native cleanup via {@link #feedBatchUnderLock}, {@link #reduce}, {@link #closeUnderLock}.
 *
 * <p>Invariants: {@link #feed} runs under {@link #feedLock} and always closes the batch in
 * {@code finally}; {@link #close} flips {@link #closed} under the lock, runs
 * {@link #closeUnderLock}, then closes {@link #session} if no {@link #preparedState} owns it.
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
abstract class AbstractDatafusionReduceSink implements ReducingExchangeSink {

    /** Single-input shortcut for the per-child table id; multi-input uses {@link #inputIdFor(int)}. */
    static final String INPUT_ID = "input-0";

    protected final ExchangeSinkContext ctx;
    protected final NativeRuntimeHandle runtimeHandle;
    protected final DatafusionLocalSession session;

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
     * Per-batch hook. Called under {@link #feedLock} after the closed-check. MUST NOT
     * close {@code batch} — the base does it in {@code finally}.
     */
    protected abstract void feedBatchUnderLock(VectorSchemaRoot batch);

    /**
     * Subclass shutdown. Runs after {@link #closed} is set; the base closes {@link #session}
     * afterwards (when {@code preparedState == null}). Return the first failure, or null.
     */
    protected abstract Throwable closeUnderLock();

    /**
     * Imports each batch from {@code outStream} into a fresh {@link VectorSchemaRoot} and
     * feeds it downstream. Caller retains ownership of {@code outStream}.
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
