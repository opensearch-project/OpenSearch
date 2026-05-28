/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streaming coordinator-side reduce sink: opens one native partition stream per child input,
 * pushes each fed batch through a tokio mpsc-backed sender, and drains the native output
 * stream into {@link ExchangeSinkContext#downstream()} inline on the {@link #reduce} caller
 * (the reduce stage's task, on a virtual thread).
 *
 * <p>Multi-input shapes route via per-child wrappers from {@link #sinkForChild(int)}; the
 * bare {@link #feed(VectorSchemaRoot)} is reserved for single-input. Feeds are concurrent
 * with the drain — backpressure is the bounded native input mpsc.
 *
 * <p>Cleanup ownership lives in {@link #reduce}'s {@code finally} (via {@link SinkState}),
 * not {@link #close}, so a close call from another thread never races a parked drain.
 *
 * <p>TODO abstraction leak: this class implements {@link MultiInputExchangeSink} unconditionally
 * even when only one child stage feeds it. The marker is meant for genuine multi-input shapes
 * (Union/Join), and callers like {@code ReduceStageExecution.inputSink} have to dispatch on
 * the logical child-stage count instead of the marker. Either split into a single-input
 * subclass and a multi-input subclass, or drop the marker and let the caller always go through
 * {@code feed()} when there's one child. Current behaviour is correct but the typing lies.
 */
public class DatafusionReduceSink extends AbstractDatafusionReduceSink implements MultiInputExchangeSink {

    private static final Logger logger = LogManager.getLogger(DatafusionReduceSink.class);

    /**
     * Per-child senders keyed by childStageId, populated in declaration order so the
     * single-input case can pick the sole entry without an explicit lookup.
     */
    private final Map<Integer, DatafusionPartitionSender> sendersByChildStageId;
    private final StreamHandle outStream;
    /** Cumulative batches fed into any native sender. */
    private final AtomicLong feedCount = new AtomicLong();

    /**
     * Routes cleanup to the {@link #reduce} caller when a drain is in flight — never to a
     * concurrent {@link #close()}, which would race {@code drop_in_place} on the senders
     * and abort the JVM. Transitions: READY → REDUCING (reduce entered) → DONE (drain
     * returned, cleanup ran). Close-before-reduce: READY → DONE inline.
     */
    enum SinkState {
        READY,
        REDUCING,
        DONE
    }

    final AtomicReference<SinkState> state = new AtomicReference<>(SinkState.READY);

    /** Guards the teardown body so concurrent + sequential close paths don't run it twice. */
    final java.util.concurrent.atomic.AtomicBoolean torndown = new java.util.concurrent.atomic.AtomicBoolean();

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        this(ctx, runtimeHandle, null);
    }

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle, DataFusionReduceState preparedState) {
        super(ctx, runtimeHandle, preparedState);
        Map<Integer, DatafusionPartitionSender> senders = new LinkedHashMap<>(childInputs.size());
        long streamPtr = 0;
        StreamHandle outStreamLocal = null;
        boolean success = false;
        try {
            if (preparedState != null) {
                // Plan was already prepared by FinalAggregateInstructionHandler. The handler
                // registered senders + captured per-input schemas in ctx.childInputs()
                // iteration order; re-index them by childStageId here for lookup during feed().
                int i = 0;
                for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                    senders.put(child.getKey(), preparedState.senders().get(i));
                    childSchemas.put(child.getKey(), preparedState.inputSchemas().get(i));
                    i++;
                }
                streamPtr = NativeBridge.executeLocalPreparedPlan(session.getPointer(), ctx.taskId());
            } else {
                // Legacy path (non-aggregate reduce): register partitions and execute the
                // fragment bytes directly. Used when no prior instruction prepared a plan.
                //
                // ctx.fragmentBytes() references each partition by its "input-<stageId>" name
                // (DataFusionFragmentConvertor names them this way during plan conversion).
                for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                    int childStageId = child.getKey();
                    byte[] producerPlanBytes = child.getValue();
                    NativeBridge.RegisteredInput registered = NativeBridge.registerPartitionStream(
                        session.getPointer(),
                        inputIdFor(childStageId),
                        producerPlanBytes
                    );
                    senders.put(childStageId, new DatafusionPartitionSender(registered.pointer()));
                    childSchemas.put(childStageId, ArrowSchemaIpc.fromBytes(registered.schemaIpc()));
                }
                streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes(), ctx.taskId());
            }
            outStreamLocal = new StreamHandle(streamPtr, runtimeHandle);
            success = true;
        } finally {
            if (!success) {
                if (streamPtr != 0) {
                    NativeBridge.streamClose(streamPtr);
                }
                // Only close senders we allocated locally (legacy path). When preparedState
                // owns them, the state's close() will.
                if (preparedState == null) {
                    for (DatafusionPartitionSender sender : senders.values()) {
                        sender.close();
                    }
                    session.close();
                }
            }
        }
        this.outStream = outStreamLocal;
        this.sendersByChildStageId = senders;
        // Drain is not started here — it runs inline on the owning reduce stage's
        // reduce() caller thread. No separate drain executor.
    }

    /**
     * Lock-free feed for the single-input case: writes to the sole registered sender.
     * Multi-input callers must use {@link #sinkForChild(int)} instead — calling this
     * method when more than one partition is registered is a programming error because
     * the routing target is ambiguous.
     */
    @Override
    public void feed(VectorSchemaRoot batch) {
        if (sendersByChildStageId.size() != 1) {
            batch.close();
            throw new IllegalStateException(
                "DatafusionReduceSink has " + sendersByChildStageId.size() + " input partitions; use sinkForChild(int) instead of feed()"
            );
        }
        feedToSender(sendersByChildStageId.values().iterator().next(), batch, childSchemas.values().iterator().next());
    }

    @Override
    public ExchangeSink sinkForChild(int childStageId) {
        DatafusionPartitionSender sender = sendersByChildStageId.get(childStageId);
        if (sender == null) {
            throw new IllegalArgumentException(
                "No registered partition for childStageId=" + childStageId + "; known ids=" + sendersByChildStageId.keySet()
            );
        }
        return new ChildSink(sender, childSchemas.get(childStageId));
    }

    /**
     * Lock-free per-sender feed. Exports the batch via Arrow C Data outside any lock
     * (the allocator is thread-safe; multiple shard handlers can export concurrently),
     * then sends it through the supplied sender. The Rust mpsc::Sender is thread-safe,
     * so multiple producers feeding the same sender is safe. If close() raced and
     * already ran senderClose, the native side returns an error ("receiver dropped")
     * which we catch and discard.
     */
    private void feedToSender(DatafusionPartitionSender sender, VectorSchemaRoot batch, Schema declaredSchema) {
        // Best-effort fast path — skip export work if already closed.
        if (closed) {
            batch.close();
            return;
        }
        BufferAllocator alloc = ctx.allocator();
        // Type-only equality check; nullability and Timestamp precision are advisory.
        if (!typesMatch(batch.getSchema(), declaredSchema)) {
            batch.close();
            throw new IllegalStateException(
                "DatafusionReduceSink: batch schema types do not match declared schema. "
                    + "declared="
                    + declaredSchema
                    + " batch="
                    + batch.getSchema()
            );
        }
        ArrowArray array = ArrowArray.allocateNew(alloc);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
        try {
            try {
                Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
            } finally {
                batch.close();
            }
            // sender.send acquires its read lock so the native borrow outlives concurrent
            // close — see DatafusionPartitionSender. Throws IllegalStateException via
            // NativeHandle.getPointer() if the sender was closed (the close-race path).
            try {
                sender.send(array.memoryAddress(), arrowSchema.memoryAddress());
                feedCount.incrementAndGet();
            } catch (IllegalStateException e) {
                // Sender close raced our send — Rust didn't take ownership, so the FFI
                // structs' release callbacks are still set. Invoke them explicitly to free
                // the exported buffers back to the Java allocator. (ArrowArray.close /
                // ArrowSchema.close in the finally below frees the wrapper but does NOT
                // invoke the C release callback.)
                array.release();
                arrowSchema.release();
                if (closed) {
                    logger.debug("[ReduceSink] send-after-close race caught, discarding batch");
                    return;
                }
                throw e;
            }
        } finally {
            // Free the wrappers. On the success path Rust nulled the release callback,
            // so close is a no-op for the data. On the failure path we already invoked
            // release explicitly above.
            array.close();
            arrowSchema.close();
        }
    }

    /**
     * Field-by-field type equality. Ignores nullability; Timestamp precision/timezone
     * parameters are tolerated because the data-node parquet reader and physical
     * planner pick a precision the Java-side declaration does not predict, and the
     * chosen precision round-trips through Arrow C Data — divergence is harmless.
     */
    private static boolean typesMatch(Schema actual, Schema declared) {
        List<Field> a = actual.getFields();
        List<Field> d = declared.getFields();
        if (a.size() != d.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            ArrowType at = a.get(i).getType();
            ArrowType dt = d.get(i).getType();
            if (at.getTypeID() == ArrowType.ArrowTypeID.Timestamp && dt.getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                continue;
            }
            if (!at.equals(dt)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Per-child wrapper returned from {@link #sinkForChild(int)}. The orchestrator
     * routes one of these per child stage, and the wrapper's close() signals EOF for
     * its specific input partition. Idempotent — duplicate close() calls are no-ops.
     */
    private final class ChildSink implements ExchangeSink {
        private final DatafusionPartitionSender sender;
        private final Schema declaredSchema;
        private volatile boolean childClosed;

        ChildSink(DatafusionPartitionSender sender, Schema declaredSchema) {
            this.sender = sender;
            this.declaredSchema = declaredSchema;
        }

        @Override
        public void feed(VectorSchemaRoot batch) {
            feedToSender(sender, batch, declaredSchema);
        }

        @Override
        public void close() {
            if (childClosed) {
                return;
            }
            childClosed = true;
            try {
                sender.close();
            } catch (Throwable t) {
                logger.warn("[ReduceSink] error closing child sender", t);
            }
        }
    }

    /**
     * Not used — feed() is overridden directly for the single-input path and
     * {@link ChildSink#feed} for the multi-input path. Required by the abstract
     * class contract.
     */
    @Override
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("DatafusionReduceSink overrides feed() directly");
    }

    /**
     * Atomic via a single {@code compareAndExchange}: prior state tells us which branch to take.
     * <ul>
     *   <li>READY: external close (no drain in flight). Tear down inline.</li>
     *   <li>REDUCING: drain is parked. Fire {@code cancel_query} so it unwinds — the
     *       in-flight {@link #reduce}'s {@code finally} calls {@code closeImpl} directly
     *       (NOT {@code super.close()}, because the base's {@code closed} flag was set by
     *       this very call and would short-circuit re-entry) so teardown runs then.</li>
     *   <li>DONE: this IS the {@code reduce()} finally call (or an idempotent second close).
     *       Do the teardown; {@link #torndown} gates against double-running it.</li>
     * </ul>
     */
    @Override
    protected Exception closeImpl() {
        SinkState before = state.compareAndExchange(SinkState.READY, SinkState.DONE);
        if (before == SinkState.REDUCING) {
            // Drain parked — dropping senders/outStream now would panic in drop_in_place.
            fireCancelQuery();
            return null;  // reduce()'s finally calls closeImpl directly to tear down.
        }
        // before == READY (we just won) or DONE (reduce's finally calling us, or duplicate close).
        if (torndown.compareAndSet(false, true) == false) {
            return null;
        }
        Exception failure = null;
        // 1. Signal EOF on every sender (ChildSink may have closed some already; idempotent).
        for (DatafusionPartitionSender sender : sendersByChildStageId.values()) {
            try {
                sender.close();
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        try {
            outStream.close();
        } catch (Exception t) {
            failure = accumulate(failure, t);
        }
        if (preparedState == null) {
            try {
                session.close();
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        return failure;
    }

    /** Test seam: overridden to count invocations without static mocking. */
    void fireCancelQuery() {
        NativeBridge.cancelQuery(ctx.taskId());
    }

    /**
     * Drains inline on the caller (the reduce stage's task, on a virtual thread).
     * Drain terminates on input EOF (all per-child wrappers closed via
     * {@link #sinkForChild}) or on cancel-Err (an external {@link #close()} fired
     * {@code cancel_query}). The {@code finally} runs {@code super.close()} so cleanup
     * happens on the same thread as the drain — no race with concurrent close.
     */
    @Override
    public void reduce(ActionListener<Void> listener) {
        SinkState before = state.compareAndExchange(SinkState.READY, SinkState.REDUCING);
        if (before == SinkState.DONE) {
            listener.onFailure(new IllegalStateException("sink closed before reduce"));
            return;
        }
        assert before == SinkState.READY : "reduce called more than once (state=" + before + ")";
        Exception failure = null;
        try {
            drainOutputIntoDownstream(outStream);
        } catch (Exception e) {
            failure = e;
        } finally {
            state.set(SinkState.DONE);
            try {
                Exception closeFailure = closeImpl();
                if (closeFailure != null) {
                    failure = accumulate(failure, closeFailure);
                }
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        if (failure == null) {
            listener.onResponse(null);
        } else {
            listener.onFailure(failure);
        }
    }

    /** Returns the cumulative number of batches fed into any native sender. For Tests */
    long feedCount() {
        return feedCount.get();
    }
}
