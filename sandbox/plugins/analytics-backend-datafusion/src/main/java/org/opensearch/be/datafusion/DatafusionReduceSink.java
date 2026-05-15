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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streaming coordinator-side reduce sink: opens one native partition stream per child
 * input, pushes each fed batch through a tokio mpsc-backed sender, and drains the
 * native output stream into {@link ExchangeSinkContext#downstream()} via a task
 * submitted to the supplied {@link Executor} at construction.
 *
 * <p>Single-input shapes register one partition under {@link AbstractDatafusionReduceSink#INPUT_ID} and accept
 * batches via the inherited {@link #feed(VectorSchemaRoot)} method. Multi-input shapes
 * (Union, Join) register one partition per child stage and require callers to obtain
 * a per-child wrapper via {@link #sinkForChild(int)} — feeds via the bare
 * {@link #feed(VectorSchemaRoot)} method are rejected since the routing target is
 * ambiguous.
 *
 * <p>Multiple shard response handlers call {@link #feed} concurrently; backpressure
 * comes from the bounded native input mpsc. The drain task runs concurrently with
 * feeds: DataFusion's plan only makes progress when its output is being polled, so
 * without a concurrent consumer feeds wedge once the input mpsc fills.
 *
 * <p><b>Lifecycle.</b> {@link #closeUnderLock} closes senders (signal input EOF),
 * joins the drain task (waits for the streamNext loop to drain remaining output and
 * exit cleanly), then closes {@link #outStream} and {@link #session}.
 */
public final class DatafusionReduceSink extends AbstractDatafusionReduceSink implements MultiInputExchangeSink {

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
     * Future for the drain task, submitted at construction. Joined in
     * {@link #closeUnderLock} so the streamNext loop has finished and all output
     * batches have been fed downstream before native handles are torn down.
     */
    private final FutureTask<Void> drainTask;
    /** Captures any throwable from the drain task for surfacing during close(). */
    private final AtomicReference<Throwable> drainFailure = new AtomicReference<>();

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle, Executor drainExecutor) {
        this(ctx, runtimeHandle, drainExecutor, null);
    }

    public DatafusionReduceSink(
        ExchangeSinkContext ctx,
        NativeRuntimeHandle runtimeHandle,
        Executor drainExecutor,
        DataFusionReduceState preparedState
    ) {
        super(ctx, runtimeHandle, preparedState);
        Map<Integer, DatafusionPartitionSender> senders = new LinkedHashMap<>(childInputs.size());
        long streamPtr = 0;
        StreamHandle outStreamLocal = null;
        boolean success = false;
        try {
            if (preparedState != null) {
                // Plan was already prepared by FinalAggregateInstructionHandler. The handler
                // registered senders in ctx.childInputs() iteration order; we re-index them
                // here by childStageId for lookup during feed().
                int i = 0;
                for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                    senders.put(child.getKey(), preparedState.senders().get(i++));
                }
                streamPtr = NativeBridge.executeLocalPreparedPlan(session.getPointer());
            } else {
                // Legacy path (non-aggregate reduce): register partitions and execute the
                // fragment bytes directly. Used when no prior instruction prepared a plan.
                //
                // ctx.fragmentBytes() references each partition by its "input-<stageId>" name
                // (DataFusionFragmentConvertor names them this way during plan conversion).
                for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                    int childStageId = child.getKey();
                    byte[] schemaIpc = child.getValue();
                    long senderPtr = NativeBridge.registerPartitionStream(session.getPointer(), inputIdFor(childStageId), schemaIpc);
                    senders.put(childStageId, new DatafusionPartitionSender(senderPtr));
                }
                streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
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

        // Submit drain AFTER native handles are constructed so the catch-block above
        // doesn't have to handle task teardown on construction failure.
        this.drainTask = new FutureTask<>(this::drainLoop, null);
        drainExecutor.execute(drainTask);
    }

    private void drainLoop() {
        // The drain parks on streamNext().join() — must run on a virtual thread so
        // the park unmounts from its carrier. On a platform thread this would hold
        // a platform-pool slot for the whole query's lifetime, capping concurrent
        // analytics queries at pool size.
        assert Thread.currentThread().isVirtual() : "drain must run on a virtual thread, got " + Thread.currentThread();
        try {
            drainOutputIntoDownstream(outStream);
        } catch (Throwable t) {
            drainFailure.set(t);
            logger.warn("[ReduceSink] drain task terminated with error", t);
        }
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
        // Type check only; nullability is advisory and ignored.
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

    /** Field-by-field type equality, ignoring nullability. */
    private static boolean typesMatch(Schema actual, Schema declared) {
        List<Field> a = actual.getFields();
        List<Field> d = declared.getFields();
        if (a.size() != d.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).getType().equals(d.get(i).getType())) {
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

    @Override
    protected Throwable closeUnderLock() {
        Throwable failure = null;
        // 1. Signal EOF on every sender (ChildSink may have closed some already; idempotent).
        for (DatafusionPartitionSender sender : sendersByChildStageId.values()) {
            try {
                sender.close();
            } catch (Throwable t) {
                failure = accumulate(failure, t);
            }
        }
        // 2. Wait for drain to drain remaining output and exit on EOF. drainLoop catches
        // every Throwable internally, so get() never surfaces an ExecutionException.
        try {
            drainTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = accumulate(failure, e);
        } catch (ExecutionException e) {
            failure = accumulate(failure, e.getCause());
        }
        Throwable drainErr = drainFailure.get();
        if (drainErr != null) {
            failure = accumulate(failure, drainErr);
        }
        // 3. Close native resources (outStream first, then session, per drop ordering).
        try {
            outStream.close();
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        }
        session.close();
        assert drainTask.isDone() : "drainTask must be DONE on closeUnderLock exit (closeUnderLock must await it)";
        return failure;
    }

    /** Returns the cumulative number of batches fed into any native sender. */
    public long feedCount() {
        return feedCount.get();
    }
}
