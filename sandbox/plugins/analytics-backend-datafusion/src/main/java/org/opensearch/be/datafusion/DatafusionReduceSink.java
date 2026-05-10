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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streaming coordinator-side reduce sink: opens one native partition stream per child
 * input, pushes each fed batch through a tokio mpsc-backed sender, and on close drains
 * the native output stream into {@link ExchangeSinkContext#downstream()}.
 *
 * <p>Single-input shapes register one partition under {@link AbstractDatafusionReduceSink#INPUT_ID} and accept
 * batches via the inherited {@link #feed(VectorSchemaRoot)} method. Multi-input shapes
 * (Union) register one partition per child stage and require callers to obtain a
 * per-child wrapper via {@link #sinkForChild(int)} — feeds via the bare
 * {@link #feed(VectorSchemaRoot)} method are rejected since the routing target is
 * ambiguous.
 *
 * <p>Overrides the base class's {@code synchronized(feedLock)} with a lock-free
 * implementation for the per-sender feed path. Multiple shard response handlers call
 * {@link #feed} concurrently; backpressure comes from the native Rust mpsc channel
 * (bounded, capacity 4). The send-after-close race is handled by catching the native
 * error when the receiver has been dropped.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Constructor registers all input partition streams and kicks off native execution.</li>
 *   <li>{@link #feed} (or {@link ChildSink#feed} via {@link #sinkForChild}) exports each
 *       batch via Arrow C Data and sends it lock-free to the appropriate sender.</li>
 *   <li>{@link #close} signals EOF on every still-open sender, drains output, and releases
 *       native resources.</li>
 * </ol>
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
     * Background thread that drains {@link #outStream} into the downstream sink as soon
     * as the FINAL plan emits batches — running concurrently with feeds.
     *
     * <p>Without this thread, the FINAL plan's downstream side is not polled until
     * {@code close()} runs {@link #drainOutputIntoDownstream}. That polling chain is
     * what causes DataFusion's input operators to pull from our partition stream's
     * receiver. Without a concurrent puller, producers wedge past the input mpsc
     * capacity (verified empirically with target_partitions=1; without RepartitionExec
     * or this drain thread, the 2nd send_blocking parks indefinitely).
     *
     * <p>The thread starts polling immediately at construction. It exits naturally
     * when the FINAL plan reaches EOF (after every {@link #sendersByChildStageId} entry
     * has been closed and DataFusion completes the last aggregation).
     */
    private final Thread drainThread;
    /** Captures any throwable from the drain thread for surfacing during close(). */
    private final AtomicReference<Throwable> drainFailure = new AtomicReference<>();

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        this(ctx, runtimeHandle, null);
    }

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle, DataFusionReduceState preparedState) {
        super(ctx, runtimeHandle, preparedState);
        Map<Integer, DatafusionPartitionSender> senders = new LinkedHashMap<>(childInputs.size());
        long streamPtr = 0;
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
            this.outStream = new StreamHandle(streamPtr, runtimeHandle);
        } catch (RuntimeException e) {
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
            // Only close senders we allocated locally (legacy path). When preparedState
            // owns them, the state's close() will.
            if (preparedState == null) {
                for (DatafusionPartitionSender sender : senders.values()) {
                    try {
                        sender.close();
                    } catch (Throwable ignore) {}
                }
                session.close();
            }
            throw e;
        }
        this.sendersByChildStageId = senders;
        // Spawn the drain thread AFTER the native handles are constructed so the catch-block
        // doesn't have to deal with thread teardown on construction failure.
        this.drainThread = new Thread(this::drainLoop, "df-reduce-drain-q" + ctx.queryId() + "-s" + ctx.stageId());
        this.drainThread.setDaemon(true);
        this.drainThread.start();
    }

    /**
     * Drain loop body. Runs on {@link #drainThread} from sink construction until the
     * FINAL plan reaches EOF (which only happens after every sender is closed).
     */
    private void drainLoop() {
        try {
            drainOutputIntoDownstream(outStream);
        } catch (Throwable t) {
            drainFailure.set(t);
            logger.warn("[ReduceSink] drain thread terminated with error", t);
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
        // Bridge DataFusion's physical types (e.g. Utf8View for string group keys) to the
        // coordinator's declared schema (Utf8) before handing the batch to Rust. Zero-copy
        // fast path when schemas already match. See coerceToDeclaredSchema().
        batch = coerceToDeclaredSchema(batch, declaredSchema, alloc);
        ArrowArray array = ArrowArray.allocateNew(alloc);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
        try {
            Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
        } catch (Throwable t) {
            array.close();
            arrowSchema.close();
            batch.close();
            throw t;
        } finally {
            batch.close();
        }
        try {
            NativeBridge.senderSend(sender.getPointer(), array.memoryAddress(), arrowSchema.memoryAddress());
            feedCount.incrementAndGet();
        } catch (RuntimeException e) {
            if (closed) {
                logger.debug("[ReduceSink] send-after-close race caught, discarding batch");
                return;
            }
            throw e;
        } finally {
            array.close();
            arrowSchema.close();
        }
    }

    /**
     * Coerces {@code batch} to {@code declaredSchema} at the Java→Rust boundary.
     * Bridges the impedance between DataFusion's physical types (e.g. {@code Utf8View}
     * for string group keys, a non-configurable HashAggregate optimization) and
     * substrait's logical "string" which the coordinator's FINAL plan consumes as
     * {@code Utf8}. One place, explicit, grows per-case on observed mismatch.
     *
     * <p>Zero-copy fast path when schemas already match (numeric-only aggregates).
     * Closes {@code batch} — caller drops its reference.
     *
     * <p><b>TODO (revisit):</b> this runtime coercer is a pragmatic fix. We attempted a
     * plan-level alternative — declare {@code Utf8View} up-front in {@code childSchemas}
     * via {@code LocalStageScheduler#physicalSchemaFor} when the child fragment root is a
     * PARTIAL HashAggregate. That approach hit SIGSEGV in Java's C Data Interface import
     * because DataFusion's optimizer promotes {@code Utf8} → {@code Utf8View} across more
     * operators than just HashAggregate (filter + sort queries like Q26 hit it too),
     * making static prediction of "when does DataFusion emit Utf8View" fragile. Arrow
     * Java 18.3's {@code BufferImportTypeVisitor.visit(Utf8View)} IS functional — the
     * structural issue is predicting the emission, not importing it. Revisit if any of:
     * <ul>
     *   <li>DataFusion exposes a stable way to query "what types will this plan emit?"
     *       that Java can consult instead of guessing.</li>
     *   <li>Substrait grows a type extension carrying view-vs-plain distinction, allowing
     *       the FINAL plan's substrait decoder to natively operate on Utf8View.</li>
     *   <li>We add a Rust normalize pass that casts {@code Utf8View} → {@code Utf8} at
     *       the PARTIAL plan's root using DataFusion's own CastExpr (vectorized, one
     *       pass per column), avoiding the per-cell Java copy here.</li>
     * </ul>
     */
    private static VectorSchemaRoot coerceToDeclaredSchema(VectorSchemaRoot batch, Schema declaredSchema, BufferAllocator alloc) {
        if (batch.getSchema().equals(declaredSchema)) {
            return batch;
        }
        VectorSchemaRoot out = VectorSchemaRoot.create(declaredSchema, alloc);
        try {
            out.allocateNew();
            int rows = batch.getRowCount();
            for (int col = 0; col < declaredSchema.getFields().size(); col++) {
                FieldVector src = batch.getVector(col);
                FieldVector dst = out.getVector(col);
                if (src.getField().getType().equals(dst.getField().getType())) {
                    src.makeTransferPair(dst).transfer();
                    continue;
                }
                ArrowType.ArrowTypeID srcId = src.getField().getType().getTypeID();
                ArrowType.ArrowTypeID dstId = dst.getField().getType().getTypeID();
                if (srcId == ArrowType.ArrowTypeID.Utf8View && dstId == ArrowType.ArrowTypeID.Utf8) {
                    ViewVarCharVector s = (ViewVarCharVector) src;
                    VarCharVector d = (VarCharVector) dst;
                    for (int r = 0; r < rows; r++) {
                        if (s.isNull(r)) {
                            d.setNull(r);
                        } else {
                            d.setSafe(r, s.get(r));
                        }
                    }
                    d.setValueCount(rows);
                    continue;
                }
                throw new IllegalStateException(
                    "coerceToDeclaredSchema: unsupported " + srcId + " → " + dstId + " for column '" + dst.getField().getName() + "'"
                );
            }
            out.setRowCount(rows);
        } catch (RuntimeException e) {
            out.close();
            throw e;
        } finally {
            batch.close();
        }
        return out;
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
        // 1. Signal EOF on every still-open sender. The drain thread, which is already
        // polling the output stream, will receive the final batches and then EOF, then
        // exit cleanly. Senders that were already closed by their ChildSink wrapper are
        // no-ops (the underlying senderClose is idempotent on the Rust side).
        for (DatafusionPartitionSender sender : sendersByChildStageId.values()) {
            try {
                sender.close();
            } catch (Throwable t) {
                failure = accumulate(failure, t);
            }
        }
        // 2. Wait for the drain thread to finish processing remaining output.
        try {
            drainThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = accumulate(failure, e);
        }
        // 3. Surface any error captured by the drain thread.
        Throwable drainErr = drainFailure.get();
        if (drainErr != null) {
            failure = accumulate(failure, drainErr);
        }
        // 4. Close native resources.
        try {
            outStream.close();
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        }
        return failure;
    }

    /** Returns the cumulative number of batches fed into any native sender. */
    public long feedCount() {
        return feedCount.get();
    }
}
