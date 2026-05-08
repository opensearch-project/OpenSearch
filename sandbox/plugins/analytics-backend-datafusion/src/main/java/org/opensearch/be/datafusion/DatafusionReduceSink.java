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
        super(ctx, runtimeHandle);
        Map<Integer, DatafusionPartitionSender> senders = new LinkedHashMap<>(childInputs.size());
        long streamPtr = 0;
        try {
            // Register one native partition per child stage. The Substrait plan in
            // ctx.fragmentBytes() references each partition by its "input-<stageId>" name
            // (DataFusionFragmentConvertor names them this way during plan conversion).
            for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                int childStageId = child.getKey();
                byte[] schemaIpc = child.getValue();
                long senderPtr = NativeBridge.registerPartitionStream(session.getPointer(), inputIdFor(childStageId), schemaIpc);
                senders.put(childStageId, new DatafusionPartitionSender(senderPtr));
            }
            streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
            this.outStream = new StreamHandle(streamPtr, runtimeHandle);
        } catch (RuntimeException e) {
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
            for (DatafusionPartitionSender sender : senders.values()) {
                try {
                    sender.close();
                } catch (Throwable ignore) {}
            }
            session.close();
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
        feedToSender(sendersByChildStageId.values().iterator().next(), batch);
    }

    @Override
    public ExchangeSink sinkForChild(int childStageId) {
        DatafusionPartitionSender sender = sendersByChildStageId.get(childStageId);
        if (sender == null) {
            throw new IllegalArgumentException(
                "No registered partition for childStageId=" + childStageId + "; known ids=" + sendersByChildStageId.keySet()
            );
        }
        return new ChildSink(sender);
    }

    /**
     * Lock-free per-sender feed. Exports the batch via Arrow C Data outside any lock
     * (the allocator is thread-safe; multiple shard handlers can export concurrently),
     * then sends it through the supplied sender. The Rust mpsc::Sender is thread-safe,
     * so multiple producers feeding the same sender is safe. If close() raced and
     * already ran senderClose, the native side returns an error ("receiver dropped")
     * which we catch and discard.
     */
    private void feedToSender(DatafusionPartitionSender sender, VectorSchemaRoot batch) {
        // Best-effort fast path — skip export work if already closed.
        if (closed) {
            batch.close();
            return;
        }
        BufferAllocator alloc = ctx.allocator();
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
     * Per-child wrapper returned from {@link #sinkForChild(int)}. The orchestrator
     * routes one of these per child stage, and the wrapper's close() signals EOF for
     * its specific input partition. Idempotent — duplicate close() calls are no-ops.
     */
    private final class ChildSink implements ExchangeSink {
        private final DatafusionPartitionSender sender;
        private volatile boolean childClosed;

        ChildSink(DatafusionPartitionSender sender) {
            this.sender = sender;
        }

        @Override
        public void feed(VectorSchemaRoot batch) {
            feedToSender(sender, batch);
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
