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
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streaming coordinator-side reduce sink: opens a native partition stream up front,
 * pushes each fed batch through a tokio mpsc-backed sender, and on close drains the native
 * output stream into {@link ExchangeSinkContext#downstream()}.
 *
 * <p>Overrides the base class's {@code synchronized(feedLock)} with a lock-free
 * implementation. Multiple shard response handlers call {@link #feed} concurrently;
 * backpressure comes from the native Rust mpsc channel (bounded, capacity 4).
 * The send-after-close race is handled by catching the native error when the
 * receiver has been dropped.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Constructor registers the input partition stream and kicks off native execution.</li>
 *   <li>{@link #feed} exports each batch via Arrow C Data and sends it lock-free.</li>
 *   <li>{@link #close} closes the sender (EOF), drains output, releases native resources.</li>
 * </ol>
 */
public final class DatafusionReduceSink extends AbstractDatafusionReduceSink {

    private static final Logger logger = LogManager.getLogger(DatafusionReduceSink.class);

    private final DatafusionPartitionSender sender;
    private final StreamHandle outStream;
    /** Cumulative batches fed into the native sender. */
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
     * when the FINAL plan reaches EOF (after {@link #sender}.close() signals input
     * EOF and DataFusion completes the last aggregation).
     */
    private final Thread drainThread;
    /** Captures any throwable from the drain thread for surfacing during close(). */
    private final AtomicReference<Throwable> drainFailure = new AtomicReference<>();

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
        long senderPtr = 0;
        long streamPtr = 0;
        try {
            senderPtr = NativeBridge.registerPartitionStream(session.getPointer(), INPUT_ID, schemaIpc);
            this.sender = new DatafusionPartitionSender(senderPtr);
            streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
            this.outStream = new StreamHandle(streamPtr, runtimeHandle);
        } catch (RuntimeException e) {
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
            if (senderPtr != 0) {
                NativeBridge.senderClose(senderPtr);
            }
            session.close();
            throw e;
        }
        // Spawn the drain thread AFTER the native handles are constructed so the catch-block
        // doesn't have to deal with thread teardown on construction failure.
        this.drainThread = new Thread(this::drainLoop, "df-reduce-drain-q" + ctx.queryId() + "-s" + ctx.stageId());
        this.drainThread.setDaemon(true);
        this.drainThread.start();
    }

    /**
     * Drain loop body. Runs on {@link #drainThread} from sink construction until the
     * FINAL plan reaches EOF (which only happens after {@code sender.close()} is called
     * by {@link #closeUnderLock}).
     *
     * <p>Polls {@link #outStream} via {@code streamNext} and forwards each emitted batch
     * to {@code ctx.downstream()}. Any throwable is captured in {@link #drainFailure}
     * and re-surfaced from {@link #closeUnderLock} via the existing accumulate pattern.
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
     * Lock-free feed: overrides the base class's synchronized feed.
     * Arrow C Data export and native send happen without any Java-side lock.
     * Backpressure comes from the Rust mpsc channel; the send-after-close
     * race is caught via the native error.
     */
    @Override
    public void feed(VectorSchemaRoot batch) {
        // Best-effort fast path — skip export work if already closed.
        if (closed) {
            batch.close();
            return;
        }
        // Export Arrow C Data outside any lock. The allocator is thread-safe;
        // multiple shard handlers can export concurrently.
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
        // No lock — send directly. The Rust mpsc::Sender is thread-safe.
        // If close() raced and already called senderClose(), the native side
        // returns an error ("receiver dropped") which we catch and discard.
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
     * Not used — feed() is overridden directly. Required by the abstract class contract.
     */
    @Override
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("DatafusionReduceSink overrides feed() directly");
    }

    @Override
    protected Throwable closeUnderLock() {
        Throwable failure = null;
        // 1. Signal EOF on input. The drain thread, which is already polling the output
        // stream, will receive the final batches and then EOF, then exit cleanly.
        try {
            sender.close();
        } catch (Throwable t) {
            failure = accumulate(failure, t);
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

    /** Returns the cumulative number of batches fed into the native sender. */
    public long feedCount() {
        return feedCount.get();
    }
}
