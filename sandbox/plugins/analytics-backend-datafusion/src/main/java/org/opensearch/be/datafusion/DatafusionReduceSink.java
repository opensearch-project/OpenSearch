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
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * Streaming coordinator-side {@link ExchangeSink}: opens a native partition stream up front,
 * pushes each fed batch through a tokio mpsc-backed sender, and on close drains the native
 * output stream into {@link ExchangeSinkContext#downstream()}.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Constructor creates a {@link DatafusionLocalSession}, registers the input partition
 *       stream, and kicks off native execution against the supplied Substrait bytes.</li>
 *   <li>{@link #feed} exports each batch via Arrow C Data and hands it to the native sender.
 *       Thread-safe — multiple shard response handlers may call {@code feed} concurrently.
 *       No Java-side lock is held during the export or send; backpressure comes from the
 *       native Rust mpsc channel (bounded, capacity 4). The send-after-close race is handled
 *       by catching the native error when the receiver has been dropped.</li>
 *   <li>{@link #close} closes the sender (EOF), drains the native output stream into
 *       {@link ExchangeSinkContext#downstream()}, and releases all native resources.
 *       Idempotent.</li>
 * </ol>
 *
 * <p>Memory accounting: the per-query Arrow {@link BufferAllocator} enforces the hard memory
 * limit — when exceeded, Arrow throws {@code OutOfMemoryException} which propagates to the
 * caller and transitions the stage to FAILED.
 */
public final class DatafusionReduceSink implements ExchangeSink {

    private static final Logger logger = LogManager.getLogger(DatafusionReduceSink.class);

    static final String INPUT_ID = "input-0";

    private final DatafusionPartitionSender sender;
    private final StreamHandle outStream;
    private final byte[] schemaIpc;
    /**
     * Volatile flag set exactly once by {@link #close()}. Used as a best-effort
     * fast path in {@link #feed} to avoid unnecessary Arrow export work after
     * close. NOT a correctness gate — the native send error handles the race.
     */
    private volatile boolean closed;
    /** Cumulative batches fed into the native sender. */
    private final AtomicLong feedCount = new AtomicLong();

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
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        // Best-effort fast path — skip export work if already closed.
        if (closed) {
            batch.close();
            return;
        }
        // Export Arrow C Data outside any lock. The allocator is thread-safe;
        // multiple shard handlers can export concurrently. Backpressure comes
        // from the native Rust mpsc channel (bounded, capacity 4) — senderSend
        // blocks when the channel is full via Tokio handle.block_on().
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
        // No lock — send directly. The Rust mpsc::Sender is thread-safe and
        // handles concurrent sends internally. If close() raced and already
        // called senderClose(), the native side returns an error ("receiver
        // dropped") which we catch and discard silently.
        try {
            NativeBridge.senderSend(sender.getPointer(), array.memoryAddress(), arrowSchema.memoryAddress());
            feedCount.incrementAndGet();
        } catch (RuntimeException e) {
            if (closed) {
                // Expected race: close() was called between our volatile check
                // and the native send. The batch was already exported but the
                // receiver is gone — just clean up and return.
                logger.debug("[ReduceSink] send-after-close race caught, discarding batch");
                return;
            }
            throw e;
        } finally {
            // ArrowArray/ArrowSchema Java wrappers can be closed after send —
            // native side has taken ownership of the underlying FFI structs.
            array.close();
            arrowSchema.close();
        }
    }

    @Override
    public void close() {
        // Single-writer: only LocalStageExecution.start() calls close().
        // The volatile flag is sufficient — no lock needed.
        if (closed) {
            return;
        }
        closed = true;
        Throwable failure = null;
        try {
            sender.close();
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        }
        try {
            drainOutputIntoDownstream(outStream);
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        }
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
