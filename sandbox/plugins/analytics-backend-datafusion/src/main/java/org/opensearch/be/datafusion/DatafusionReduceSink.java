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
import org.apache.arrow.c.CDataDictionaryProvider;
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
 * Coordinator-side {@link ExchangeSink} backed by a native DataFusion local session.
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

    private final ExchangeSinkContext ctx;
    private final NativeRuntimeHandle runtimeHandle;
    private final DatafusionLocalSession session;
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
        this.ctx = ctx;
        this.runtimeHandle = runtimeHandle;
        this.schemaIpc = ArrowSchemaIpc.toBytes(ctx.inputSchema());
        this.session = new DatafusionLocalSession(runtimeHandle.get());
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
            if (sender != null) {
                try {
                    sender.close();
                } catch (Throwable t) {
                    failure = t;
                }
            }
            try {
                drainOutputIntoDownstream();
            } catch (Throwable t) {
                if (failure == null) {
                    failure = t;
                } else {
                    failure.addSuppressed(t);
                }
            }
        } finally {
            if (outStream != null) {
                try {
                    outStream.close();
                } catch (Throwable t) {
                    if (failure == null) {
                        failure = t;
                    } else {
                        failure.addSuppressed(t);
                    }
                }
            }
            try {
                session.close();
            } catch (Throwable t) {
                if (failure == null) {
                    failure = t;
                } else {
                    failure.addSuppressed(t);
                }
            }
            // Intentionally DO NOT close ctx.downstream() here. The downstream
            // accumulates drained result batches (typically a RowProducingSink),
            // and its lifecycle belongs to the walker/orchestrator which still
            // needs to read the buffered batches via outputSource().readResult().
            // Closing it here would clear the batches before the walker reads,
            // causing the query to return an empty result — see LocalStageExecution
            // for the downstream close contract.
        }
        if (failure != null) {
            if (failure instanceof RuntimeException re) {
                throw re;
            }
            if (failure instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(failure);
        }
    }

    private void drainOutputIntoDownstream() {
        BufferAllocator alloc = ctx.allocator();
        try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(outStream.getPointer(), listener));
            Schema outSchema;
            try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr)) {
                Field structField = importField(alloc, arrowSchema, dictProvider);
                outSchema = new Schema(structField.getChildren(), structField.getMetadata());
            }
            while (true) {
                long arrayAddr = asyncCall(listener -> NativeBridge.streamNext(runtimeHandle.get(), outStream.getPointer(), listener));
                if (arrayAddr == 0) {
                    break;
                }
                VectorSchemaRoot vsr = VectorSchemaRoot.create(outSchema, alloc);
                try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)) {
                    Data.importIntoVectorSchemaRoot(alloc, arrowArray, vsr, dictProvider);
                }
                ctx.downstream().feed(vsr);
            }
        }
    }

    private static long asyncCall(Consumer<ActionListener<Long>> call) {
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

    /** Returns the cumulative number of batches fed into the native sender. */
    public long feedCount() {
        return feedCount.get();
    }
}
