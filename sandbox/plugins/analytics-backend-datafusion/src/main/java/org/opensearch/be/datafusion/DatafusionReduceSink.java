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
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
 *       Thread-safe — multiple shard response handlers may call {@code feed} concurrently.</li>
 *   <li>{@link #close} closes the sender (EOF), drains the native output stream into
 *       {@link ExchangeSinkContext#downstream()}, closes the downstream, and releases all
 *       native resources. Idempotent.</li>
 * </ol>
 */
public final class DatafusionReduceSink implements ExchangeSink {

    static final String INPUT_ID = "input-0";

    private final ExchangeSinkContext ctx;
    private final NativeRuntimeHandle runtimeHandle;
    private final DatafusionLocalSession session;
    private final DatafusionPartitionSender sender;
    private final StreamHandle outStream;
    private final byte[] schemaIpc;
    private final Object feedLock = new Object();
    private volatile boolean closed;

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
        synchronized (feedLock) {
            if (closed) {
                batch.close();
                return;
            }
            BufferAllocator alloc = ctx.allocator();
            try (ArrowArray array = ArrowArray.allocateNew(alloc); ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc)) {
                Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
                NativeBridge.senderSend(sender.getPointer(), array.memoryAddress(), arrowSchema.memoryAddress());
            } finally {
                batch.close();
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
}
