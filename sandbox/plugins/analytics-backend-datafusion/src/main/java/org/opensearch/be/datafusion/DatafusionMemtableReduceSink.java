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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * Memtable variant of {@link DatafusionReduceSink}: instead of opening a streaming partition
 * and pushing each shard response through it, this sink buffers every fed
 * {@link VectorSchemaRoot} as an exported Arrow C Data pair and on {@link #close()} hands the
 * full set across in one native call. The native side builds a {@code MemTable}, registers it,
 * and runs the Substrait plan against the materialized input.
 *
 * <p>Trade-offs:
 * <ul>
 *   <li>+ No tokio mpsc, no cross-runtime spawn machinery in the input path. The single-shot
 *       handoff is simpler to reason about and matches the lifecycle already used for the
 *       output stream.</li>
 *   <li>− All input batches live in memory until {@code close()}. Use the streaming sink when
 *       the working set is too large to retain.</li>
 * </ul>
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Constructor creates a {@link DatafusionLocalSession}. No partition is registered yet.</li>
 *   <li>{@link #feed} exports each batch via Arrow C Data, retains the FFI structs, and closes
 *       the {@link VectorSchemaRoot}. Thread-safe.</li>
 *   <li>{@link #close} calls {@link NativeBridge#registerMemtable} once with all accumulated
 *       batches, then executes the plan and drains the output into
 *       {@link ExchangeSinkContext#downstream()}. Idempotent.</li>
 * </ol>
 *
 * <p>Like the streaming variant, this sink intentionally does NOT close the downstream — the
 * walker/orchestrator owns its lifecycle.
 */
public final class DatafusionMemtableReduceSink implements ExchangeSink {

    static final String INPUT_ID = "input-0";

    private final ExchangeSinkContext ctx;
    private final NativeRuntimeHandle runtimeHandle;
    private final DatafusionLocalSession session;
    private final byte[] schemaIpc;
    private final List<ArrowArray> arrays = new ArrayList<>();
    private final List<ArrowSchema> schemas = new ArrayList<>();
    private final Object feedLock = new Object();
    private volatile boolean closed;

    public DatafusionMemtableReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        this.ctx = ctx;
        this.runtimeHandle = runtimeHandle;
        this.schemaIpc = ArrowSchemaIpc.toBytes(ctx.inputSchema());
        this.session = new DatafusionLocalSession(runtimeHandle.get());
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        synchronized (feedLock) {
            if (closed) {
                batch.close();
                return;
            }
            BufferAllocator alloc = ctx.allocator();
            ArrowArray array = ArrowArray.allocateNew(alloc);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
            try {
                Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
                arrays.add(array);
                schemas.add(arrowSchema);
                array = null;
                arrowSchema = null;
            } finally {
                if (array != null) {
                    array.close();
                }
                if (arrowSchema != null) {
                    arrowSchema.close();
                }
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
        long streamPtr = 0;
        try {
            long[] arrayPtrs = new long[arrays.size()];
            long[] schemaPtrs = new long[schemas.size()];
            for (int i = 0; i < arrays.size(); i++) {
                arrayPtrs[i] = arrays.get(i).memoryAddress();
                schemaPtrs[i] = schemas.get(i).memoryAddress();
            }
            NativeBridge.registerMemtable(session.getPointer(), INPUT_ID, schemaIpc, arrayPtrs, schemaPtrs);

            streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
            try (StreamHandle outStream = new StreamHandle(streamPtr, runtimeHandle)) {
                streamPtr = 0;
                drainOutputIntoDownstream(outStream);
            }
        } catch (Throwable t) {
            failure = t;
        } finally {
            // The Arrow Java wrappers must always be closed. On the success path Rust has
            // consumed the underlying FFI structs (release callback nulled), so close is a
            // no-op for the data. On the failure-before-handoff path close releases the
            // exported data buffers back to the Java allocator.
            for (ArrowArray a : arrays) {
                try {
                    a.close();
                } catch (Throwable t) {
                    if (failure == null) {
                        failure = t;
                    } else {
                        failure.addSuppressed(t);
                    }
                }
            }
            for (ArrowSchema s : schemas) {
                try {
                    s.close();
                } catch (Throwable t) {
                    if (failure == null) {
                        failure = t;
                    } else {
                        failure.addSuppressed(t);
                    }
                }
            }
            arrays.clear();
            schemas.clear();
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
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

    private void drainOutputIntoDownstream(StreamHandle outStream) {
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
