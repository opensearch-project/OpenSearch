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
 * Shared lifecycle skeleton for coordinator-side {@link ExchangeSink}s backed by a native
 * DataFusion local session. Subclasses customise per-batch handling and the close-time
 * native handoff via {@link #feedBatchUnderLock} and {@link #closeUnderLock}.
 *
 * <p>Lifecycle invariants enforced by this base:
 * <ul>
 *   <li>{@link #feed} synchronises on {@link #feedLock}, short-circuits when {@link #closed},
 *       and always closes the supplied {@link VectorSchemaRoot} in {@code finally} regardless
 *       of whether {@link #feedBatchUnderLock} succeeds.</li>
 *   <li>{@link #close} flips {@link #closed} once under {@link #feedLock}, runs the
 *       subclass-specific {@link #closeUnderLock} hook, and unconditionally closes
 *       {@link #session} in {@code finally}, accumulating any failures and rethrowing.</li>
 *   <li>The downstream from {@link ExchangeSinkContext#downstream()} is intentionally NOT
 *       closed here — it accumulates drained results consumed by the walker after the
 *       sink is done.</li>
 * </ul>
 *
 * @opensearch.internal
 */
abstract class AbstractDatafusionReduceSink implements ExchangeSink {

    /** Substrait/DataFusion table name for the single registered input partition. */
    // TODO: This will change to represent child ID and taken as input in context
    // for now we have a single partition.
    static final String INPUT_ID = "input-0";

    protected final ExchangeSinkContext ctx;
    protected final NativeRuntimeHandle runtimeHandle;
    protected final DatafusionLocalSession session;
    protected final byte[] schemaIpc;

    /** Guards {@link #closed} and serialises {@link #feed}/{@link #close} against producers. */
    protected final Object feedLock = new Object();

    /** Set once in {@link #close} under {@link #feedLock}. Visible to all threads via volatile. */
    protected volatile boolean closed;

    protected AbstractDatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
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
            try {
                feedBatchUnderLock(batch);
            } finally {
                batch.close();
            }
        }
    }

    @Override
    public final void close() {
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
            try {
                session.close();
            } catch (Throwable t) {
                failure = accumulate(failure, t);
            }
        }
        rethrow(failure);
    }

    /**
     * Per-batch hook. Called inside {@code synchronized(feedLock)} after {@code closed} is
     * verified false. Implementations export and hand off (or buffer) {@code batch} via the
     * native bridge. Implementations MUST NOT close {@code batch} — the base class does that
     * in {@code finally}.
     */
    protected abstract void feedBatchUnderLock(VectorSchemaRoot batch);

    /**
     * Subclass-specific shutdown. Runs after {@link #closed} is set and before
     * {@link #session} is closed. Implementations should close their owned native resources
     * (sender, output stream, accumulated FFI structs, …) and drain any pending output.
     *
     * @return the first failure encountered (use {@link #accumulate(Throwable, Throwable)}
     *         when multiple steps may fail), or {@code null} on clean shutdown.
     */
    protected abstract Throwable closeUnderLock();

    /**
     * Drains a native output stream into {@link ExchangeSinkContext#downstream()}, importing
     * each {@link ArrowArray} into a fresh {@link VectorSchemaRoot} on the Java side.
     */
    protected final void drainOutputIntoDownstream(StreamHandle outStream) {
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

    /**
     * Synchronously awaits the result of an async native call expressed as a
     * {@code Consumer<ActionListener<Long>>}. Restores interrupt state on
     * {@link InterruptedException} and unwraps {@link ExecutionException} to surface the
     * original cause.
     */
    protected static long asyncCall(Consumer<ActionListener<Long>> call) {
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
