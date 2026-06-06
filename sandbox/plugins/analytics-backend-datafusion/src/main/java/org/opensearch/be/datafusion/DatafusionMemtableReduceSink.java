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
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Buffered variant of {@link DatafusionReduceSink}: every fed batch is exported as an Arrow
 * C Data pair and held until {@link #reduce} hands the full set to native in one
 * {@code registerMemtable + executeLocalPlan + drain} call. No tokio mpsc in the input path.
 *
 * <p>Trade-off: all input batches live in memory until {@link #reduce}. Use the streaming
 * sink when the working set is too large to retain.
 *
 * <p><b>Single-input only</b> — the constructor rejects multi-input shapes. The
 * {@link DataFusionAnalyticsBackendPlugin} provider auto-falls-back to streaming when
 * {@code childInputs.size() > 1}, so callers shouldn't see this in practice.
 *
 * <p>TODO: support multi-input by registering one {@code MemTable} per child stage with
 * per-child buffer accumulation (mirroring the streaming sink's {@code ChildSink} approach).
 */
public final class DatafusionMemtableReduceSink extends AbstractDatafusionReduceSink {

    private final List<ArrowArray> arrays = new ArrayList<>();
    private final List<ArrowSchema> schemas = new ArrayList<>();
    private final byte[] producerPlanBytes;
    /** Single-fire guard so a stray second {@link #reduce} call doesn't re-execute the plan. */
    private final AtomicBoolean reduceStarted = new AtomicBoolean(false);

    public DatafusionMemtableReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
        // Fail fast and close the parent-allocated native session before propagating —
        // super() opened a DatafusionLocalSession that would otherwise leak on construction failure.
        if (childInputs.size() != 1) {
            try {
                session.close();
            } catch (Throwable ignore) {
                // Original IllegalStateException carries the actionable message; suppress cleanup errors.
            }
            throw new IllegalStateException(
                "DatafusionMemtableReduceSink supports a single input only; got "
                    + childInputs.size()
                    + " child inputs. Use streaming mode (DatafusionReduceSink) for multi-input shapes,"
                    + " or set "
                    + DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE.getKey()
                    + "=streaming. The DataFusionAnalyticsBackendPlugin sink provider auto-falls-back"
                    + " when this limit is hit at request time, so reaching here means the sink was"
                    + " constructed directly."
            );
        }
        this.producerPlanBytes = childInputs.values().iterator().next();
    }

    /** Memtable buffers all input first — never schedule concurrently with feeds. */
    @Override
    public boolean supportsEagerScheduling() {
        return false;
    }

    @Override
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
        BufferAllocator alloc = ctx.allocator();
        ViewVectorSanitizer.sanitize(batch);
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
        }
    }

    /**
     * Registers the buffered batches as a MemTable, executes the plan, drains into the
     * downstream. Holds {@link #feedLock} for the duration so a concurrent {@link #close}
     * (or late {@link #feed}) can't race the buffer-list iteration. Releases the Arrow
     * Java wrappers in {@code finally} — on success Rust already consumed the underlying
     * data (release callback nulled), so wrapper close is a no-op.
     */
    @Override
    public void reduce(ActionListener<Void> listener) {
        assert reduceStarted.compareAndSet(false, true) : "reduce called more than once";
        Exception failure = null;
        synchronized (feedLock) {
            if (closed) {
                listener.onFailure(new IllegalStateException("sink closed before reduce"));
                return;
            }
            long streamPtr = 0;
            try {
                long[] arrayPtrs = new long[arrays.size()];
                long[] schemaPtrs = new long[schemas.size()];
                for (int i = 0; i < arrays.size(); i++) {
                    arrayPtrs[i] = arrays.get(i).memoryAddress();
                    schemaPtrs[i] = schemas.get(i).memoryAddress();
                }
                // Multi-input would need one registerMemtable call per child stage with a
                // distinct "input-<childStageId>" table id and separate buffer accumulation
                // per child (the constructor enforces single-input today; see class javadoc).
                int singleChildStageId = childInputs.keySet().iterator().next();
                NativeBridge.RegisteredInput registered = NativeBridge.registerMemtable(
                    session.getPointer(),
                    inputIdFor(singleChildStageId),
                    producerPlanBytes,
                    arrayPtrs,
                    schemaPtrs
                );
                childSchemas.put(singleChildStageId, ArrowSchemaIpc.fromBytes(registered.schemaIpc()));

                streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes(), ctx.taskId());
                try (StreamHandle outStream = new StreamHandle(streamPtr, runtimeHandle)) {
                    streamPtr = 0;
                    drainOutputIntoDownstream(outStream);
                }
            } catch (Exception t) {
                failure = accumulate(failure, t);
            } finally {
                failure = releaseBuffersInto(failure);
                if (streamPtr != 0) {
                    NativeBridge.streamClose(streamPtr);
                }
            }
        }
        if (failure == null) {
            listener.onResponse(null);
        } else {
            listener.onFailure(failure);
        }
    }

    /** Releases any buffers reduce() didn't consume (cancel-before-reduce path), then session. */
    @Override
    protected Exception closeImpl() {
        Exception failure = releaseBuffersInto(null);
        if (preparedState == null) {
            try {
                session.close();
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        return failure;
    }

    private Exception releaseBuffersInto(Exception failure) {
        for (ArrowArray a : arrays) {
            try {
                a.close();
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        for (ArrowSchema s : schemas) {
            try {
                s.close();
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        arrays.clear();
        schemas.clear();
        return failure;
    }
}
