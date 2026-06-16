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
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Buffered variant of {@link DatafusionReduceSink}: every fed batch is exported as an Arrow
 * C Data pair and held until {@link #reduce} hands the full set(s) to native via one
 * {@code registerMemtable} call per child input, then {@code executeLocalPlan + drain}. No
 * tokio mpsc in the input path.
 *
 * <p><b>Why buffered, especially for multi-input.</b> The streaming sink
 * ({@link DatafusionReduceSink}) registers each input as a bounded-mpsc-backed
 * {@code StreamingTable} and drains the plan concurrently with producer feeds. For a
 * <em>multi-input</em> coordinator operator (e.g. a {@code Join} over two SINGLETON-reduced
 * inputs) that concurrency can deadlock: a {@code HashJoinExec} collects its build side fully
 * before reading the probe, so if the build-side producer's cross-node delivery back-pressures
 * on a bounded input mpsc the join is not yet draining, the producer blocks, never signals EOF,
 * and the join's build never completes (observed on TPC-H q15). Buffering each input fully in
 * Java before execution removes every bounded native channel from the input path, so producers
 * always run to completion + EOF, and the plan executes over complete in-memory inputs.
 *
 * <p>Trade-off: all input batches live in memory until {@link #reduce}. For very large streaming
 * reductions prefer {@link DatafusionReduceSink} via {@code datafusion.reduce.input_mode=streaming}.
 *
 * <p><b>Multi-input.</b> {@link #sinkForChild(int)} returns a per-child buffering wrapper; each
 * child stage's output accumulates into its own batch list keyed by child-stage id and registers
 * as a distinct {@code "input-<childStageId>"} {@code MemTable} at {@link #reduce} time. The
 * single-input case keeps the bare {@link #feed(VectorSchemaRoot)} path (one input under
 * {@link AbstractDatafusionReduceSink#INPUT_ID}).
 */
public final class DatafusionMemtableReduceSink extends AbstractDatafusionReduceSink implements MultiInputExchangeSink {

    /** Per-child buffered Arrow C-data pairs, keyed by child-stage id (iteration order = childInputs). */
    private final Map<Integer, List<ArrowArray>> arraysByChild = new LinkedHashMap<>();
    private final Map<Integer, List<ArrowSchema>> schemasByChild = new LinkedHashMap<>();
    /** Single-fire guard so a stray second {@link #reduce} call doesn't re-execute the plan. */
    private final AtomicBoolean reduceStarted = new AtomicBoolean(false);

    /**
     * Per-query off-heap budget for the buffered inputs: the limit of the query's Arrow
     * allocator (set by {@code analytics.coordinator.buffer_limit}). {@link Long#MAX_VALUE}
     * means the operator disabled the per-query limit (shared coordinator allocator) — the
     * size guard is then inert and the path behaves exactly as before. The live usage is read
     * from the allocator itself ({@code getAllocatedMemory}); we only cache the limit here.
     */
    private final long bufferBudget;

    public DatafusionMemtableReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
        if (childInputs.isEmpty()) {
            try {
                session.close();
            } catch (Throwable ignore) {
                // Original IllegalStateException carries the actionable message; suppress cleanup errors.
            }
            throw new IllegalStateException("DatafusionMemtableReduceSink requires at least one child input");
        }
        for (Integer childStageId : childInputs.keySet()) {
            arraysByChild.put(childStageId, new ArrayList<>());
            schemasByChild.put(childStageId, new ArrayList<>());
        }
        this.bufferBudget = ctx.allocator().getLimit();
    }

    /** Memtable buffers all input first — never schedule concurrently with feeds (no bounded
     *  channel in the input path means non-eager is also deadlock-free for multi-input). */
    @Override
    public boolean supportsEagerScheduling() {
        return false;
    }

    /**
     * Single-input feed: buffers into the sole child's batch list. Multi-input callers route via
     * {@link #sinkForChild(int)} instead.
     */
    @Override
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
        if (childInputs.size() != 1) {
            throw new IllegalStateException(
                "DatafusionMemtableReduceSink has " + childInputs.size() + " inputs; use sinkForChild(int) instead of feed()"
            );
        }
        bufferBatch(childInputs.keySet().iterator().next(), batch);
    }

    /** Exports {@code batch} as an Arrow C-data pair into the given child's buffer. */
    private void bufferBatch(int childStageId, VectorSchemaRoot batch) {
        // Fail fast BEFORE allocating, so the per-query Arrow allocator never hits a hard limit.
        // The coordinator-centric reduce buffers every input fully, so these retained buffers
        // dominate the allocator's usage. A raw Arrow OutOfMemoryException can fire at ANY
        // allocation site (here, the FFI export, the Flight receive, the native drain) and cannot
        // be reliably caught — once thrown it leaks the in-flight buffers and the cause is masked
        // as a TaskCancelledException. So we refuse the batch up front when retaining it would not
        // fit, with an actionable exception.
        //
        // The binding limit is the SMALLER of two ceilings, so we check both:
        // (1) the per-query budget (this allocator's own limit, analytics.coordinator.buffer_limit)
        // — projected against live usage (getAllocatedMemory), and
        // (2) the allocator's getHeadroom(), which Arrow computes across the ANCESTOR chain
        // (the shared POOL_QUERY pool can be smaller than this child's nominal limit, so
        // getLimit() alone overstates what's actually grantable — getHeadroom() does not).
        // exportVectorSchemaRoot shares the batch buffers by reference plus allocates per-buffer
        // private data; we add an FFI margin so the clean failure trips before the raw OOM. Inert
        // when the budget is unbounded (Long.MAX_VALUE — operator disabled the per-query limit).
        BufferAllocator alloc = ctx.allocator();
        long batchBytes = bufferSize(batch);
        long need = batchBytes + FFI_EXPORT_OVERHEAD_BYTES;
        if (bufferBudget != Long.MAX_VALUE) {
            long projected = alloc.getAllocatedMemory() + need;
            if (projected > bufferBudget) {
                // batch is closed by the caller's try-with-resources; do not allocate.
                throw new ReduceSizeExceededException(projected, bufferBudget);
            }
        }
        // Ancestor-aware check: even within the per-query budget, the shared pool may be exhausted.
        // getHeadroom() returns the effective grantable bytes up the allocator tree.
        if (need > alloc.getHeadroom()) {
            throw new ReduceSizeExceededException(alloc.getAllocatedMemory() + need, alloc.getAllocatedMemory() + alloc.getHeadroom());
        }
        ArrowArray array = ArrowArray.allocateNew(alloc);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
        try {
            Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
            arraysByChild.get(childStageId).add(array);
            schemasByChild.get(childStageId).add(arrowSchema);
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
     * Headroom margin reserved per buffered batch for the Arrow C-Data export's control
     * structures (the {@code ArrowArray} + {@code ArrowSchema} FFI structs and per-buffer
     * {@code ExportedArrayPrivateData}). Keeps the clean {@link ReduceSizeExceededException}
     * tripping before the raw allocator-limit OOM.
     */
    private static final long FFI_EXPORT_OVERHEAD_BYTES = 64 * 1024;

    /** Sums per-vector buffer sizes; the off-heap cost of retaining this batch's data. */
    private static long bufferSize(VectorSchemaRoot batch) {
        long total = 0L;
        for (org.apache.arrow.vector.FieldVector v : batch.getFieldVectors()) {
            total += v.getBufferSize();
        }
        return total;
    }

    @Override
    public ExchangeSink sinkForChild(int childStageId) {
        if (!arraysByChild.containsKey(childStageId)) {
            throw new IllegalArgumentException(
                "No registered input for childStageId=" + childStageId + "; known ids=" + arraysByChild.keySet()
            );
        }
        return new ChildSink(childStageId);
    }

    /**
     * Registers each child's buffered batches as a distinct {@code "input-<childStageId>"}
     * MemTable, executes the plan, drains into the downstream. Holds {@link #feedLock} for the
     * duration so a concurrent {@link #close} (or late {@link #feed}) can't race the buffer-list
     * iteration. Releases the Arrow Java wrappers in {@code finally}.
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
                for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                    int childStageId = child.getKey();
                    List<ArrowArray> arrays = arraysByChild.get(childStageId);
                    List<ArrowSchema> schemas = schemasByChild.get(childStageId);
                    long[] arrayPtrs = new long[arrays.size()];
                    long[] schemaPtrs = new long[schemas.size()];
                    for (int i = 0; i < arrays.size(); i++) {
                        arrayPtrs[i] = arrays.get(i).memoryAddress();
                        schemaPtrs[i] = schemas.get(i).memoryAddress();
                    }
                    NativeBridge.RegisteredInput registered = NativeBridge.registerMemtable(
                        session.getPointer(),
                        inputIdFor(childStageId),
                        child.getValue(),
                        arrayPtrs,
                        schemaPtrs
                    );
                    // register_memtable MOVED these FFI structs into native (FFI_ArrowArray::from_raw);
                    // they no longer hold Java-side buffers. Drop the container references now (close
                    // only, no release) and remove them from the lists so the finally's
                    // release-then-close path only ever touches inputs native did NOT consume — a
                    // partial failure (child A imported, child B throws) then frees B correctly
                    // without double-releasing A.
                    releaseConsumedChild(childStageId);
                    childSchemas.put(childStageId, ArrowSchemaIpc.fromBytes(registered.schemaIpc()));
                }

                streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes(), ctx.taskId());
                try (StreamHandle outStream = new StreamHandle(streamPtr, runtimeHandle)) {
                    streamPtr = 0;
                    drainOutputIntoDownstream(outStream);
                }
            } catch (Exception t) {
                failure = accumulate(failure, t);
            } finally {
                // Anything left in the lists was NOT consumed by native (a child that threw before
                // import, or all children if executeLocalPlan failed pre-registration) — release+close.
                failure = releaseUnconsumedBuffers(failure);
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

    /**
     * Releases any buffers {@link #reduce} didn't consume, then the session. When {@code reduce()}
     * ran, it already cleared the per-child lists in its {@code finally}, so this finds them empty.
     * When the sink is closed BEFORE {@code reduce()} (cancel path — e.g. a size-guard
     * {@link ReduceSizeExceededException} on a sibling input, or an external cancel), the exported
     * Arrow C-data structs were never imported by native and still hold live buffers: they must be
     * {@code release()}d (fire the C release callback to free the buffers) THEN {@code close()}d
     * (free the 128-byte FFI container). {@code close()} alone leaks the buffers.
     */
    @Override
    protected Exception closeImpl() {
        Exception failure = releaseUnconsumedBuffers(null);
        if (preparedState == null) {
            try {
                session.close();
            } catch (Exception t) {
                failure = accumulate(failure, t);
            }
        }
        return failure;
    }

    /**
     * Drops the container references for ONE child whose FFI structs native just consumed via
     * {@code register_memtable} ({@code FFI_ArrowArray::from_raw} moved them out of Java). Java
     * only frees the 128-byte container with {@code close()} — calling {@code release()} here would
     * double-fire the C release callback Rust now owns. Removes the child's lists so the reduce
     * {@code finally}'s release-then-close path never touches consumed inputs. Swallows close errors
     * (best-effort; the reduce result is the meaningful signal).
     */
    private void releaseConsumedChild(int childStageId) {
        List<ArrowArray> arrays = arraysByChild.get(childStageId);
        if (arrays != null) {
            for (ArrowArray a : arrays) {
                try {
                    a.close();
                } catch (Exception ignore) {
                    // container free only; data buffers are owned by native now
                }
            }
            arrays.clear();
        }
        List<ArrowSchema> schemas = schemasByChild.get(childStageId);
        if (schemas != null) {
            for (ArrowSchema s : schemas) {
                try {
                    s.close();
                } catch (Exception ignore) {
                    // container free only
                }
            }
            schemas.clear();
        }
    }

    /**
     * Frees buffers that native never imported (cancel-before-reduce, or a child that threw before
     * its import): {@code release()} fires the C release callback to free the data buffers, then
     * {@code close()} frees the FFI container. {@code close()} alone leaks the buffers. Used by
     * {@link #closeImpl} and {@link #reduce}'s {@code finally}.
     */
    private Exception releaseUnconsumedBuffers(Exception failure) {
        for (List<ArrowArray> arrays : arraysByChild.values()) {
            for (ArrowArray a : arrays) {
                try {
                    a.release();
                } catch (Exception t) {
                    failure = accumulate(failure, t);
                }
                try {
                    a.close();
                } catch (Exception t) {
                    failure = accumulate(failure, t);
                }
            }
            arrays.clear();
        }
        for (List<ArrowSchema> schemas : schemasByChild.values()) {
            for (ArrowSchema s : schemas) {
                try {
                    s.release();
                } catch (Exception t) {
                    failure = accumulate(failure, t);
                }
                try {
                    s.close();
                } catch (Exception t) {
                    failure = accumulate(failure, t);
                }
            }
            schemas.clear();
        }
        return failure;
    }

    /**
     * Per-child buffering wrapper handed to the orchestrator via {@link #sinkForChild(int)}. Each
     * fed batch is exported + appended to its child's buffer; {@code close()} is a no-op (the
     * parent sink's {@link #reduce}/{@link #close} own the buffer lifecycle).
     */
    private final class ChildSink implements ExchangeSink {
        private final int childStageId;

        ChildSink(int childStageId) {
            this.childStageId = childStageId;
        }

        @Override
        public void feed(VectorSchemaRoot batch) {
            synchronized (feedLock) {
                if (closed) {
                    batch.close();
                    return;
                }
                try (batch) {
                    bufferBatch(childStageId, batch);
                }
            }
        }

        @Override
        public void close() {
            // Per-child EOF carries no native signal in the buffered path — the parent's reduce()
            // registers each child's accumulated buffer as a complete MemTable. No-op.
        }
    }
}
