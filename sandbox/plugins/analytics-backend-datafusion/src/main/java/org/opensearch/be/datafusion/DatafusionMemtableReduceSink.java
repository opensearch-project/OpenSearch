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
 * {@link #INPUT_ID}).
 */
public final class DatafusionMemtableReduceSink extends AbstractDatafusionReduceSink implements MultiInputExchangeSink {

    /** Per-child buffered Arrow C-data pairs, keyed by child-stage id (iteration order = childInputs). */
    private final Map<Integer, List<ArrowArray>> arraysByChild = new LinkedHashMap<>();
    private final Map<Integer, List<ArrowSchema>> schemasByChild = new LinkedHashMap<>();
    /** Single-fire guard so a stray second {@link #reduce} call doesn't re-execute the plan. */
    private final AtomicBoolean reduceStarted = new AtomicBoolean(false);

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
        BufferAllocator alloc = ctx.allocator();
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
        for (List<ArrowArray> arrays : arraysByChild.values()) {
            for (ArrowArray a : arrays) {
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
