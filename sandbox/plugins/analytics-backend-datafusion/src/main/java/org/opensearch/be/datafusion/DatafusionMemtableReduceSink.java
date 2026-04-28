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

import java.util.ArrayList;
import java.util.List;

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
 * <p>Lifecycle invariants and {@code feed}/{@code close} skeleton are implemented in
 * {@link AbstractDatafusionReduceSink}. This subclass owns the buffered FFI structs and the
 * close-time {@code registerMemtable + executeLocalPlan + drain} sequence.
 */
public final class DatafusionMemtableReduceSink extends AbstractDatafusionReduceSink {

    private final List<ArrowArray> arrays = new ArrayList<>();
    private final List<ArrowSchema> schemas = new ArrayList<>();

    public DatafusionMemtableReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
    }

    @Override
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
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
        }
    }

    @Override
    protected Throwable closeUnderLock() {
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
            failure = accumulate(failure, t);
        } finally {
            // The Arrow Java wrappers must always be closed. On the success path Rust has
            // consumed the underlying FFI structs (release callback nulled), so close is a
            // no-op for the data. On the failure-before-handoff path close releases the
            // exported data buffers back to the Java allocator.
            for (ArrowArray a : arrays) {
                try {
                    a.close();
                } catch (Throwable t) {
                    failure = accumulate(failure, t);
                }
            }
            for (ArrowSchema s : schemas) {
                try {
                    s.close();
                } catch (Throwable t) {
                    failure = accumulate(failure, t);
                }
            }
            arrays.clear();
            schemas.clear();
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
        }
        return failure;
    }
}
