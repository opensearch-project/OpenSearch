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
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

/**
 * Streaming coordinator-side {@link ExchangeSink}: opens a native partition stream up front,
 * pushes each fed batch through a tokio mpsc-backed sender, and on close drains the native
 * output stream into {@link ExchangeSinkContext#downstream()}.
 *
 * <p>Lifecycle invariants and {@code feed}/{@code close} skeleton are implemented in
 * {@link AbstractDatafusionReduceSink}. This subclass owns the streaming-specific resources
 * (sender, output stream) and the per-batch send / close-time drain hooks.
 */
public final class DatafusionReduceSink extends AbstractDatafusionReduceSink {

    private final DatafusionPartitionSender sender;
    private final StreamHandle outStream;

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
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
        BufferAllocator alloc = ctx.allocator();
        try (ArrowArray array = ArrowArray.allocateNew(alloc); ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc)) {
            Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
            NativeBridge.senderSend(sender.getPointer(), array.memoryAddress(), arrowSchema.memoryAddress());
        }
    }

    @Override
    protected Throwable closeUnderLock() {
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
}
