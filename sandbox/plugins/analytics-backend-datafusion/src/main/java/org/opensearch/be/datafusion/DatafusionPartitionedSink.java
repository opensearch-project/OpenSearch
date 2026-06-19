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
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ShuffleSender;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.core.action.ActionListener;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DataFusion's hash-partitioned {@link ExchangeSink}. Sits at the tail of a producer fragment's
 * output: for every batch the engine emits, hash-partition by {@code hashKeyChannels} into
 * {@code partitionCount} buckets via DataFusion's native {@code BatchPartitioner} (so the
 * partition assignment matches what {@code HashJoinExec} would compute on the consumer side),
 * serialize each non-empty partition to Arrow IPC bytes, and ship the bytes to
 * {@code targetWorkerNodeIds[partitionIndex]} via the framework-provided {@link ShuffleSender}.
 *
 * <p><b>Why native partitioning?</b> Hash-shuffle joins require producers and the consumer's
 * {@code HashJoinExec} to agree on which partition a row lands in. DataFusion's
 * {@code RepartitionExec(Hash)} uses {@code REPARTITION_RANDOM_STATE} as its hasher seed; calling
 * the same {@code BatchPartitioner} from the producer guarantees consistency without
 * reverse-engineering the seed in JVM code.
 *
 * <p><b>Lifecycle.</b> {@code feed} is called once per engine batch; {@code close} is called once
 * after the engine stream drains. On close, every target receives an {@code isLast=true} send
 * (empty payload) so the consumer's per-side {@code awaitReady} latch releases. Send completions
 * are tracked through one shared {@link AtomicInteger} pending counter — the framework's calling
 * code can poll completion by waiting until the counter returns to zero, but the sink itself
 * does not block on send acknowledgement (sends are fire-and-forget with retry handled inside
 * the {@code ShuffleSender}).
 *
 * <p><b>Threading.</b> {@code feed} is callable from any thread; the sink itself is not
 * re-entrant per partition (one call's exports use the same allocator). Producer engines emit
 * batches sequentially per stream so concurrent feeds aren't expected, but the partitioning +
 * IPC serialization happen outside any lock so multiple feeds would interleave their FFM calls,
 * which is safe.
 *
 * <p><b>Failure surface.</b> A native partitioner error or IPC serialization error stamps a
 * sticky {@link #firstError}; subsequent feeds and {@code close} skip work but still send
 * {@code isLast} so the consumer doesn't hang. The first error is rethrown from {@code close}.
 */
public final class DatafusionPartitionedSink implements ExchangeSink {

    private static final Logger LOGGER = LogManager.getLogger(DatafusionPartitionedSink.class);

    private final BufferAllocator alloc;
    private final int[] hashKeyIndices;
    private final int partitionCount;
    private final List<String> targetWorkerNodeIds;
    private final ShuffleSender sender;
    private final String logTag;

    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();
    private volatile boolean closed;
    private long batchesFed;

    /**
     * @param alloc                buffer allocator for Arrow C-data exports / IPC serialization
     * @param hashKeyChannels      0-indexed input channels to hash on
     * @param partitionCount       number of consumer partitions
     * @param targetWorkerNodeIds  one entry per partition; index {@code p} = destination for
     *                             partition {@code p}; {@code size() == partitionCount}
     * @param sender               framework-provided wire wrapper, already stamped with the
     *                             {@code (queryId, targetStageId, side)} this sink is shipping
     *                             into; the sink calls {@link ShuffleSender#send} per partition
     * @param logTag               opaque identifier used in log messages so producer streams from
     *                             different stages / sides can be distinguished. Typical value:
     *                             {@code "queryId/stage/side"}
     */
    public DatafusionPartitionedSink(
        BufferAllocator alloc,
        List<Integer> hashKeyChannels,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        ShuffleSender sender,
        String logTag
    ) {
        if (targetWorkerNodeIds.size() != partitionCount) {
            throw new IllegalArgumentException(
                "targetWorkerNodeIds.size() (" + targetWorkerNodeIds.size() + ") must equal partitionCount (" + partitionCount + ")"
            );
        }
        this.alloc = alloc;
        this.hashKeyIndices = hashKeyChannels.stream().mapToInt(Integer::intValue).toArray();
        this.partitionCount = partitionCount;
        this.targetWorkerNodeIds = List.copyOf(targetWorkerNodeIds);
        this.sender = sender;
        this.logTag = logTag;
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        if (closed) {
            batch.close();
            throw new IllegalStateException("DatafusionPartitionedSink: feed() after close()");
        }
        if (firstError.get() != null) {
            // A previous feed already failed; drop further work but keep the pipeline draining
            // so close() can finalize and surface the first error.
            batch.close();
            return;
        }
        if (batch.getRowCount() == 0) {
            batch.close();
            return;
        }

        ArrowArray inputArray = ArrowArray.allocateNew(alloc);
        ArrowSchema inputSchema = ArrowSchema.allocateNew(alloc);
        long[] partitionedFfi = null;
        try {
            Data.exportVectorSchemaRoot(alloc, batch, null, inputArray, inputSchema);
            partitionedFfi = NativeBridge.partitionBatchByHash(
                inputArray.memoryAddress(),
                inputSchema.memoryAddress(),
                hashKeyIndices,
                partitionCount
            );
        } catch (Throwable t) {
            firstError.compareAndSet(null, t);
            LOGGER.warn("DatafusionPartitionedSink: partition failed for " + logTag, t);
            // Best-effort cleanup of the input wrappers.
            try {
                inputArray.close();
            } catch (Throwable ignore) {}
            try {
                inputSchema.close();
            } catch (Throwable ignore) {}
            try {
                batch.close();
            } catch (Throwable ignore) {}
            return;
        }

        // df_partition_batch_by_hash re-exported the input FFI structs in place with a Rust-side
        // release callback (see api.rs partition_batch_by_hash). Fire release() before close() so
        // the Rust-side callback decrements its Arc refs to the imported ArrayData; then closing
        // the Java wrappers frees their 128-byte container buffers. Without release(), the Rust
        // ArrayData (which transitively holds the original Java-side ExportedArrayPrivateData
        // alive via the Arc'd FFI_ArrowArray) never drops, and the original batch's buffers leak.
        try {
            inputArray.release();
        } catch (Throwable ignore) {}
        try {
            inputArray.close();
        } catch (Throwable ignore) {}
        try {
            inputSchema.release();
        } catch (Throwable ignore) {}
        try {
            inputSchema.close();
        } catch (Throwable ignore) {}
        // exportVectorSchemaRoot moved the batch's buffers into the export; closing the source
        // root only releases its tracking shells. release() above is what frees the buffers.
        try {
            batch.close();
        } catch (Throwable ignore) {}

        // For each partition: import the (array, schema) pair back into a Java VSR, serialize to
        // IPC bytes, and ship.
        for (int p = 0; p < partitionCount; p++) {
            long arrayPtr = partitionedFfi[2 * p];
            long schemaPtr = partitionedFfi[2 * p + 1];
            if (arrayPtr == 0 || schemaPtr == 0) {
                // Empty partition for this batch — partitioner returns null FFI structs when no
                // rows hashed there. Skip serialization; isLast at close() will release the latch.
                continue;
            }
            try {
                byte[] ipcBytes = importAndSerializeToIpc(arrayPtr, schemaPtr);
                if (ipcBytes.length == 0) {
                    continue;
                }
                shipPayload(p, ipcBytes, /* isLast */ false);
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
                LOGGER.warn("DatafusionPartitionedSink: failed to ship partition " + p + " for " + logTag, t);
            }
        }

        batchesFed++;
    }

    /**
     * Imports an FFI (array, schema) pair into a Java {@link VectorSchemaRoot} and serializes it
     * to a single Arrow IPC stream containing one batch. Closes the C wrappers and the imported
     * root before returning. The returned bytes own no native memory.
     */
    private byte[] importAndSerializeToIpc(long arrayPtr, long schemaPtr) throws java.io.IOException {
        try (
            ArrowArray arrowArray = ArrowArray.wrap(arrayPtr);
            ArrowSchema arrowSchema = ArrowSchema.wrap(schemaPtr);
            CDataDictionaryProvider dictionaryProvider = new CDataDictionaryProvider();
            VectorSchemaRoot root = Data.importVectorSchemaRoot(alloc, arrowArray, arrowSchema, dictionaryProvider);
        ) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, dictionaryProvider, Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    /**
     * Hands one partition's payload to the framework {@link ShuffleSender}. Increments the
     * {@link #pending} counter before send and decrements on completion (success or failure) so
     * the producer fragment's caller can wait on quiescence.
     */
    private void shipPayload(int partitionIndex, byte[] data, boolean isLast) {
        String targetNodeId = targetWorkerNodeIds.get(partitionIndex);
        pending.incrementAndGet();
        sender.send(targetNodeId, partitionIndex, data, isLast, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                pending.decrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                firstError.compareAndSet(null, e);
                pending.decrementAndGet();
                LOGGER.warn(
                    "DatafusionPartitionedSink: send failed ("
                        + logTag
                        + ", partition="
                        + partitionIndex
                        + ", target="
                        + targetNodeId
                        + ")",
                    e
                );
            }
        });
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // PHASE 1: drain all in-flight DATA sends to zero BEFORE shipping any isLast. This is
        // load-bearing for correctness: a data send for partition p (issued in feed()) may still be
        // backpressure-retrying inside ShuffleSender when close() runs. If we shipped isLast(p) now,
        // the consumer's awaitReady could fire (once every producer's isLast arrives) and the
        // consumer could begin draining BEFORE the retried data send lands — the late chunk would be
        // admitted after the drain snapshot and silently dropped (codex round-5 BLOCKER #2). Ordering
        // all data sends ahead of every isLast guarantees that when a consumer sees this producer's
        // isLast, every row this producer will ever ship has already been buffered.
        awaitPendingDrain("data sends");

        // PHASE 2: send isLast=true to every target so the consumer's per-side awaitReady latch fires.
        // Even partitions we never produced rows for need an isLast — the consumer counts senders
        // by partition, not by row count.
        for (int p = 0; p < partitionCount; p++) {
            try {
                shipPayload(p, new byte[0], /* isLast */ true);
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
                LOGGER.warn("DatafusionPartitionedSink: failed to ship final marker for partition " + p + " (" + logTag + ")", t);
            }
        }
        // Wait for the isLast sends to drain too, so the sink is fully quiesced on return.
        awaitPendingDrain("isLast markers");
        Throwable err = firstError.get();
        LOGGER.debug(
            "DatafusionPartitionedSink: closed ({}, batches={}, partitions={}, error={})",
            logTag,
            batchesFed,
            partitionCount,
            err == null ? "none" : err.getMessage()
        );
        if (err != null) {
            if (err instanceof RuntimeException re) throw re;
            if (err instanceof Error e) throw e;
            throw new RuntimeException(err);
        }
    }

    /**
     * Spins until the {@link #pending} send counter returns to zero (or a 60s deadline lapses).
     * {@code ShuffleSender} handles backpressure retry, so this is bounded by the per-send retry cap ×
     * partitionCount + drain latency. Spinning is acceptable: close() happens once per producer
     * fragment (post stream-drain) on a worker thread already off the transport hot path. {@code phase}
     * names the wave being drained for the timeout log.
     */
    private void awaitPendingDrain(String phase) {
        long deadlineMs = System.currentTimeMillis() + 60_000;
        while (pending.get() > 0 && System.currentTimeMillis() < deadlineMs) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                firstError.compareAndSet(null, e);
                return;
            }
        }
        if (pending.get() > 0) {
            LOGGER.warn("DatafusionPartitionedSink: timed out draining {} for {} ({} sends still pending)", phase, logTag, pending.get());
        }
    }
}
