/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinator-side {@link ExchangeSink} that buffers build-side Arrow batches from a broadcast
 * build stage and, on {@link #close()}, serializes the whole buffer to a single Arrow IPC
 * stream byte array. The resulting bytes are exposed via {@link #ipcBytesFuture()} for the
 * broadcast-dispatch orchestrator to embed in a
 * {@code BroadcastInjectionInstructionNode} attached to each probe-stage
 * {@code FragmentExecutionRequest}.
 *
 * <p>Unlike {@link DatafusionMemtableReduceSink}, this sink does not talk to DataFusion's FFM at
 * all — it has no session, no registered memtable, no native handle. It is purely a
 * Java-side collector producing wire bytes. The memtable registration happens on each probe
 * data node when {@code BroadcastInjectionHandler} decodes the payload.
 *
 * <p>Threading: {@link #feed} and {@link #close} may be called from multiple shard-handler
 * threads concurrently; both are serialized on a single lock. Close-once semantics — a
 * second {@code close()} is a no-op.
 */
public final class BroadcastCaptureSink implements ExchangeSink {

    private static final Logger LOGGER = LogManager.getLogger(BroadcastCaptureSink.class);

    /** Default runtime byte cap when no explicit limit is set. Long.MAX_VALUE = effectively no cap. */
    private static final long DEFAULT_MAX_BYTES = Long.MAX_VALUE;

    private final BufferAllocator alloc;
    private final Schema fallbackSchema;
    private final long maxBytes;
    private final Object lock = new Object();
    private final List<VectorSchemaRoot> batches = new ArrayList<>();
    private final CompletableFuture<byte[]> ipcBytesFuture = new CompletableFuture<>();
    private volatile Schema schema;
    private long accumulatedBytes;
    private boolean closed;
    private boolean exceeded;

    public BroadcastCaptureSink(BufferAllocator alloc) {
        this(alloc, null, DEFAULT_MAX_BYTES);
    }

    public BroadcastCaptureSink(BufferAllocator alloc, Schema fallbackSchema) {
        this(alloc, fallbackSchema, DEFAULT_MAX_BYTES);
    }

    /**
     * Creates a capture sink with an explicit fallback schema and a runtime byte cap. If the
     * accumulated buffer-size of fed batches exceeds {@code maxBytes}, the sink fails its
     * future with {@link BroadcastSizeExceededException} on close — the dispatcher routes
     * that to the terminal listener as a query failure, preventing a runaway broadcast from
     * blowing up coordinator memory.
     *
     * <p>The fallback schema is used when the build stage emits no batches at all (or only
     * zero-column phantoms produced by {@code RowResponseCodec} for empty IPC payloads).
     * Without it, an all-empty build payload caused the memtable to register with
     * {@code Schema(List.of())} and break the probe Substrait plan.
     *
     * @param maxBytes runtime cap on accumulated build-side buffer size, in bytes; pass
     *                 {@code Long.MAX_VALUE} to disable.
     */
    public BroadcastCaptureSink(BufferAllocator alloc, Schema fallbackSchema, long maxBytes) {
        this.alloc = alloc;
        this.fallbackSchema = fallbackSchema;
        this.maxBytes = maxBytes;
    }

    /** Future completed with the Arrow IPC byte buffer when {@link #close()} finishes. */
    public CompletableFuture<byte[]> ipcBytesFuture() {
        return ipcBytesFuture;
    }

    /**
     * Appends a batch to the in-memory buffer. The sink takes ownership — callers must not
     * close the root afterwards; {@link #close()} closes every retained batch.
     *
     * <p>Schema capture policy: the real build-side schema is adopted from the first batch
     * that has at least one column. Empty/header-only batches (produced by the response codec
     * for a null shard payload) keep {@code schema} null until a real batch arrives; if the
     * whole build side is empty, {@link #close()} falls back to a zero-column schema.
     *
     * <p>If a later non-empty batch disagrees with an already-captured empty-schema snapshot,
     * we upgrade the snapshot to the real schema. Arrow IPC writer requires a single schema
     * for the stream, and the real schema is the one the probe-side join needs.
     */
    @Override
    public void feed(VectorSchemaRoot batch) {
        long batchBytes = bufferSize(batch);
        synchronized (lock) {
            if (closed) {
                batch.close();
                throw new IllegalStateException("BroadcastCaptureSink: feed() after close()");
            }
            // Runtime cap check: if accumulating this batch would exceed the limit, mark the
            // sink as exceeded and drop the batch. Subsequent batches are dropped too. The
            // failure is surfaced from close() so the dispatcher can route it through the
            // normal terminal-listener path.
            if (exceeded || accumulatedBytes + batchBytes > maxBytes) {
                exceeded = true;
                accumulatedBytes += batchBytes; // track the would-be total for the error message
                batch.close();
                return;
            }
            accumulatedBytes += batchBytes;
            Schema batchSchema = batch.getSchema();
            boolean batchHasColumns = !batchSchema.getFields().isEmpty();
            if (batchHasColumns && (schema == null || schema.getFields().isEmpty())) {
                schema = batchSchema;
            } else if (schema == null) {
                // First batch was a zero-column root and we don't have a real schema yet;
                // remember the empty schema so close() can still produce a header-only stream
                // if no real batch ever arrives.
                schema = batchSchema;
            }
            batches.add(batch);
        }
    }

    /** Sums per-vector buffer sizes for the accumulator; cheap, off the lock-protected path. */
    private static long bufferSize(VectorSchemaRoot batch) {
        long total = 0L;
        for (org.apache.arrow.vector.FieldVector v : batch.getFieldVectors()) {
            total += v.getBufferSize();
        }
        return total;
    }

    @Override
    public void close() {
        List<VectorSchemaRoot> toSerialize;
        Schema schemaSnapshot;
        boolean wasExceeded;
        long observedBytes;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            toSerialize = new ArrayList<>(batches);
            batches.clear();
            schemaSnapshot = schema;
            wasExceeded = exceeded;
            observedBytes = accumulatedBytes;
        }

        if (wasExceeded) {
            BroadcastSizeExceededException ex = new BroadcastSizeExceededException(observedBytes, maxBytes);
            ipcBytesFuture.completeExceptionally(ex);
            LOGGER.warn("[BroadcastCaptureSink] {}", ex.getMessage());
            // Still close any retained batches in the finally below.
            for (VectorSchemaRoot root : toSerialize) {
                try {
                    root.close();
                } catch (Throwable ignore) {
                    // best-effort cleanup
                }
            }
            return;
        }

        try {
            byte[] bytes = serializeToIpc(schemaSnapshot, toSerialize);
            ipcBytesFuture.complete(bytes);
            LOGGER.debug("[BroadcastCaptureSink] captured {} batches → {} bytes", toSerialize.size(), bytes.length);
        } catch (Throwable t) {
            ipcBytesFuture.completeExceptionally(t);
            LOGGER.warn("[BroadcastCaptureSink] IPC serialization failed", t);
        } finally {
            for (VectorSchemaRoot root : toSerialize) {
                try {
                    root.close();
                } catch (Throwable ignore) {
                    // best-effort cleanup — an earlier exception is already being surfaced via the future
                }
            }
        }
    }

    /**
     * Serializes a list of batches to an Arrow IPC stream. Returns an empty-schema header-only
     * stream if no batches were fed (preserves downstream's ability to decode and register an
     * empty memtable rather than failing on the probe side).
     */
    private byte[] serializeToIpc(Schema schemaSnapshot, List<VectorSchemaRoot> toSerialize) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Schema selection priority:
        // 1. A real schema captured from a fed batch (preferred).
        // 2. The fallback schema supplied at construction (build-side row type).
        // 3. An empty schema as a last-resort header so the IPC stream is decodable.
        // The fallback exists because RowResponseCodec turns a null shard payload into a
        // zero-column root, and the data-node serializer omits the schema header when it
        // emits no batches at all. Without a fallback, an all-empty build side would
        // register a zero-column memtable on the probe side and break the join's NamedScan.
        if (schemaSnapshot == null || schemaSnapshot.getFields().isEmpty()) {
            if (fallbackSchema != null && !fallbackSchema.getFields().isEmpty()) {
                schemaSnapshot = fallbackSchema;
            } else if (schemaSnapshot == null) {
                schemaSnapshot = new Schema(List.of());
            }
        }
        int headerColumnCount = schemaSnapshot.getFields().size();
        try (
            VectorSchemaRoot headerRoot = VectorSchemaRoot.create(schemaSnapshot, alloc);
            ArrowStreamWriter writer = new ArrowStreamWriter(headerRoot, null, Channels.newChannel(out))
        ) {
            writer.start();
            // P2a — if the captured schema is empty (every fed batch was a zero-column root
            // from a null shard payload), emit header-only. On the probe side the broadcast
            // memtable has no columns so the join over it produces zero rows for INNER and
            // preserves the probe side for OUTER — both correct for "empty build side".
            if (headerColumnCount > 0) {
                for (VectorSchemaRoot batch : toSerialize) {
                    // Skip batches whose column count doesn't match the header. RowResponseCodec
                    // produces a zero-column root for a null payload; the real build-side schema
                    // is the one adopted by feed()'s schema-upgrade rule. Transferring a mismatched
                    // batch into the header root would walk off the field list.
                    if (batch.getFieldVectors().size() != headerColumnCount) {
                        continue;
                    }
                    // Transfer vectors into the header root so the writer emits them with the
                    // original schema's field ordering, then clear so the next batch's transfer is clean.
                    for (int i = 0; i < batch.getFieldVectors().size(); i++) {
                        batch.getFieldVectors().get(i).makeTransferPair(headerRoot.getFieldVectors().get(i)).transfer();
                    }
                    headerRoot.setRowCount(batch.getRowCount());
                    writer.writeBatch();
                    headerRoot.clear();
                }
            }
            writer.end();
        }
        return out.toByteArray();
    }
}
