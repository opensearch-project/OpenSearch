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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShuffleBufferAccess;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * Handler for {@link ShuffleScanInstructionNode} on a hash-shuffle worker.
 *
 * <p>Bridges the node-local {@link org.opensearch.analytics.exec.shuffle.ShuffleBufferManager}
 * to a DataFusion {@code StreamingTable} via {@link
 * NativeBridge#registerPartitionStreamOnSessionContext} — the worker's Substrait plan
 * references the resulting {@code NamedScan} so the hash-join's input resolves to the
 * partitioned stream.
 *
 * <p>The handler runs synchronously on the data-node executor thread that processes the
 * fragment's instruction list. {@link ShuffleBufferAccess#awaitReady} blocks here until both
 * left and right producers have all reported {@code isLast} for this partition, then the
 * handler drains the buffer's accumulated IPC chunks for the {@code "left"} or {@code "right"}
 * side (the named-input id encodes which) and pushes each batch into the native sender.
 *
 * <p>Chain-ordering requirement: same as {@link BroadcastInjectionHandler} — must run AFTER
 * {@link ShardScanInstructionHandler} so the {@code SessionContextHandle} exists. The
 * dispatcher appends two of these per partition (left, right) after the shard-scan setup.
 *
 * <p>Schema discovery: the partition's IPC chunks each include their own schema header (per
 * the Arrow IPC stream spec). The handler reads the first chunk's header to derive the schema
 * for the streaming-table registration. If the buffer's accumulated bytes are empty (no
 * producer rows for this partition) the handler treats the partition as a zero-batch stream —
 * the registered table is still resolvable so the worker plan binds, and the join produces
 * zero rows from this partition.
 *
 * <p>Failure surface: any IO exception during decode, alignment, or FFM registration is
 * surfaced as a {@link RuntimeException} that propagates back through
 * {@code AnalyticsSearchService}'s instruction-handler loop and fails the fragment.
 *
 * @opensearch.internal
 */
public class ShuffleScanHandler implements FragmentInstructionHandler<ShuffleScanInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleScanHandler.class);

    /** Cap on how long the consumer waits for both producer sides to mark {@code isLast}.
     *  Real producers complete within milliseconds on a healthy cluster — the cap exists
     *  solely as a backstop against stuck producers (cancelled queries cascade through the
     *  walker faster than this). Operator-tuneable via {@code analytics.mpp.shuffle.recv_timeout}
     *  once that cluster setting is plumbed into {@link ShardScanExecutionContext}; today the
     *  handler reads the JVM system property of the same name as a stopgap so integration
     *  tests can dial it down without waiting on full SPI plumbing.
     *
     *  <p>5s default keeps test timelines tight while leaving headroom for slow CI hosts.
     *  Real shuffle producers are far faster — a single batch RTT over local transport is
     *  microseconds. */
    private static final long DEFAULT_AWAIT_READY_TIMEOUT_MS = Long.parseLong(
        System.getProperty("analytics.mpp.shuffle.recv_timeout_ms", "5000")
    );

    @Override
    public BackendExecutionContext apply(
        ShuffleScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        if (!(backendContext instanceof DataFusionSessionState sessionState)) {
            throw new IllegalStateException(
                "ShuffleScanHandler: expected DataFusionSessionState from a prior handler "
                    + "(typically ShardScanInstructionHandler), got "
                    + (backendContext == null ? "null" : backendContext.getClass().getSimpleName())
                    + ". The shuffle-scan instruction must be appended after the scan-setup instruction "
                    + "in the worker stage's plan alternative."
            );
        }
        if (!(commonContext instanceof ShardScanExecutionContext shardCtx)) {
            throw new IllegalStateException(
                "ShuffleScanHandler: expected ShardScanExecutionContext, got "
                    + (commonContext == null ? "null" : commonContext.getClass().getSimpleName())
            );
        }
        ShuffleBufferRegistry registry = shardCtx.getShuffleBufferRegistry();
        if (registry == null) {
            throw new IllegalStateException(
                "ShuffleScanHandler: ShuffleBufferRegistry not plumbed into ShardScanExecutionContext. "
                    + "AnalyticsSearchService.setShuffleBufferRegistry must be called at plugin startup."
            );
        }
        // The instruction carries side ("left"/"right") explicitly. namedInputId is the canonical
        // "input-<producerStageId>" the fragment convertor emits for the StageInputScan leaf below
        // the stripped OpenSearchShuffleExchange — that's what the worker's Substrait plan binds
        // its NamedScan against.
        String inputId = node.getNamedInputId();
        String side = node.getSide();
        boolean isLeftSide = "left".equals(side);
        if (!isLeftSide && !"right".equals(side)) {
            throw new IllegalStateException(
                "ShuffleScanHandler: side must be 'left' or 'right', got '" + side + "' (namedInputId=" + inputId + ")"
            );
        }

        ShuffleBufferAccess buffer = registry.getOrCreate(node.getQueryId(), node.getTargetStageId(), node.getShufflePartitionIndex());
        // expectedSenders for both sides are set eagerly by the ShuffleWorkerSetupHandler
        // (which runs before any ShuffleScanHandler) so the buffer knows BOTH sides' counts
        // before either side's awaitReady call blocks. Setting them per-side here would
        // deadlock — the second side's count is set only AFTER the first side's blocked.

        try {
            LOGGER.debug(
                "ShuffleScanHandler: awaiting partition stream queryId={}, stage={}, partition={}, side={}, expectedSenders={}",
                node.getQueryId(),
                node.getTargetStageId(),
                node.getShufflePartitionIndex(),
                side,
                node.getExpectedSenders()
            );
            // Block here until BOTH sides' producers have all reported isLast for this partition.
            // The buffer's awaitReady gates on left-and-right because the handler runs once for
            // each side; both invocations agree the buffer is fully populated before either
            // returns. The current handler still drains only its own side, but we must wait for
            // both because the producer-side dispatch fires concurrently and the IPC bytes for
            // the not-yet-arrived side are racing in the same buffer.
            if (!buffer.awaitReady(DEFAULT_AWAIT_READY_TIMEOUT_MS)) {
                throw new RuntimeException(
                    "ShuffleScanHandler: timed out waiting for shuffle producers to finish for "
                        + inputId
                        + " (timeout="
                        + DEFAULT_AWAIT_READY_TIMEOUT_MS
                        + "ms)"
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("ShuffleScanHandler: interrupted while awaiting shuffle producers for " + inputId, e);
        }

        List<byte[]> sideData = isLeftSide ? buffer.getLeftData() : buffer.getRightData();

        BufferAllocator alloc = shardCtx.getAllocator();
        // Empty partition: register an empty memtable and return — no streaming drain needed.
        if (sideData.isEmpty()) {
            LOGGER.debug(
                "ShuffleScanHandler: empty partition for {} (queryId={}, stage={}, part={}); registering empty memtable",
                inputId,
                node.getQueryId(),
                node.getTargetStageId(),
                node.getShufflePartitionIndex()
            );
            NativeBridge.registerMemtableOnSessionContext(
                sessionState.sessionContextHandle().getPointer(),
                inputId,
                new byte[0],
                new long[0],
                new long[0]
            );
            return backendContext;
        }

        // Schema from the first chunk's IPC header — needed at registration time.
        byte[] schemaIpc;
        try {
            schemaIpc = extractSchemaIpc(sideData.get(0), alloc);
        } catch (Exception e) {
            throw new RuntimeException("ShuffleScanHandler: failed to extract schema for " + inputId, e);
        }
        long senderPtr = NativeBridge.registerPartitionStreamOnSessionContext(
            sessionState.sessionContextHandle().getPointer(),
            inputId,
            schemaIpc
        );
        DatafusionPartitionSender sender = new DatafusionPartitionSender(senderPtr);

        // Drain chunks into the native sender on a background thread. The native partition
        // stream is a bounded mpsc (capacity 4): synchronous draining inside this handler
        // would block on the 5th send because the consumer (engine.execute → HashJoinExec)
        // doesn't run until ALL handlers finish. Running the drain off-thread lets
        // engine.execute start in parallel, drain the channel, and unblock the sender.
        //
        // The background thread owns the sender's lifecycle: it closes the sender after the
        // last chunk (signals EOF to the native StreamingTable). Failures stamp the closure
        // and close the sender so the partition still terminates rather than hanging the join.
        final List<byte[]> chunks = sideData;
        final DatafusionPartitionSender finalSender = sender;
        Thread drainThread = new Thread(() -> {
            int totalBatches = 0;
            try {
                for (byte[] chunk : chunks) {
                    totalBatches += pumpChunkIntoSender(chunk, alloc, finalSender);
                }
                LOGGER.debug(
                    "ShuffleScanHandler.drain: drained {} batches across {} chunks for {} (side={}, partition={})",
                    totalBatches,
                    chunks.size(),
                    inputId,
                    side,
                    node.getShufflePartitionIndex()
                );
            } catch (Throwable t) {
                LOGGER.warn("ShuffleScanHandler.drain failed for " + inputId, t);
            } finally {
                try {
                    finalSender.close();
                } catch (Throwable closeErr) {
                    LOGGER.warn("ShuffleScanHandler.drain: sender.close failed for " + inputId, closeErr);
                }
            }
        }, "shuffle-drain-" + node.getQueryId() + "-" + node.getTargetStageId() + "-" + side + "-" + node.getShufflePartitionIndex());
        drainThread.setDaemon(true);
        drainThread.start();

        return backendContext;
    }

    /**
     * Reads the schema header from an Arrow IPC stream chunk and returns an isolated
     * IPC-stream blob containing only that schema. The returned bytes are the same shape
     * {@link ArrowSchemaIpc#toBytes} produces — what
     * {@link NativeBridge#registerPartitionStreamOnSessionContext} expects.
     */
    private static byte[] extractSchemaIpc(byte[] chunkIpc, BufferAllocator alloc) throws Exception {
        try (ByteArrayInputStream in = new ByteArrayInputStream(chunkIpc); ArrowStreamReader reader = new ArrowStreamReader(in, alloc)) {
            return ArrowSchemaIpc.toBytes(reader.getVectorSchemaRoot().getSchema());
        }
    }

    /**
     * Decodes every batch in {@code chunkIpc} and pushes each into the native sender via Arrow
     * C Data Interface. Returns the batch count for logging.
     */
    private static int pumpChunkIntoSender(byte[] chunkIpc, BufferAllocator alloc, DatafusionPartitionSender sender) throws Exception {
        int count = 0;
        try (ByteArrayInputStream in = new ByteArrayInputStream(chunkIpc); ArrowStreamReader reader = new ArrowStreamReader(in, alloc)) {
            VectorSchemaRoot rootView = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                // Each loadNextBatch mutates rootView in-place. Export immediately; the native
                // sender takes ownership of the FFI structs on success.
                ArrowArray array = ArrowArray.allocateNew(alloc);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
                boolean handedOff = false;
                try {
                    Data.exportVectorSchemaRoot(alloc, rootView, null, array, arrowSchema);
                    sender.send(array.memoryAddress(), arrowSchema.memoryAddress());
                    handedOff = true;
                    count++;
                } finally {
                    // On success Rust released the underlying FFI structs (the release callback
                    // is nulled); on failure we own them and must close. Java-side close is safe
                    // either way — close on a hand-off-d wrapper is a no-op.
                    if (!handedOff) {
                        try {
                            array.close();
                        } catch (Throwable ignore) {
                            // best-effort — primary error is being surfaced
                        }
                        try {
                            arrowSchema.close();
                        } catch (Throwable ignore) {
                            // best-effort — primary error is being surfaced
                        }
                    } else {
                        // Always close the Java wrappers' tracking state. Native already owns
                        // the underlying memory.
                        try {
                            array.close();
                        } catch (Throwable ignore) {
                            // best-effort
                        }
                        try {
                            arrowSchema.close();
                        } catch (Throwable ignore) {
                            // best-effort
                        }
                    }
                }
            }
        }
        return count;
    }
}
