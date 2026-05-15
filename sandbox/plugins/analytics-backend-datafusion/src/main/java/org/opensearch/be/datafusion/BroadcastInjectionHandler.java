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
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Handler for {@link BroadcastInjectionInstructionNode} on a probe-side data node.
 *
 * <p>Receives coordinator-collected build-side Arrow IPC batches and registers them as a
 * {@code MemTable} on the probe's DataFusion session under the instruction's
 * {@link BroadcastInjectionInstructionNode#getNamedInputId() namedInputId}. The probe-side
 * Substrait plan references this named input via a {@code NamedScan}, so the join's build
 * side resolves to the in-memory broadcast payload rather than a stage-input partition
 * stream.
 *
 * <p>Chain-ordering requirement: this handler must run AFTER {@link ShardScanInstructionHandler}
 * so that {@link DataFusionSessionState} (carrying the native {@code SessionContextHandle}) has
 * already been produced. The broadcast-dispatch orchestrator (in analytics-engine) is
 * responsible for appending {@code BroadcastInjectionInstructionNode} to the probe stage's
 * existing instruction list, not prepending.
 *
 * <p>Memtable registration mirrors the happy path of
 * {@link DatafusionMemtableReduceSink#closeUnderLock}: export each
 * {@link VectorSchemaRoot} via Arrow C Data Interface, accumulate the FFI struct pointers in
 * parallel arrays, hand them across in a single {@code NativeBridge.registerMemtable} call.
 * Native takes ownership on success; the Java wrappers are closed in the {@code finally} block
 * so failure paths release buffers back to the allocator before surfacing the error.
 *
 * <p>Returns {@code backendContext} unchanged — the session handle survives for the downstream
 * {@link DatafusionSearchExecEngine} to execute the probe's join plan against both the
 * shard's data and the newly registered memtable.
 */
public class BroadcastInjectionHandler implements FragmentInstructionHandler<BroadcastInjectionInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(BroadcastInjectionHandler.class);

    @Override
    public BackendExecutionContext apply(
        BroadcastInjectionInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        if (!(backendContext instanceof DataFusionSessionState sessionState)) {
            throw new IllegalStateException(
                "BroadcastInjectionHandler: expected DataFusionSessionState from a prior handler "
                    + "(e.g. ShardScanInstructionHandler), got "
                    + (backendContext == null ? "null" : backendContext.getClass().getSimpleName())
                    + ". The broadcast-injection instruction must be appended after the scan-setup instruction "
                    + "in the probe stage's plan alternative."
            );
        }
        byte[] ipcBytes = node.getBroadcastData();
        if (ipcBytes == null || ipcBytes.length == 0) {
            // Empty broadcast payload — coordinator observed zero build rows. Registering an
            // empty memtable preserves probe-side plan structure (NamedScan resolves cleanly)
            // and yields the correct zero-join-output for INNER and outer-with-empty-build.
            LOGGER.debug("[BroadcastInjectionHandler] empty broadcast payload for {}", node.getNamedInputId());
            // Still register a header-only memtable so the NamedScan resolves — defer to the
            // reader-driven path below (the ArrowStreamReader handles a zero-batch stream).
        }

        BufferAllocator alloc = ((ShardScanExecutionContext) commonContext).getAllocator();
        List<ArrowArray> arrays = new ArrayList<>();
        List<ArrowSchema> schemas = new ArrayList<>();
        byte[] schemaIpc;

        try (
            ByteArrayInputStream in = new ByteArrayInputStream(ipcBytes == null ? new byte[0] : ipcBytes);
            ArrowStreamReader reader = new ArrowStreamReader(in, alloc)
        ) {
            // Capture the schema IPC bytes once — registerMemtable wants a standalone IPC
            // schema blob (the same shape produced by ArrowSchemaIpc.toBytes).
            VectorSchemaRoot rootView = reader.getVectorSchemaRoot();
            schemaIpc = ArrowSchemaIpc.toBytes(rootView.getSchema());

            while (reader.loadNextBatch()) {
                // Each loadNextBatch() mutates rootView in-place. Export immediately; the export
                // transfers the current batch's vectors into the fresh (array, schema) pair.
                ArrowArray array = ArrowArray.allocateNew(alloc);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
                try {
                    Data.exportVectorSchemaRoot(alloc, rootView, null, array, arrowSchema);
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
        } catch (Exception e) {
            releaseWrappers(arrays, schemas);
            throw new RuntimeException("BroadcastInjectionHandler: failed to decode Arrow IPC payload for " + node.getNamedInputId(), e);
        }

        try {
            long[] arrayPtrs = new long[arrays.size()];
            long[] schemaPtrs = new long[schemas.size()];
            for (int i = 0; i < arrays.size(); i++) {
                arrayPtrs[i] = arrays.get(i).memoryAddress();
                schemaPtrs[i] = schemas.get(i).memoryAddress();
            }
            // Use the SessionContext variant — the probe-side scan-setup handler produced a
            // SessionContextHandle (shard-scan path), not a LocalSession (coord-reduce path).
            // Registering against a LocalSession via a wrong-type pointer cast is a memory-safety
            // bug that hangs or crashes the JVM; the SessionContext variant is type-correct.
            NativeBridge.registerMemtableOnSessionContext(
                sessionState.sessionContextHandle().getPointer(),
                node.getNamedInputId(),
                schemaIpc,
                arrayPtrs,
                schemaPtrs
            );
            LOGGER.debug(
                "[BroadcastInjectionHandler] registered memtable {} ({} batches, buildSideIndex={})",
                node.getNamedInputId(),
                arrays.size(),
                node.getBuildSideIndex()
            );
        } catch (Throwable t) {
            // Native registration failed — Rust has not taken ownership. Release wrappers now.
            releaseWrappers(arrays, schemas);
            throw t;
        } finally {
            // On success, Rust has consumed the underlying FFI structs (release callback nulled),
            // so Java-side close is a no-op on the data. Always call close so the Java wrappers
            // themselves don't leak their own tracking state.
            releaseWrappers(arrays, schemas);
        }

        return backendContext;
    }

    private static void releaseWrappers(List<ArrowArray> arrays, List<ArrowSchema> schemas) {
        for (ArrowArray a : arrays) {
            try {
                a.close();
            } catch (Throwable ignore) {
                // Best-effort cleanup — primary error (if any) is already being surfaced.
            }
        }
        for (ArrowSchema s : schemas) {
            try {
                s.close();
            } catch (Throwable ignore) {
                // Best-effort cleanup.
            }
        }
    }
}
