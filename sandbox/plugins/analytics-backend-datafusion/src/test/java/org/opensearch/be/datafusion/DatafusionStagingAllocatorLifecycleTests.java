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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Regression tests for the staging-allocator LIFETIME contract, i.e. the bug behind
 * {@code IllegalStateException: Memory was leaked by query. Memory leaked: (1024)} thrown from
 * {@code BaseAllocator#close} via the old {@code reclaimDrainedStaging}.
 *
 * <p><b>Why this asserts the contract rather than the race.</b> The failure was a data race between the
 * producer thread (sweeping drained staging allocators) and the Flight executor thread (charging the
 * reused stream root's allocator in {@code BaseFixedWidthVector#transferTo}). The window is a couple of
 * statements wide with no injection point, so a thread-racing test would reproduce only probabilistically.
 * What IS deterministic is the invariant the transport depends on and the old code broke:
 * {@code FlightServerChannel#transferIntoStreamRoot} builds its long-lived stream root on the FIRST
 * emitted batch's vector allocator, and its comment states "The producer's allocator must be long-lived
 * (not closed per-request)". So:
 * <ul>
 *   <li>every batch of one stream must be imported on the SAME allocator, and</li>
 *   <li>that allocator must still be OPEN after later batches are imported and earlier ones released.</li>
 * </ul>
 * The old per-batch design violated both: batch N+1 got a fresh allocator, and batch N's was closed as
 * soon as its own buffers drained — while the transport was still charging it.
 */
public class DatafusionStagingAllocatorLifecycleTests extends OpenSearchTestCase {

    private static final int ROWS = 8192; // DataFusion's default batch size
    private RootAllocator root;
    // Resources the production DatafusionResultStream#close would own; the test must release them itself.
    private final List<CDataDictionaryProvider> providers = new ArrayList<>();
    private final List<BufferAllocator> producers = new ArrayList<>();
    private final List<DatafusionResultStream.BatchIterator> iterators = new ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        // Release in the order production does: staging allocator, then the dictionary provider, then the
        // stand-in native producer allocators. Only then can the root close cleanly.
        for (DatafusionResultStream.BatchIterator it : iterators) {
            it.closeStagingAllocator();
        }
        for (CDataDictionaryProvider dp : providers) {
            dp.close();
        }
        for (BufferAllocator p : producers) {
            p.close();
        }
        root.close();
        super.tearDown();
    }

    /**
     * Imports three batches through the production path and asserts all three land on the SAME staging
     * allocator, which is still open at the end.
     *
     * <p>Fails on the pre-fix code: each {@code importBatch} minted a new
     * {@code datafusion-import-staging} child, so the allocators differ between batches.
     */
    public void testAllBatchesOfAStreamShareOneOpenStagingAllocator() throws Exception {
        DatafusionResultStream.BatchIterator it = newIterator();

        BufferAllocator first = null;
        for (int i = 0; i < 3; i++) {
            VectorSchemaRoot imported = importOneBatch(it);
            BufferAllocator batchAllocator = imported.getFieldVectors().getFirst().getAllocator();
            if (first == null) {
                first = batchAllocator;
            } else {
                assertSame(
                    "every batch of a stream must import onto the SAME staging allocator — the Flight "
                        + "stream root is built on the first batch's allocator and reused for all later batches",
                    first,
                    batchAllocator
                );
            }
            // The consumer releases this batch. Under the old code this drained the allocator to zero and
            // made it eligible for the per-batch sweep on the NEXT import.
            imported.close();
        }

        assertNotNull("staging allocator must exist after importing", it.stagingAllocator());
        assertSame("the tracked staging allocator is the one batches were imported on", first, it.stagingAllocator());
        // The decisive assertion: still usable by the transport after later imports + earlier releases.
        assertEquals("staging allocator must NOT have been closed per-batch", 0L, it.stagingAllocator().getAllocatedMemory());
        it.stagingAllocator().assertOpen();
    }

    /**
     * Pins the exact sequence that threw in production: import batch 1, release it (allocator drains to
     * zero), then import batch 2 — under the old code the sweep at the head of {@code importBatch} closed
     * batch 1's allocator, which is the very allocator the Flight stream root was built on.
     */
    public void testReleasingABatchDoesNotCloseTheAllocatorTheTransportHolds() throws Exception {
        DatafusionResultStream.BatchIterator it = newIterator();

        VectorSchemaRoot batch1 = importOneBatch(it);
        BufferAllocator transportAllocator = batch1.getFieldVectors().getFirst().getAllocator();
        batch1.close(); // consumer drains it to zero
        assertEquals(
            "precondition: allocator is drained, i.e. sweep-eligible under the old design",
            0L,
            transportAllocator.getAllocatedMemory()
        );

        VectorSchemaRoot batch2 = importOneBatch(it); // old code swept batch1's allocator here
        try {
            // Simulates FlightServerChannel#transferIntoStreamRoot charging the long-lived allocator for a
            // LATER batch. On a closed allocator this throws (IllegalStateException under -ea, which tests
            // run with) — that is the production failure, reproduced deterministically.
            try (VectorSchemaRoot streamRoot = VectorSchemaRoot.create(batch2.getSchema(), transportAllocator)) {
                streamRoot.allocateNew();
                streamRoot.setRowCount(1);
            }
        } finally {
            batch2.close();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────

    private DatafusionResultStream.BatchIterator newIterator() {
        // streamHandle is never touched by importBatch, so a null handle is safe and keeps the test free of
        // the native runtime (no .so needed).
        CDataDictionaryProvider dp = new CDataDictionaryProvider();
        providers.add(dp);
        DatafusionResultStream.BatchIterator it = new DatafusionResultStream.BatchIterator(null, root, dp);
        iterators.add(it);
        return it;
    }

    /** Exports a fresh batch across the C Data Interface and imports it via the production path. */
    private VectorSchemaRoot importOneBatch(DatafusionResultStream.BatchIterator it) {
        BufferAllocator producer = root.newChildAllocator("producer", 0, Long.MAX_VALUE);
        producers.add(producer);
        try (ArrowArray array = ArrowArray.allocateNew(producer); ArrowSchema cSchema = ArrowSchema.allocateNew(producer)) {
            try (VectorSchemaRoot source = VectorSchemaRoot.create(intSchema(producer), producer)) {
                IntVector v = (IntVector) source.getVector(0);
                v.allocateNew(ROWS);
                for (int i = 0; i < ROWS; i++) {
                    v.set(i, i);
                }
                source.setRowCount(ROWS);
                Data.exportVectorSchemaRoot(producer, source, null, array, cSchema);
            }
            // Consume the exported C schema (not source.getSchema()) so its C-side release callback fires;
            // an unconsumed ArrowSchema strands its exported children in the producer allocator.
            Schema schema = Data.importSchema(root, cSchema, providers.getLast());
            return it.importBatchForTest(schema, array);
        }
    }

    private static Schema intSchema(BufferAllocator alloc) {
        try (IntVector probe = new IntVector("n", alloc)) {
            return new Schema(List.of(probe.getField()));
        }
    }
}
