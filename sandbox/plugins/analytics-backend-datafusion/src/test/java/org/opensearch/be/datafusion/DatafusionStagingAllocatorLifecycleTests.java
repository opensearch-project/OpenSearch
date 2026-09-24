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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Regression tests for the staging-allocator OWNERSHIP contract: the allocator batches are imported onto is
 * supplied by the caller ({@code AnalyticsSearchService} in production, via
 * {@code ShardScanExecutionContext#getImportStagingAllocator}), node-scoped, and never created or closed by
 * the stream. Two separate bugs are pinned here, both rooted in a stream owning its staging allocator:
 *
 * <ul>
 *   <li><b>Use-after-close.</b> {@code FlightServerChannel#transferIntoStreamRoot} builds its long-lived
 *       stream root on the FIRST emitted batch's vector allocator — its comment states "The producer's
 *       allocator must be long-lived (not closed per-request)" — and charges that same allocator for every
 *       later batch. A per-batch staging allocator closed as soon as its own buffers drained was therefore
 *       closed while the transport was still using it, throwing {@code IllegalStateException: Memory was
 *       leaked by query} from {@code BaseAllocator#close}. The window is a couple of statements wide with no
 *       injection point, so what these tests assert is the deterministic invariant behind it: every batch of a
 *       stream imports onto the SAME allocator, which is still OPEN afterwards.</li>
 *   <li><b>Stranded child allocators.</b> Keeping a per-stream allocator open instead (to dodge the race)
 *       leaks it until node reboot: {@code BaseAllocator#newChildAllocator} registers every child in the
 *       parent's {@code childAllocators} map unconditionally, and only the child's own {@code close()}
 *       deregisters it — so an un-closed staging child stays strongly referenced by the node-lifetime root,
 *       one per query. {@link #testStreamsDoNotMintOrStrandChildAllocators} pins that the stream mints no
 *       child allocator at all.</li>
 * </ul>
 */
public class DatafusionStagingAllocatorLifecycleTests extends OpenSearchTestCase {

    private static final int ROWS = 8192; // DataFusion's default batch size
    private RootAllocator root;
    /** Stands in for the node-scoped allocator AnalyticsSearchService owns and hands to every stream. */
    private BufferAllocator staging;
    /** Stands in for the native (Rust) allocator owning the exported buffers; long-lived like the real one. */
    private BufferAllocator producer;
    // Resources the production DatafusionResultStream#close would own; the test must release them itself.
    private final List<CDataDictionaryProvider> providers = new ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = new RootAllocator(Long.MAX_VALUE);
        staging = root.newChildAllocator("arrow-import-staging", 0, Long.MAX_VALUE);
        producer = root.newChildAllocator("producer", 0, Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        // No stream closes the staging allocator — the caller does, exactly as AnalyticsSearchService#close
        // does in production.
        for (CDataDictionaryProvider dp : providers) {
            dp.close();
        }
        staging.close();
        producer.close();
        root.close();
        super.tearDown();
    }

    /**
     * Imports three batches through the production path and asserts all three land on the SAME staging
     * allocator — the caller-supplied one — which is still open at the end.
     */
    public void testAllBatchesOfAStreamShareOneOpenStagingAllocator() throws Exception {
        DatafusionResultStream.BatchIterator it = newIterator();

        for (int i = 0; i < 3; i++) {
            VectorSchemaRoot imported = importOneBatch(it);
            assertSame(
                "every batch of a stream must import onto the caller-supplied staging allocator — the Flight "
                    + "stream root is built on the first batch's allocator and reused for all later batches",
                staging,
                imported.getFieldVectors().getFirst().getAllocator()
            );
            // The consumer releases this batch. Under the old per-batch code this drained the allocator to
            // zero and made it eligible for the sweep on the NEXT import.
            imported.close();
        }

        assertSame("the iterator must not substitute an allocator of its own", staging, it.stagingAllocator());
        // The decisive assertion: still usable by the transport after later imports + earlier releases.
        assertEquals("staging allocator must NOT have been closed per-batch", 0L, staging.getAllocatedMemory());
        staging.assertOpen();
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

    /**
     * The leak regression: streaming N results must not add a single child allocator to the root. A stream
     * that mints its own staging child either has to close it (racing the transport — see the tests above) or
     * leave it open, in which case the root's {@code childAllocators} map retains it for the node's lifetime.
     * Neither is acceptable, so the stream mints nothing.
     */
    public void testStreamsDoNotMintOrStrandChildAllocators() throws Exception {
        Set<BufferAllocator> before = new HashSet<>(root.getChildAllocators());

        for (int stream = 0; stream < 3; stream++) {
            DatafusionResultStream.BatchIterator it = newIterator();
            VectorSchemaRoot imported = importOneBatch(it);
            imported.close();      // consumer releases the batch
            it.closeLastBatch();   // what DatafusionResultStream#close does to the iterator
        }

        assertEquals(
            "importing on a caller-supplied staging allocator must add no child allocator to the root — an "
                + "un-closed child stays registered in the parent's childAllocators map until node reboot",
            before,
            new HashSet<>(root.getChildAllocators())
        );
        staging.assertOpen();
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────

    private DatafusionResultStream.BatchIterator newIterator() {
        // streamHandle is never touched by importBatch, so a null handle is safe and keeps the test free of
        // the native runtime (no .so needed).
        CDataDictionaryProvider dp = new CDataDictionaryProvider();
        providers.add(dp);
        return new DatafusionResultStream.BatchIterator(null, root, staging, dp);
    }

    /** Exports a fresh batch across the C Data Interface and imports it via the production path. */
    private VectorSchemaRoot importOneBatch(DatafusionResultStream.BatchIterator it) {
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
