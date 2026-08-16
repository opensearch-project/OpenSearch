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
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Regression tests for the native batch leak on Arrow C Data Interface import under allocator pressure.
 *
 * <p>Self-contained (no native runtime): a batch is built on a "producer" allocator and exported across
 * the C Data Interface. The producer allocator stands in for the native (Rust) allocator that owns the
 * exported buffers — when import releases the C Data array the producer drains to zero; when it leaks the
 * producer stays non-zero.
 *
 * <p>{@link #testDirectImportIntoBoundedAllocatorLeaksExportedBatch} pins the arrow-java bug: importing
 * directly into a bounded allocator throws {@link OutOfMemoryException} mid-array and strands the whole
 * exported batch (producer not drained). {@link #testProductionStagingImportReleasesBatch} drives the
 * production path ({@link DatafusionResultStream.BatchIterator#importOntoStaging}) and asserts the batch is
 * released even when the caller's allocator is far too small — this test fails on the unfixed direct-import
 * code and passes with the staging import.
 */
public class DatafusionImportLeakTests extends OpenSearchTestCase {

    private static final int ROWS = 4096;
    private static final int VALUE_BYTES = 512;
    private static final int COLUMNS = 4;
    private static final long TINY_LIMIT = 64 * 1024; // far smaller than the ~8 MB batch

    private RootAllocator root;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        root.close();
        super.tearDown();
    }

    /**
     * Production path: {@link DatafusionResultStream.BatchIterator#importOntoStaging} imports onto an
     * unbounded staging child of the root, so a caller allocator far too small to hold the batch does not
     * cause a mid-import OOM; the batch imports, and closing it releases the exported buffers — the producer
     * allocator drains to zero.
     *
     * <p>On the unfixed code (import directly into the bounded caller allocator) this leaks and the assertion
     * fails, so this is a genuine regression guard.
     */
    public void testProductionStagingImportReleasesBatch() throws Exception {
        BufferAllocator producer = root.newChildAllocator("producer", 0, Long.MAX_VALUE);
        try (ArrowArray array = ArrowArray.allocateNew(producer); ArrowSchema cSchema = ArrowSchema.allocateNew(producer)) {
            exportBatch(producer, array, cSchema);
            assertTrue("producer holds the exported batch before import", producer.getAllocatedMemory() > 0);

            // Stand-in for the caller allocator: intentionally far smaller than the batch.
            BufferAllocator tinyTarget = root.newChildAllocator("tiny-target", 0, TINY_LIMIT);
            BufferAllocator staging = tinyTarget.getRoot().newChildAllocator("datafusion-import-staging", 0, Long.MAX_VALUE);
            try (CDataDictionaryProvider dp = new CDataDictionaryProvider()) {
                Schema schema = Data.importSchema(staging, cSchema, dp);
                VectorSchemaRoot imported = DatafusionResultStream.BatchIterator.importOntoStaging(staging, schema, array, dp);
                imported.close();                 // consumer releases the batch → C Data release fires
            }
            staging.close();
            tinyTarget.close();
        }
        assertEquals("staging import must release the exported batch (producer drained)", 0L, producer.getAllocatedMemory());
        producer.close();
    }

    /**
     * Pins the arrow-java bug this fix works around: importing the exported batch <em>directly</em> into a
     * bounded allocator (the pre-fix behaviour) throws {@link OutOfMemoryException} mid-array <b>and</b>
     * leaks — the producer allocator is not drained because the C Data release callback never fires.
     */
    public void testDirectImportIntoBoundedAllocatorLeaksExportedBatch() throws Exception {
        // Dedicated local root, deliberately abandoned: this test demonstrates the arrow-java leak, so the
        // producer/target/C-Data structs cannot be drained or closed. Keeping it off the shared #root (and
        // never closing anything here) avoids tripping the allocator leak detector on this expected leak.
        RootAllocator localRoot = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator producer = localRoot.newChildAllocator("producer-direct", 0, Long.MAX_VALUE);
        ArrowArray array = ArrowArray.allocateNew(producer);
        ArrowSchema cSchema = ArrowSchema.allocateNew(producer);
        exportBatch(producer, array, cSchema);

        BufferAllocator tinyTarget = localRoot.newChildAllocator("tiny-target-direct", 0, TINY_LIMIT);
        CDataDictionaryProvider dp = new CDataDictionaryProvider();
        Schema schema = Data.importSchema(tinyTarget, cSchema, dp);
        VectorSchemaRoot target = VectorSchemaRoot.create(schema, tinyTarget);
        // importIntoVectorSchemaRoot wraps the allocator OOM as IllegalArgumentException
        // ("Could not load buffers for field ...") with the OutOfMemoryException as its cause.
        Exception thrown = expectThrows(Exception.class, () -> Data.importIntoVectorSchemaRoot(tinyTarget, array, target, dp));
        assertTrue("mid-import failure must be an allocator OOM, was: " + thrown, hasOomCause(thrown));
        assertTrue("arrow-java strands the exported batch on a mid-import OOM (producer not drained)", producer.getAllocatedMemory() > 0);
        // Nothing is closed: the mid-import OOM strands buffers on producer and target and never fires the
        // C-Data release, so no close would succeed. localRoot is abandoned; the JVM reclaims the wrappers.
    }

    /** True if {@code t} is, or is caused by, an Arrow {@link OutOfMemoryException}. */
    private static boolean hasOomCause(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof OutOfMemoryException) {
                return true;
            }
        }
        return false;
    }

    /** Builds a multi-column VarChar batch on {@code alloc} and exports it into {@code array}/{@code cSchema}. */
    private void exportBatch(BufferAllocator alloc, ArrowArray array, ArrowSchema cSchema) {
        VectorSchemaRoot source = buildBatch(alloc);
        try {
            long total = 0;
            for (FieldVector v : source.getFieldVectors()) {
                total += v.getBufferSize();
            }
            assertTrue("test setup: batch must exceed the tiny target limit", total > TINY_LIMIT);
            Data.exportVectorSchemaRoot(alloc, source, null, array, cSchema);
        } finally {
            source.close();
        }
    }

    private VectorSchemaRoot buildBatch(BufferAllocator alloc) {
        byte[] value = new byte[VALUE_BYTES];
        Arrays.fill(value, (byte) 'x');
        List<FieldVector> fieldVectors = new ArrayList<>(COLUMNS);
        for (int c = 0; c < COLUMNS; c++) {
            VarCharVector v = new VarCharVector("col" + c, alloc);
            v.allocateNew((long) ROWS * VALUE_BYTES, ROWS);
            for (int r = 0; r < ROWS; r++) {
                v.setSafe(r, value);
            }
            v.setValueCount(ROWS);
            fieldVectors.add(v);
        }
        return new VectorSchemaRoot(fieldVectors);
    }
}
