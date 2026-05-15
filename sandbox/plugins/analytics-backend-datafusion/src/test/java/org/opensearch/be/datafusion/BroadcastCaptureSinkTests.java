/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link BroadcastCaptureSink}. No native deps — pure Arrow IPC round-trip.
 */
public class BroadcastCaptureSinkTests extends OpenSearchTestCase {

    private static Schema longSchema() {
        return new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    }

    private static VectorSchemaRoot makeBatch(RootAllocator alloc, Schema schema, long[] vals) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        BigIntVector v = (BigIntVector) root.getVector("x");
        v.allocateNew(vals.length);
        for (int i = 0; i < vals.length; i++) {
            v.setSafe(i, vals[i]);
        }
        v.setValueCount(vals.length);
        root.setRowCount(vals.length);
        return root;
    }

    public void testFeedThenCloseProducesIpcBytes() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc);
            sink.feed(makeBatch(alloc, longSchema(), new long[] { 1L, 2L, 3L }));
            sink.feed(makeBatch(alloc, longSchema(), new long[] { 4L, 5L }));
            sink.close();

            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(bytes);
            assertTrue("IPC bytes must be non-empty", bytes.length > 0);

            // Roundtrip — decode the IPC stream and confirm we get two batches with the expected values.
            List<Long> observed = new ArrayList<>();
            try (
                RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), readerAlloc)
            ) {
                int batchIdx = 0;
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    BigIntVector v = (BigIntVector) root.getVector("x");
                    for (int i = 0; i < v.getValueCount(); i++) {
                        observed.add(v.get(i));
                    }
                    batchIdx++;
                }
                assertEquals("expected 2 roundtripped batches", 2, batchIdx);
            }
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), observed);
        }
    }

    public void testCloseWithoutFeedProducesHeaderOnlyStream() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc);
            sink.close();
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(bytes);
            assertTrue("empty-capture stream should still have IPC header", bytes.length > 0);

            try (
                RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), readerAlloc)
            ) {
                assertFalse("empty-capture stream must have zero batches", reader.loadNextBatch());
            }
        }
    }

    public void testFeedAfterCloseThrows() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc);
            sink.close();
            expectThrows(IllegalStateException.class, () -> sink.feed(makeBatch(alloc, longSchema(), new long[] { 1L })));
        }
    }

    /**
     * Regression: when the first fed batch is a zero-column {@link VectorSchemaRoot}
     * (produced for a null shard payload), the sink must NOT snap the empty schema as the
     * broadcast schema. A later non-empty batch from another shard upgrades the snapshot to
     * the real schema, and the empty batch is skipped during serialization.
     */
    public void testEmptyFirstBatchDoesNotSnapEmptySchema() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc);
            // Simulate a null-payload shard response: zero-column root from a fresh Schema.
            VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(new Schema(List.of()), alloc);
            emptyRoot.setRowCount(0);
            sink.feed(emptyRoot);
            // Second shard has real data.
            sink.feed(makeBatch(alloc, longSchema(), new long[] { 7L, 8L }));
            sink.close();
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);

            // Roundtrip must see the real schema and the real batch.
            try (
                RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), readerAlloc)
            ) {
                assertEquals(
                    "roundtripped schema must be the long schema, not empty",
                    longSchema().getFields(),
                    reader.getVectorSchemaRoot().getSchema().getFields()
                );
                assertTrue("first batch loads", reader.loadNextBatch());
                BigIntVector v = (BigIntVector) reader.getVectorSchemaRoot().getVector("x");
                assertEquals(2, v.getValueCount());
                assertEquals(7L, v.get(0));
                assertEquals(8L, v.get(1));
                assertFalse("only one non-empty batch", reader.loadNextBatch());
            }
        }
    }

    /**
     * Regression: if every fed batch is empty-schema (the whole build side had null
     * payloads), the sink produces a header-only stream with an empty schema. The broadcast
     * memtable registered from this payload joins zero rows — correct semantics for INNER.
     */
    public void testAllEmptyBatchesYieldsHeaderOnlyStream() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc);
            VectorSchemaRoot empty1 = VectorSchemaRoot.create(new Schema(List.of()), alloc);
            empty1.setRowCount(0);
            sink.feed(empty1);
            VectorSchemaRoot empty2 = VectorSchemaRoot.create(new Schema(List.of()), alloc);
            empty2.setRowCount(0);
            sink.feed(empty2);
            sink.close();
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            try (
                RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), readerAlloc)
            ) {
                assertFalse("all-empty feed must produce zero batches", reader.loadNextBatch());
            }
        }
    }

    /**
     * Regression: when the build stage emits no real batches at all, the sink
     * must use the explicit fallback schema in the IPC header — not {@code Schema(List.of())}.
     * The probe-side handler then registers a memtable with the right column types and the
     * join's NamedScan binds correctly (yielding zero matches for INNER joins).
     */
    public void testFallbackSchemaUsedWhenNoRealBatches() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc, longSchema());
            sink.close(); // no batches at all
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            try (
                RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), readerAlloc)
            ) {
                assertEquals(
                    "fallback schema must propagate to the IPC header",
                    longSchema().getFields(),
                    reader.getVectorSchemaRoot().getSchema().getFields()
                );
                assertFalse(reader.loadNextBatch());
            }
        }
    }

    /**
     * Regression: even when every fed batch is a zero-column phantom from a null
     * shard payload, the fallback schema wins. The IPC header carries the real build schema
     * and zero record batches are emitted — same correct behavior as the all-empty case.
     */
    public void testFallbackSchemaUsedWhenAllPhantomBatches() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc, longSchema());
            VectorSchemaRoot phantom = VectorSchemaRoot.create(new Schema(List.of()), alloc);
            phantom.setRowCount(0);
            sink.feed(phantom);
            sink.close();
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            try (
                RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), readerAlloc)
            ) {
                assertEquals(
                    "fallback schema must override the phantom-batch's empty schema",
                    longSchema().getFields(),
                    reader.getVectorSchemaRoot().getSchema().getFields()
                );
                assertFalse(reader.loadNextBatch());
            }
        }
    }

    /**
     * Runtime byte cap: when accumulated buffer size exceeds the limit, close() fails the
     * future with {@link BroadcastSizeExceededException} so the dispatcher can route the
     * failure through the terminal listener.
     */
    public void testByteCapFailsFutureWhenExceeded() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // Tiny cap: a single 3-row long batch (~24 bytes data + buffer overhead) easily exceeds it.
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc, longSchema(), /* maxBytes */ 8L);
            sink.feed(makeBatch(alloc, longSchema(), new long[] { 1L, 2L, 3L }));
            sink.close();

            try {
                sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
                fail("expected BroadcastSizeExceededException");
            } catch (java.util.concurrent.ExecutionException ee) {
                assertTrue(
                    "cause must be BroadcastSizeExceededException, was: " + ee.getCause(),
                    ee.getCause() instanceof BroadcastSizeExceededException
                );
            }
        }
    }

    /**
     * Cap not exceeded: a tiny batch under the limit succeeds normally.
     */
    public void testByteCapPassesWhenUnderLimit() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // Generous cap.
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc, longSchema(), /* maxBytes */ 1024L * 1024L);
            sink.feed(makeBatch(alloc, longSchema(), new long[] { 1L, 2L, 3L }));
            sink.close();
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            assertTrue("under-cap broadcast must produce IPC bytes", bytes.length > 0);
        }
    }

    public void testCloseIsIdempotent() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            BroadcastCaptureSink sink = new BroadcastCaptureSink(alloc);
            sink.feed(makeBatch(alloc, longSchema(), new long[] { 42L }));
            sink.close();
            sink.close(); // no-op, must not throw
            byte[] bytes = sink.ipcBytesFuture().get(5, TimeUnit.SECONDS);
            assertTrue(bytes.length > 0);
        }
    }
}
