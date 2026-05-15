/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for DatafusionResultStream — resource lifecycle, leak detection, edge cases.
 */
public class DatafusionResultStreamTests extends OpenSearchTestCase {

    private ReaderHandle readerHandle;
    private NativeRuntimeHandle runtimeHandle;
    private RootAllocator testRootAllocator;
    private Arena configArena;
    private long queryConfigPtr;
    private long tieredStorePtr;
    private NativeStoreHandle storeHandle;
    private final java.util.List<BufferAllocator> allocatorsToClose = new java.util.ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("spill");
        long ptr = NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024);
        runtimeHandle = new NativeRuntimeHandle(ptr);
        testRootAllocator = new RootAllocator(Long.MAX_VALUE);

        // Create a real TieredObjectStore (local-only) and wrap in NativeStoreHandle
        tieredStorePtr = NativeStoreTestHelper.createTieredObjectStore(0L, 0L);
        long boxPtr = NativeStoreTestHelper.getObjectStoreBoxPtr(tieredStorePtr);
        storeHandle = new NativeStoreHandle(boxPtr, NativeStoreTestHelper::destroyObjectStoreBoxPtr);

        Path dataDir = createTempDir("data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));
        readerHandle = new ReaderHandle(dataDir.toString(), new String[] { "test.parquet" }, storeHandle);

        configArena = Arena.ofConfined();
        MemorySegment configSegment = configArena.allocate(WireConfigSnapshot.BYTE_SIZE);
        WireConfigSnapshot.builder().build().writeTo(configSegment);
        queryConfigPtr = configSegment.address();
    }

    @Override
    public void tearDown() throws Exception {
        configArena.close();
        readerHandle.close();
        storeHandle.close();
        NativeStoreTestHelper.destroyTieredObjectStore(tieredStorePtr);
        runtimeHandle.close();
        NativeBridge.shutdownTokioRuntimeManager();
        // Caller owns child allocators now (see DatafusionResultStream.close javadoc).
        // Close them in reverse registration order so child-before-parent invariants hold.
        for (int i = allocatorsToClose.size() - 1; i >= 0; i--) {
            allocatorsToClose.get(i).close();
        }
        testRootAllocator.close();
        super.tearDown();
    }

    public void testCloseWithoutIterating() throws Exception {
        // Stream created but never iterated — close must not leak
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table")) {
            assertNotNull(stream);
            // deliberately don't call iterator()
        }
    }

    public void testCloseAfterPartialIteration() throws Exception {
        // Iterate one batch then close — remaining native resources must be freed
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table")) {
            Iterator<EngineResultBatch> it = stream.iterator();
            assertTrue(it.hasNext());
            EngineResultBatch batch = it.next();
            try {
                assertTrue(batch.getRowCount() > 0);
            } finally {
                batch.getArrowRoot().close();
            }
            // close without exhausting the stream
        }
    }

    public void testCloseAfterFullIteration() throws Exception {
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table")) {
            Iterator<EngineResultBatch> it = stream.iterator();
            int totalRows = 0;
            while (it.hasNext()) {
                EngineResultBatch batch = it.next();
                try {
                    totalRows += batch.getRowCount();
                } finally {
                    batch.getArrowRoot().close();
                }
            }
            assertEquals(2, totalRows);
        }
    }

    public void testNextWithoutHasNextWorks() throws Exception {
        // Calling next() directly should work (it calls hasNext internally)
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table")) {
            Iterator<EngineResultBatch> it = stream.iterator();
            EngineResultBatch batch = it.next();
            try {
                assertTrue(batch.getRowCount() > 0);
            } finally {
                batch.getArrowRoot().close();
            }
        }
    }

    public void testEmptyResultYieldsOneZeroRowBatchWithSchema() throws Exception {
        // Streaming Flight requires ≥1 schema-bearing frame before completeStream; empty
        // native streams synthesise a zero-row batch carrying the schema.
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table WHERE message > 999")) {
            Iterator<EngineResultBatch> it = stream.iterator();
            assertTrue("empty stream must yield exactly one zero-row schema batch", it.hasNext());
            EngineResultBatch batch = it.next();
            try {
                assertEquals(0, batch.getRowCount());
                assertEquals(java.util.List.of("message"), batch.getFieldNames());
            } finally {
                batch.getArrowRoot().close();
            }
            assertFalse("after consuming the schema batch the stream is empty", it.hasNext());
            expectThrows(NoSuchElementException.class, it::next);
        }
    }

    public void testHasNextIsIdempotent() throws Exception {
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table")) {
            Iterator<EngineResultBatch> it = stream.iterator();
            // Multiple hasNext calls should not advance the stream
            assertTrue(it.hasNext());
            assertTrue(it.hasNext());
            assertTrue(it.hasNext());
            EngineResultBatch batch = it.next();
            try {
                assertTrue(batch.getRowCount() > 0);
            } finally {
                batch.getArrowRoot().close();
            }
        }
    }

    public void testIteratorReturnsSameInstance() throws Exception {
        try (DatafusionResultStream stream = createStream("SELECT message FROM test_table")) {
            Iterator<EngineResultBatch> it1 = stream.iterator();
            Iterator<EngineResultBatch> it2 = stream.iterator();
            assertSame(it1, it2);
        }
    }

    public void testBatchFieldAccess() throws Exception {
        try (DatafusionResultStream stream = createStream("SELECT message, message2 FROM test_table")) {
            Iterator<EngineResultBatch> it = stream.iterator();
            assertTrue(it.hasNext());
            EngineResultBatch batch = it.next();
            try {
                assertEquals(2, batch.getFieldNames().size());
                assertTrue(batch.getFieldNames().contains("message"));
                assertTrue(batch.getFieldNames().contains("message2"));
                assertNotNull(batch.getFieldValue("message", 0));
                expectThrows(IllegalArgumentException.class, () -> batch.getFieldValue("nonexistent", 0));
            } finally {
                batch.getArrowRoot().close();
            }
        }
    }

    public void testNativeQueryFailureDoesNotLeak() {
        // Invalid substrait bytes should cause native failure — verify error propagates and no leak
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryAsync(
            readerHandle.getPointer(),
            "test_table",
            new byte[] { 0, 1, 2 },
            runtimeHandle.get(),
            0L,
            queryConfigPtr,
            new ActionListener<>() {
                @Override
                public void onResponse(Long ptr) {
                    future.complete(ptr);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        Exception ex = expectThrows(Exception.class, future::join);
        assertNotNull("Native error should propagate", ex.getCause());
        assertTrue(
            "Error should mention substrait/decode failure, got: " + ex.getCause().getMessage(),
            ex.getCause().getMessage().toLowerCase(Locale.ROOT).contains("substrait")
                || ex.getCause().getMessage().toLowerCase(Locale.ROOT).contains("decode")
                || ex.getCause().getMessage().toLowerCase(Locale.ROOT).contains("failed")
        );
    }

    public void testCloseAfterNativeStreamNextFailure() throws Exception {
        // Create a valid stream, close the runtime handle to force streamNext failure,
        // then verify the stream still closes cleanly
        Path spillDir2 = createTempDir("spill2");
        long ptr2 = NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir2.toString(), 64 * 1024 * 1024);
        NativeRuntimeHandle tempRuntime = new NativeRuntimeHandle(ptr2);

        byte[] substrait = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(),
            "test_table",
            "SELECT message FROM test_table",
            runtimeHandle.get()
        );
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryAsync(
            readerHandle.getPointer(),
            "test_table",
            substrait,
            tempRuntime.get(),
            0L,
            queryConfigPtr,
            new ActionListener<>() {
                @Override
                public void onResponse(Long p) {
                    future.complete(p);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        long streamPtr = future.join();

        BufferAllocator failureAlloc = testRootAllocator.newChildAllocator("test-failure", 0, Long.MAX_VALUE);
        allocatorsToClose.add(failureAlloc);
        DatafusionResultStream stream = new DatafusionResultStream(
            new org.opensearch.be.datafusion.nativelib.StreamHandle(streamPtr, tempRuntime),
            failureAlloc
        );

        // Close runtime — streamNext should now fail with IllegalStateException from NativeRuntimeHandle.get()
        tempRuntime.close();

        Iterator<EngineResultBatch> it = stream.iterator();
        Exception ex = expectThrows(Exception.class, it::hasNext);
        // Should fail because runtime handle is closed — get() throws IllegalStateException
        assertTrue(
            "Expected IllegalStateException from closed runtime handle, got: " + ex.getMessage(),
            ex.getCause() instanceof IllegalStateException || ex instanceof IllegalStateException
        );

        // Stream close should still work even after the failure
        stream.close();
    }

    public void testDoubleCloseIsHarmless() throws Exception {
        DatafusionResultStream stream = createStream("SELECT message FROM test_table");
        stream.close();
        // Second close should not throw (NativeHandle uses AtomicBoolean)
        stream.close();
    }

    private DatafusionResultStream createStream(String sql) {
        byte[] substrait = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", sql, runtimeHandle.get());
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryAsync(
            readerHandle.getPointer(),
            "test_table",
            substrait,
            runtimeHandle.get(),
            0L,
            queryConfigPtr,
            new ActionListener<>() {
                @Override
                public void onResponse(Long ptr) {
                    future.complete(ptr);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        long streamPtr = future.join();
        BufferAllocator childAllocator = testRootAllocator.newChildAllocator("test-stream", 0, Long.MAX_VALUE);
        allocatorsToClose.add(childAllocator);
        return new DatafusionResultStream(
            new org.opensearch.be.datafusion.nativelib.StreamHandle(streamPtr, runtimeHandle),
            childAllocator
        );
    }
}
