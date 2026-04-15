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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.IndexFilterBridge;
import org.opensearch.index.engine.exec.IndexFilterCollectorProvider;
import org.opensearch.index.engine.exec.IndexFilterDelegate;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.arrow.c.Data.importField;

/**
 * Integration tests for Substrait-driven tree query execution.
 * <p>
 * Tests the new flow: SQL with {@code index_filter('column', 'value')} →
 * Substrait bytes via {@code sqlToSubstraitWithIndexFilter} →
 * {@code executeSubstraitTreeQueryAsync} → Rust extracts filter, builds
 * BoolNode tree, resolves collectors via JNI callbacks to
 * {@link IndexFilterBridge} → filtered Arrow result stream.
 * <p>
 * Uses the existing {@code test.parquet} (2 rows: message=2,3; message2=3,4;
 * message3=4,5) and a mock {@link IndexFilterCollectorProvider} that returns
 * pre-configured bitsets without needing a real Lucene index.
 */
public class SubstraitTreeQueryTests extends OpenSearchTestCase {

    private long runtimePtr;
    private long readerPtr;
    private Path parquetPath;

    private static boolean runtimeInitialized = false;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (runtimeInitialized == false) {
            NativeBridge.initTokioRuntimeManager(2);
            runtimeInitialized = true;
        }
        Path spillDir = createTempDir("datafusion-spill");
        runtimePtr = NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024);

        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        parquetPath = dataDir.resolve("test.parquet");
        Files.copy(testParquet, parquetPath);
        readerPtr = NativeBridge.createDatafusionReader(dataDir.toString(), new String[] { "test.parquet" });
    }

    @Override
    public void tearDown() throws Exception {
        NativeBridge.closeDatafusionReader(readerPtr);
        NativeBridge.closeGlobalRuntime(runtimePtr);
        super.tearDown();
    }

    // ── Test 1: index_filter + predicate with mock collector ────────

    /**
     * Tests a query combining {@code index_filter('message', '2')} with a
     * predicate {@code message2 > 2}. The mock collector returns a bitset
     * matching row 0 (message=2). The predicate further filters.
     * <p>
     * test.parquet has 2 rows:
     * <ul>
     *   <li>row 0: message=2, message2=3, message3=4</li>
     *   <li>row 1: message=3, message2=4, message3=5</li>
     * </ul>
     * The mock collector for "message:2" returns bitset {0} (only row 0 matches).
     * The predicate message2 > 2 is true for both rows, so the final result
     * should be row 0 only (intersection of collector and predicate).
     */
    public void testSubstraitTreeQueryWithMockCollector() throws Exception {
        // Configure mock: "message:2" → bitset with bit 0 set (row 0 matches)
        MockCollectorProvider mockProvider = new MockCollectorProvider();
        mockProvider.addBitset("message:2", bitsetForDocs(0, 2));

        DataFormat mockFormat = new MockDataFormat("mock-lucene");
        Map<String, DataFormat> columnToFormat = new HashMap<>();
        columnToFormat.put("message", mockFormat);

        Map<DataFormat, IndexFilterCollectorProvider> providers = new HashMap<>();
        providers.put(mockFormat, mockProvider);

        try (IndexFilterDelegate delegate = new IndexFilterDelegate(columnToFormat, providers)) {
            long contextId = delegate.getContextId();

            // Generate substrait with index_filter UDF
            String sql = "SELECT message, message2 FROM test_table "
                + "WHERE index_filter('message', '2') AND message2 > 2";
            byte[] substraitBytes = NativeBridge.sqlToSubstraitWithIndexFilter(
                readerPtr, "test_table", sql, runtimePtr
            );
            assertNotNull("Substrait bytes should not be null", substraitBytes);
            assertTrue("Substrait bytes should not be empty", substraitBytes.length > 0);

            // Execute substrait tree query
            String[] parquetPaths = new String[] { parquetPath.toString() };
            CompletableFuture<Long> future = new CompletableFuture<>();
            NativeBridge.executeSubstraitTreeQueryAsync(
                contextId, parquetPaths, "test_table", substraitBytes, 1, runtimePtr,
                new ActionListener<>() {
                    @Override public void onResponse(Long v) { future.complete(v); }
                    @Override public void onFailure(Exception e) { future.completeExceptionally(e); }
                }
            );

            try {
                long streamPtr = future.join();
                assertTrue("Stream pointer should be non-zero", streamPtr != 0);
                List<Object[]> rows = consumeStream(streamPtr);
                // Mock collector returns {row 0}, predicate message2 > 2 is true for row 0 (message2=3)
                // So we expect 1 row: message=2, message2=3
                assertEquals("Expected 1 row matching both index_filter and predicate", 1, rows.size());
                assertEquals(2L, rows.get(0)[0]); // message=2
                assertEquals(3L, rows.get(0)[1]); // message2=3
            } catch (CompletionException ce) {
                // If the Rust side doesn't support this yet, the test documents the expected behavior
                String msg = ce.getCause().getMessage();
                logger.warn("Substrait tree query failed (may be expected during development): {}", msg);
                // Re-throw to fail the test — this is an integration test that should pass
                throw ce;
            }
        }
    }

    // ── Test 2: predicate-only (no index_filter calls) ──────────────

    /**
     * Tests a query with only predicates and no index_filter calls.
     * {@code WHERE message > 2} should return row 1 (message=3).
     */
    public void testSubstraitTreeQueryPredicateOnly() throws Exception {
        // No mock provider needed — no index_filter calls in the query
        DataFormat mockFormat = new MockDataFormat("mock-lucene");
        Map<String, DataFormat> columnToFormat = new HashMap<>();
        Map<DataFormat, IndexFilterCollectorProvider> providers = new HashMap<>();
        providers.put(mockFormat, new MockCollectorProvider());

        try (IndexFilterDelegate delegate = new IndexFilterDelegate(columnToFormat, providers)) {
            long contextId = delegate.getContextId();

            String sql = "SELECT message, message2 FROM test_table WHERE message > 2";
            byte[] substraitBytes = NativeBridge.sqlToSubstraitWithIndexFilter(
                readerPtr, "test_table", sql, runtimePtr
            );
            assertNotNull(substraitBytes);

            String[] parquetPaths = new String[] { parquetPath.toString() };
            CompletableFuture<Long> future = new CompletableFuture<>();
            NativeBridge.executeSubstraitTreeQueryAsync(
                contextId, parquetPaths, "test_table", substraitBytes, 1, runtimePtr,
                new ActionListener<>() {
                    @Override public void onResponse(Long v) { future.complete(v); }
                    @Override public void onFailure(Exception e) { future.completeExceptionally(e); }
                }
            );

            try {
                long streamPtr = future.join();
                assertTrue("Stream pointer should be non-zero", streamPtr != 0);
                List<Object[]> rows = consumeStream(streamPtr);
                // message > 2 matches row 1 (message=3)
                assertEquals("Expected 1 row where message > 2", 1, rows.size());
                assertEquals(3L, rows.get(0)[0]); // message=3
                assertEquals(4L, rows.get(0)[1]); // message2=4
            } catch (CompletionException ce) {
                String msg = ce.getCause().getMessage();
                logger.warn("Predicate-only tree query failed: {}", msg);
                throw ce;
            }
        }
    }

    // ── Test 3: Bridge lifecycle cleanup ────────────────────────────

    /**
     * Verifies that {@link IndexFilterDelegate#close()} unregisters the
     * context from the bridge, and subsequent {@code createProvider} calls
     * for that contextId return the sentinel value -1.
     */
    public void testBridgeLifecycleCleanup() throws Exception {
        MockCollectorProvider mockProvider = new MockCollectorProvider();
        DataFormat mockFormat = new MockDataFormat("mock-lucene");

        Map<String, DataFormat> columnToFormat = new HashMap<>();
        columnToFormat.put("message", mockFormat);

        Map<DataFormat, IndexFilterCollectorProvider> providers = new HashMap<>();
        providers.put(mockFormat, mockProvider);

        long contextId;
        try (IndexFilterDelegate delegate = new IndexFilterDelegate(columnToFormat, providers)) {
            contextId = delegate.getContextId();
            assertTrue("contextId should be positive", contextId > 0);

            // While open, createProvider should succeed
            int providerKey = IndexFilterBridge.createProvider(contextId, "message", "2");
            assertTrue("createProvider should return a valid key while context is open", providerKey > 0);
        }
        // After close, createProvider should return -1
        int result = IndexFilterBridge.createProvider(contextId, "message", "2");
        assertEquals("createProvider should return -1 after context is unregistered", -1, result);
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override public void onResponse(Long v) { future.complete(v); }
            @Override public void onFailure(Exception e) { future.completeExceptionally(e); }
        });
        return future.join();
    }

    private List<Object[]> consumeStream(long streamPtr) {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()
        ) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(streamPtr, listener));
            Schema schema = new Schema(
                importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(), null
            );
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            List<Object[]> rows = new ArrayList<>();

            while (true) {
                long arrayAddr = asyncCall(listener -> NativeBridge.streamNext(runtimePtr, streamPtr, listener));
                if (arrayAddr == 0) break;
                Data.importIntoVectorSchemaRoot(allocator, ArrowArray.wrap(arrayAddr), root, dictProvider);
                int cols = root.getFieldVectors().size();
                for (int r = 0; r < root.getRowCount(); r++) {
                    Object[] row = new Object[cols];
                    for (int c = 0; c < cols; c++) {
                        row[c] = root.getFieldVectors().get(c).getObject(r);
                    }
                    rows.add(row);
                }
            }
            root.close();
            NativeBridge.streamClose(streamPtr);
            return rows;
        }
    }

    /**
     * Creates a packed long[] bitset with the specified doc IDs set.
     *
     * @param docId the doc ID to set (0-based relative to segment)
     * @param maxDoc the maximum doc count for sizing
     * @return packed long[] bitset
     */
    private static long[] bitsetForDocs(int docId, int maxDoc) {
        BitSet bs = new BitSet(maxDoc);
        bs.set(docId);
        return bs.toLongArray();
    }

    // ── Mock implementations ────────────────────────────────────────

    /**
     * A mock {@link DataFormat} for testing. Uses name-based equality.
     */
    private static class MockDataFormat extends DataFormat {
        private final String name;

        MockDataFormat(String name) {
            this.name = name;
        }

        @Override public String name() { return name; }
        @Override public long priority() { return 100L; }
        @Override public Set<FieldTypeCapabilities> supportedFields() { return Set.of(); }
    }

    /**
     * Mock {@link IndexFilterCollectorProvider} that accepts query bytes in
     * "column:value" format and returns pre-configured bitsets.
     * <p>
     * This allows testing the full Substrait-driven tree query pipeline
     * without needing a real Lucene index. The mock provider:
     * <ul>
     *   <li>Parses "column:value" from query bytes</li>
     *   <li>Looks up a pre-configured bitset for that key</li>
     *   <li>Returns the bitset on {@code collectDocs} calls</li>
     * </ul>
     */
    private static class MockCollectorProvider implements IndexFilterCollectorProvider {

        private final Map<String, long[]> configuredBitsets = new HashMap<>();
        private final ConcurrentHashMap<Integer, String> activeProviders = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, String> activeCollectors = new ConcurrentHashMap<>();
        private final AtomicInteger nextProviderKey = new AtomicInteger(1);
        private final AtomicInteger nextCollectorKey = new AtomicInteger(1);

        /**
         * Configures a bitset to return for a given "column:value" query.
         */
        void addBitset(String queryKey, long[] bitset) {
            configuredBitsets.put(queryKey, bitset);
        }

        @Override
        public int createProvider(byte[] queryBytes) throws IOException {
            String queryStr = new String(queryBytes, StandardCharsets.UTF_8);
            int key = nextProviderKey.getAndIncrement();
            activeProviders.put(key, queryStr);
            return key;
        }

        @Override
        public int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
            String queryStr = activeProviders.get(providerKey);
            if (queryStr == null) return -1;
            int key = nextCollectorKey.getAndIncrement();
            activeCollectors.put(key, queryStr);
            return key;
        }

        @Override
        public long[] collectDocs(int collectorKey, int minDoc, int maxDoc) {
            String queryStr = activeCollectors.get(collectorKey);
            if (queryStr == null) return new long[0];
            long[] bitset = configuredBitsets.get(queryStr);
            return bitset != null ? bitset : new long[0];
        }

        @Override
        public void releaseCollector(int collectorKey) {
            activeCollectors.remove(collectorKey);
        }

        @Override
        public void releaseProvider(int providerKey) {
            activeProviders.remove(providerKey);
        }

        @Override
        public void close() throws IOException {
            activeProviders.clear();
            activeCollectors.clear();
        }
    }
}
