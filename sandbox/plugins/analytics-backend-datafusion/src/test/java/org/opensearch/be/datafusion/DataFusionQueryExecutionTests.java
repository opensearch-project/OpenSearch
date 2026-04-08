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
import org.opensearch.test.OpenSearchTestCase;

import org.junit.AfterClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

/**
 * End-to-end test: SQL → Substrait → executeQueryAsync → Arrow C Data → Java.
 * Tests use sqlToSubstrait to generate plan bytes, then feed them through
 * the same executeQueryAsync path used in production.
 */
public class DataFusionQueryExecutionTests extends OpenSearchTestCase {

    private long runtimePtr;
    private long readerPtr;

    private static boolean runtimeInitialized = false;

    @AfterClass
    public static void cleanUpRuntime() {
        if (runtimeInitialized) {
            NativeBridge.shutdownTokioRuntimeManager();
            runtimeInitialized = false;
        }
    }

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
        Files.copy(testParquet, dataDir.resolve("test.parquet"));
        readerPtr = NativeBridge.createDatafusionReader(dataDir.toString(), new String[] { "test.parquet" });
    }

    @Override
    public void tearDown() throws Exception {
        NativeBridge.closeDatafusionReader(readerPtr);
        NativeBridge.closeGlobalRuntime(runtimePtr);
        super.tearDown();
    }

    public void testSelectAllQuery() throws Exception {
        List<Object[]> rows = executeQuery("SELECT message, message2, message3 FROM test_table");
        assertEquals(2, rows.size());
        // row 0: message=2, message2=3, message3=4
        assertEquals(2L, rows.get(0)[0]);
        assertEquals(3L, rows.get(0)[1]);
        assertEquals(4L, rows.get(0)[2]);
        // row 1: message=3, message2=4, message3=5
        assertEquals(3L, rows.get(1)[0]);
    }

    public void testFilterQuery() throws Exception {
        List<Object[]> rows = executeQuery("SELECT message FROM test_table WHERE message > 2");
        assertEquals(1, rows.size());
        assertEquals(3L, rows.get(0)[0]);
    }

    public void testAggregationQuery() throws Exception {
        List<Object[]> rows = executeQuery("SELECT SUM(message) as total, COUNT(*) as cnt FROM test_table");
        assertEquals(1, rows.size());
        assertEquals(5L, rows.get(0)[0]); // 2 + 3
        assertEquals(2L, rows.get(0)[1]);
    }

    /**
     * Converts SQL → substrait bytes, then executes via the real executeQueryAsync path,
     * and collects all result rows.
     */
    private List<Object[]> executeQuery(String sql) {
        // Step 1: SQL → Substrait (test helper)
        byte[] substraitBytes = NativeBridge.sqlToSubstrait(readerPtr, "test_table", sql, runtimePtr);
        assertNotNull(substraitBytes);
        assertTrue(substraitBytes.length > 0);

        // Step 2: executeQueryAsync (production path)
        long streamPtr = asyncCall(
            listener -> NativeBridge.executeQueryAsync(readerPtr, "test_table", substraitBytes, runtimePtr, listener)
        );
        assertTrue(streamPtr != 0);

        // Step 3: Read results via Arrow C Data
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()
        ) {

            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(streamPtr, listener));
            Schema schema = new Schema(importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(), null);
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

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                future.complete(v);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future.join();
    }
}
