/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tests the plugin-level execution chain:
 * DatafusionReader → DatafusionContext → DatafusionSearchExecEngine → EngineResultStream → EngineResultBatch.
 * Uses sqlToSubstrait to generate plan bytes, then exercises the real plugin classes.
 */
public class DatafusionSearchExecEngineTests extends OpenSearchTestCase {

    private ReaderHandle readerHandle;
    private NativeRuntimeHandle runtimeHandle;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long ptr = NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024);
        runtimeHandle = new NativeRuntimeHandle(ptr);

        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));
        readerHandle = new ReaderHandle(dataDir.toString(), new String[] { "test.parquet" });
    }

    @Override
    public void tearDown() throws Exception {
        readerHandle.close();
        // NativeRuntimeHandle.close() calls closeGlobalRuntime
        runtimeHandle.close();
        super.tearDown();
    }

    public void testEngineExecuteSelectAll() throws Exception {
        byte[] substrait = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(),
            "test_table",
            "SELECT message, message2 FROM test_table",
            runtimeHandle.get()
        );

        // Build the plugin-level objects
        DatafusionReader reader = createReader();
        DatafusionContext context = new DatafusionContext(null, reader, runtimeHandle);
        context.setDatafusionQuery(new DatafusionQuery("test_table", substrait, 0L));

        try (
            DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(
                context,
                () -> new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE)
            )
        ) {
            try (EngineResultStream stream = engine.execute(null)) {
                List<Object[]> rows = collectRows(stream);
                assertEquals(2, rows.size());
                assertEquals(2L, rows.get(0)[0]); // message
                assertEquals(3L, rows.get(0)[1]); // message2
                assertEquals(3L, rows.get(1)[0]);
                assertEquals(4L, rows.get(1)[1]);
            }
        }
    }

    public void testEngineExecuteAggregation() throws Exception {
        byte[] substrait = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(),
            "test_table",
            "SELECT SUM(message) as total FROM test_table",
            runtimeHandle.get()
        );

        DatafusionReader reader = createReader();
        DatafusionContext context = new DatafusionContext(null, reader, runtimeHandle);
        context.setDatafusionQuery(new DatafusionQuery("test_table", substrait, 0L));

        try (
            DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(
                context,
                () -> new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE)
            )
        ) {
            try (EngineResultStream stream = engine.execute(null)) {
                List<Object[]> rows = collectRows(stream);
                assertEquals(1, rows.size());
                assertEquals(5L, rows.get(0)[0]); // 2 + 3
            }
        }
    }

    public void testEngineExecuteFilter() throws Exception {
        byte[] substrait = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(),
            "test_table",
            "SELECT message FROM test_table WHERE message = 3",
            runtimeHandle.get()
        );

        DatafusionReader reader = createReader();
        DatafusionContext context = new DatafusionContext(null, reader, runtimeHandle);
        context.setDatafusionQuery(new DatafusionQuery("test_table", substrait, 0L));

        try (
            DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(
                context,
                () -> new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE)
            )
        ) {
            try (EngineResultStream stream = engine.execute(null)) {
                List<Object[]> rows = collectRows(stream);
                assertEquals(1, rows.size());
                assertEquals(3L, rows.get(0)[0]);
            }
        }
    }

    private DatafusionReader createReader() {
        // Wrap the raw pointer in a ReaderHandle via the existing native pointer
        return new DatafusionReader(readerHandle.getPointer());
    }

    private List<Object[]> collectRows(EngineResultStream stream) {
        List<Object[]> rows = new ArrayList<>();
        Iterator<EngineResultBatch> it = stream.iterator();
        while (it.hasNext()) {
            EngineResultBatch batch = it.next();
            int cols = batch.getFieldNames().size();
            for (int r = 0; r < batch.getRowCount(); r++) {
                Object[] row = new Object[cols];
                for (int c = 0; c < cols; c++) {
                    row[c] = batch.getFieldValue(batch.getFieldNames().get(c), r);
                }
                rows.add(row);
            }
        }
        return rows;
    }
}
