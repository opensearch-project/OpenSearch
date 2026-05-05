/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

public class VSRManagerTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private Schema schema;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Settings indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettingsBuilder).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Settings settings = Settings.builder().put("node.name", "vsrmanager-test").build();
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                1,
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        bufferPool.close();
        super.tearDown();
    }

    public void testConstructionInitializesActiveVSR() throws Exception {
        String filePath = createTempDir().resolve("init.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);
        assertNotNull(manager.getActiveManagedVSR());
        assertEquals(VSRState.ACTIVE, manager.getActiveManagedVSR().getState());
        // flush handles freeze + close internally
        manager.flush();
    }

    public void testFlushWithNoDataReturnsMetadata() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);
        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(0, metadata.numRows());
    }

    public void testFlushWithData() throws Exception {
        String filePath = createTempDir().resolve("data.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);

        ManagedVSR active = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) active.getVector("val");
        vec.setSafe(0, 10);
        vec.setSafe(1, 20);
        active.setRowCount(2);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(2, metadata.numRows());
        assertNull(manager.getActiveManagedVSR());
    }

    public void testAddDocument() throws Exception {
        String filePath = createTempDir().resolve("add-doc.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(valField, 42);
        manager.addDocument(doc);

        assertEquals(1, manager.getActiveManagedVSR().getRowCount());

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    public void testSyncAfterFlush() throws Exception {
        String filePath = createTempDir().resolve("sync.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);

        ManagedVSR active = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) active.getVector("val");
        vec.setSafe(0, 10);
        active.setRowCount(1);

        manager.flush();
        manager.sync();
        assertTrue(java.nio.file.Files.exists(java.nio.file.Path.of(filePath)));
    }

    public void testMaybeRotateNoOpBelowThreshold() throws Exception {
        String filePath = createTempDir().resolve("norotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);
        ManagedVSR original = manager.getActiveManagedVSR();
        original.setRowCount(100);
        manager.maybeRotateActiveVSR();
        assertSame(original, manager.getActiveManagedVSR());
        manager.flush();
    }

    public void testMaybeRotateAtThreshold() throws Exception {
        String filePath = createTempDir().resolve("rotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);

        ManagedVSR original = manager.getActiveManagedVSR();
        original.setRowCount(50000);
        manager.maybeRotateActiveVSR();

        ManagedVSR newActive = manager.getActiveManagedVSR();
        assertNotSame(original, newActive);
        assertEquals(VSRState.ACTIVE, newActive.getState());
        manager.flush();
    }

    public void testFlushAfterRotation() throws Exception {
        String filePath = createTempDir().resolve("rotate-flush.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool);

        // Fill first VSR to trigger rotation
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec1 = (IntVector) first.getVector("val");
        for (int i = 0; i < 50000; i++) {
            vec1.setSafe(i, i);
        }
        first.setRowCount(50000);
        manager.maybeRotateActiveVSR();

        // Add data to second VSR
        ManagedVSR second = manager.getActiveManagedVSR();
        IntVector vec2 = (IntVector) second.getVector("val");
        vec2.setSafe(0, 99);
        second.setRowCount(1);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(50001, metadata.numRows());
    }

    public void testRotationAwaitsWhenFrozenSlotOccupied() throws Exception {
        String filePath = createTempDir().resolve("double-rotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool);

        // Fill first VSR to trigger rotation (async write submitted)
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec1 = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec1.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        ManagedVSR second = manager.getActiveManagedVSR();
        assertNotSame(first, second);

        // Fill second VSR — rotation returns false while frozen slot is occupied
        IntVector vec2 = (IntVector) second.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec2.setSafe(i, i + 100);
        }
        second.setRowCount(100);

        // Wait for background write to complete, then rotation should succeed
        Thread.sleep(500);
        manager.maybeRotateActiveVSR();

        ManagedVSR third = manager.getActiveManagedVSR();
        assertNotSame(second, third);
        assertEquals(VSRState.ACTIVE, third.getState());

        manager.flush();
        manager.close();
    }

    public void testRotationWritesHappenOnBackgroundThread() throws Exception {
        String filePath = createTempDir().resolve("bg-thread.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool);

        // Fill and rotate
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        // New active VSR should be immediately available for writes
        ManagedVSR second = manager.getActiveManagedVSR();
        assertNotNull(second);
        assertEquals(VSRState.ACTIVE, second.getState());
        assertEquals(0, second.getRowCount());

        // Can write to second VSR while background write may still be in progress
        IntVector vec2 = (IntVector) second.getVector("val");
        vec2.setSafe(0, 42);
        second.setRowCount(1);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(101, metadata.numRows());
    }

    public void testFlushAwaitsBackgroundWrite() throws Exception {
        String filePath = createTempDir().resolve("flush-await.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool);

        // Fill and rotate to trigger background write
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        // Add data to second VSR and flush immediately — flush must await background write
        ManagedVSR second = manager.getActiveManagedVSR();
        IntVector vec2 = (IntVector) second.getVector("val");
        vec2.setSafe(0, 999);
        second.setRowCount(1);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        // Both the rotated batch (100 rows) and the flushed batch (1 row) should be in the file
        assertEquals(101, metadata.numRows());
    }

    public void testCloseAwaitsBackgroundWrite() throws Exception {
        String filePath = createTempDir().resolve("close-await.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool);

        // Fill and rotate to trigger background write
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        // Close should not throw — it should await the background write gracefully
        manager.close();
    }
}
