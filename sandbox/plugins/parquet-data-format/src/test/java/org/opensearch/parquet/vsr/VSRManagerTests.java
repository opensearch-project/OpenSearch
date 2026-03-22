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
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class VSRManagerTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    @Override
    public void tearDown() throws Exception {
        bufferPool.close();
        super.tearDown();
    }

    public void testConstructionInitializesActiveVSR() throws Exception {
        String filePath = createTempDir().resolve("init.parquet").toString();
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);
        assertNotNull(manager.getActiveManagedVSR());
        assertEquals(VSRState.ACTIVE, manager.getActiveManagedVSR().getState());
        // flush handles freeze + close internally
        manager.flush();
    }

    public void testFlushWithNoDataReturnsNull() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);
        assertNull(manager.flush());
    }

    public void testFlushWithData() throws Exception {
        String filePath = createTempDir().resolve("data.parquet").toString();
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);

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
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);

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
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);

        ManagedVSR active = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) active.getVector("val");
        vec.setSafe(0, 10);
        active.setRowCount(1);

        manager.flush();
        manager.sync();
        assertTrue(new java.io.File(filePath).exists());
    }

    public void testMaybeRotateNoOpBelowThreshold() throws Exception {
        String filePath = createTempDir().resolve("norotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);
        ManagedVSR original = manager.getActiveManagedVSR();
        original.setRowCount(100);
        manager.maybeRotateActiveVSR();
        assertSame(original, manager.getActiveManagedVSR());
        manager.flush();
    }

    public void testMaybeRotateAtThreshold() throws Exception {
        String filePath = createTempDir().resolve("rotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);

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
        VSRManager manager = new VSRManager(filePath, schema, bufferPool, 50000);

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
}
