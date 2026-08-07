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
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class VSRManagerTests extends ParquetBaseTests {

    private static final DataFormat PARQUET_FORMAT = new ParquetDataFormat();
    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    /** Minimal schema VSRManager is constructed with; addDocument tests reconcile metadata fields in via {@link #reconcileMetadata}. */
    private Schema schema;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
        bufferPool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
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
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    public void testConstructionInitializesActiveVSR() throws Exception {
        String filePath = createTempDir().resolve("init.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        assertNotNull(manager.getActiveManagedVSR());
        assertEquals(VSRState.ACTIVE, manager.getActiveManagedVSR().getState());
        // flush handles freeze + close internally
        manager.flush();
    }

    public void testFlushWithNoDataReturnsMetadata() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        ParquetFileMetadata metadata = manager.flush();
        // With lazy native writer init, flush returns null when no data was written
        assertNull(metadata);
    }

    public void testFlushWithData() throws Exception {
        String filePath = createTempDir().resolve("data.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

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
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("add-doc.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(valField, 42);
        doc.setRowId("__row_id__", 0);
        manager.addDocument(doc);

        assertEquals(1, manager.getActiveManagedVSR().getRowCount());

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    public void testMaybeRotateNoOpBelowThreshold() throws Exception {
        String filePath = createTempDir().resolve("norotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        ManagedVSR original = manager.getActiveManagedVSR();
        original.setRowCount(100);
        manager.maybeRotateActiveVSR();
        assertSame(original, manager.getActiveManagedVSR());
        manager.flush();
    }

    /**
     * Regression for the ingest-pool leak: when {@code close()} fails to drain a background write
     * (awaitPendingWrite throws {@code IOException("Background VSR write failed...")}), it must still
     * release the VSR pool. The pre-fix close() called {@code vsrPool.close()} only after
     * awaitPendingWrite/flush, so a background-write failure skipped it and stranded the per-VSR
     * child allocators' off-heap buffers on the ingest pool for the node's lifetime ("Memory was
     * leaked by query"). Here we buffer data into the active VSR, inject an already-failed
     * pendingWrite so close() takes the throwing path, and assert the pool drains to zero.
     */
    public void testCloseReleasesPoolWhenBackgroundWriteFailed() throws Exception {
        String filePath = createTempDir().resolve("bgwrite-fail.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        // Materialize buffers on the active VSR's child allocator so the pool holds bytes.
        ManagedVSR active = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) active.getVector("val");
        for (int i = 0; i < 1000; i++) {
            vec.setSafe(i, i);
        }
        active.setRowCount(1000);
        assertTrue("pool holds buffers before close", bufferPool.getTotalAllocatedBytes() > 0);

        // Inject an already-failed background write so close()'s awaitPendingWrite throws — the exact
        // condition (Background VSR write failed) that used to skip vsrPool.close().
        java.util.concurrent.CompletableFuture<Object> failed = new java.util.concurrent.CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("simulated native write failure"));
        manager.setPendingWrite(failed);

        RuntimeException thrown = expectThrows(RuntimeException.class, manager::close);
        assertTrue(
            "close still surfaces the background-write failure",
            thrown.getMessage() != null && thrown.getMessage().contains("Failed to close VSRManager")
        );

        assertEquals("VSR pool must be released even when the background write failed", 0, bufferPool.getTotalAllocatedBytes());
    }

    public void testMaybeRotateAtThreshold() throws Exception {
        String filePath = createTempDir().resolve("rotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

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
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

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
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

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
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

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
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

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
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

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

    public void testAddDocumentAfterReconcileSchemaAddsVector() throws Exception {
        String filePath = createTempDir().resolve("unknown-field.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 1L);

        // Simulate a mapping update: the new schema introduces a tag field. reconcileSchema
        // adds the missing vector to the active VSR before addDocument runs.
        Schema updatedSchema = schemaWith("tag", new ArrowType.Utf8());
        manager.reconcileSchema(updatedSchema);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType tagField = new KeywordFieldMapper.KeywordFieldType("tag");
        assignTestCapabilities(valField, PARQUET_FORMAT);
        assignTestCapabilities(tagField, PARQUET_FORMAT);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        doc.addField(valField, 42);
        doc.addField(tagField, "hello");
        manager.addDocument(doc);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    public void testIsSchemaMutableBeforeAndAfterFlush() throws Exception {
        String filePath = createTempDir().resolve("schema-mutable.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 1L);
        reconcileMetadata(manager);

        assertTrue(manager.isSchemaMutable());

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        doc.addField(valField, 1);
        manager.addDocument(doc);

        manager.flush();
        assertFalse(manager.isSchemaMutable());
    }

    public void testSchemaUpdatePropagatesAcrossRotation() throws Exception {
        String filePath = createTempDir().resolve("schema-rotation.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 1, threadPool, 1L);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType tagField = new KeywordFieldMapper.KeywordFieldType("tag");
        assignTestCapabilities(valField, PARQUET_FORMAT);
        assignTestCapabilities(tagField, PARQUET_FORMAT);

        // Reconcile once before any docs — the tag vector must persist across the VSR
        // rotation triggered by maxRowsPerVSR=1.
        manager.reconcileSchema(schemaWith("tag", new ArrowType.Utf8()));
        {
            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
            doc1.addField(valField, 1);
            doc1.addField(tagField, "a");
            manager.addDocument(doc1);
        }

        {
            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 1L);
            doc2.addField(valField, 2);
            doc2.addField(tagField, "b");
            manager.addDocument(doc2); // this would've triggerer the rotation
        }

        {
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 2L);
            doc3.addField(valField, 3);
            doc3.addField(tagField, "c");
            manager.addDocument(doc3); // this would've triggerer the rotation
        }

        ParquetFileMetadata metadata = manager.flush();
        assertEquals(3, metadata.numRows());
    }

    public void testReconcileSchemaAddsMultipleVectorsAtOnce() throws Exception {
        String filePath = createTempDir().resolve("multi-unknown.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 1L);

        // Single reconcileSchema call adds three vectors plus the metadata fields the next
        // addDocument needs.
        List<Field> updatedFields = new ArrayList<>(schema.getFields());
        updatedFields.addAll(metadataFields());
        updatedFields.add(new Field("tag1", FieldType.nullable(new ArrowType.Utf8()), null));
        updatedFields.add(new Field("tag2", FieldType.nullable(new ArrowType.Utf8()), null));
        updatedFields.add(new Field("tag3", FieldType.nullable(new ArrowType.Utf8()), null));
        manager.reconcileSchema(new Schema(updatedFields));

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType tag1Field = new KeywordFieldMapper.KeywordFieldType("tag1");
        KeywordFieldMapper.KeywordFieldType tag2Field = new KeywordFieldMapper.KeywordFieldType("tag2");
        KeywordFieldMapper.KeywordFieldType tag3Field = new KeywordFieldMapper.KeywordFieldType("tag3");
        assignTestCapabilities(valField, PARQUET_FORMAT);
        assignTestCapabilities(tag1Field, PARQUET_FORMAT);
        assignTestCapabilities(tag2Field, PARQUET_FORMAT);
        assignTestCapabilities(tag3Field, PARQUET_FORMAT);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        doc.addField(valField, 1);
        doc.addField(tag1Field, "a");
        doc.addField(tag2Field, "b");
        doc.addField(tag3Field, "c");
        manager.addDocument(doc);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    /**
     * Verifies that {@link VSRManager#getAcceptedRows} is incremented only after a
     * successful row admit, and decremented by {@link VSRManager#rollbackTo(long)}.
     */
    public void testAcceptedRowsCounterTracksAdmitsAndRollbacks() throws Exception {
        String filePath = createTempDir().resolve("accepted-counter.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        reconcileMetadata(manager);
        try {
            assertEquals(0L, manager.getAcceptedRows());

            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
            manager.addDocument(doc1);
            assertEquals(1L, manager.getAcceptedRows());

            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 1L);
            manager.addDocument(doc2);
            assertEquals(2L, manager.getAcceptedRows());

            manager.rollbackTo(1L);
            assertEquals(1L, manager.getAcceptedRows());

            // Next doc reuses rowId 1 (the slot freed by rollback).
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 1L);
            manager.addDocument(doc3);
            assertEquals(2L, manager.getAcceptedRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies that the rowId column in the active VSR is sorted, contiguous, and
     * starts at 0 — the per-flush invariant that protects cross-format correlation.
     * Reads the rowId vector directly so it doesn't depend on the native flush.
     */
    public void testRowIdColumnIsSortedAndContiguous() throws Exception {
        String filePath = createTempDir().resolve("rowid-monotonic.parquet").toString();
        // Schema must declare __row_id__ for VSRManager to populate the column.
        List<Field> fields = new ArrayList<>(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema schemaWithRowId = new Schema(fields);
        VSRManager manager = new VSRManager(filePath, indexSettings, schemaWithRowId, bufferPool, 50000, threadPool, 0L);
        try {
            for (int i = 0; i < 50; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, (long) i);
                manager.addDocument(doc);
            }

            org.apache.arrow.vector.BigIntVector rowIdVector = (org.apache.arrow.vector.BigIntVector) manager.getActiveManagedVSR()
                .getVector(DocumentInput.ROW_ID_FIELD);
            assertNotNull(rowIdVector);
            for (int i = 0; i < 50; i++) {
                assertEquals("rowId at position " + i + " must equal " + i, (long) i, rowIdVector.get(i));
            }
        } finally {
            manager.close();
        }
    }

    /** Returns a copy of the test schema with one extra field appended (alongside metadata fields). */
    private Schema schemaWith(String name, ArrowType type) {
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.addAll(metadataFields());
        fields.add(new Field(name, FieldType.nullable(type), null));
        return new Schema(fields);
    }

    /**
     * Simulates the production mapping-update path: reconcile the active VSR with the
     * production-shaped schema (val + metadata fields) so that subsequent
     * {@code addDocument} calls find every vector they need.
     */
    private void reconcileMetadata(VSRManager manager) {
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.addAll(metadataFields());
        manager.reconcileSchema(new Schema(fields));
    }

    public void testAddDocumentAfterSuccessfulBackgroundWriteDoesNotThrow() throws Exception {
        // Use a very low rotation threshold to trigger background writes frequently
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("bg-write-success.parquet").toString();
        int lowThreshold = randomIntBetween(2, 5);
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, lowThreshold, threadPool, 0L);

        NumberFieldMapper.NumberFieldType valField = createNumberField("val", NumberFieldMapper.NumberType.INTEGER);

        // Run multiple rotation cycles — each cycle fills the VSR to threshold,
        // triggers background write, waits for completion, then verifies next addDocument works
        int cycles = randomIntBetween(3, 8);
        int rowId = 0;
        for (int cycle = 0; cycle < cycles; cycle++) {
            for (int i = 0; i < lowThreshold; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.addField(valField, rowId);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
                manager.addDocument(doc);
                rowId++;
            }

            // Wait for background write to complete using assertBusy
            assertBusy(() -> {
                Future<?> f = manager.getPendingWrite();
                assertTrue("Background write should complete", f == null || f.isDone());
            });

            // This addDocument must NOT throw — verifies the fix for the
            // exceptionNow() bug on successfully completed futures
            ParquetDocumentInput nextDoc = new ParquetDocumentInput();
            populateMetadataFields(nextDoc);
            nextDoc.addField(valField, rowId);
            nextDoc.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
            manager.addDocument(nextDoc);
            rowId++;
        }

        manager.flush();
    }

    public void testContinuousAddDocumentAcrossMultipleRotationsWithoutWaiting() throws Exception {
        // Continuously add documents across many rotations without ever waiting for
        // background writes — verifies no error when future is still running or not yet done
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("continuous-add.parquet").toString();
        int lowThreshold = randomIntBetween(2, 4);
        int totalDocs = lowThreshold * randomIntBetween(5, 12);
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, lowThreshold, threadPool, 0L);

        NumberFieldMapper.NumberFieldType valField = createNumberField("val", NumberFieldMapper.NumberType.INTEGER);

        // Add all docs in a tight loop — no waiting between rotations
        for (int i = 0; i < totalDocs; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(valField, i);
            doc.setRowId(DocumentInput.ROW_ID_FIELD, i);
            manager.addDocument(doc);
        }

        // Flush at the end — must succeed regardless of pending write state
        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(totalDocs, metadata.numRows());
    }

    public void testAllowsDistinctFieldsInSingleDocument() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("price", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("qty", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("distinct.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        NumberFieldMapper.NumberFieldType priceField = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.INTEGER);
        NumberFieldMapper.NumberFieldType qtyField = new NumberFieldMapper.NumberFieldType("qty", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(priceField, PARQUET_FORMAT);
        assignTestCapabilities(qtyField, PARQUET_FORMAT);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(priceField, 10);
        doc.addField(qtyField, 5);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);

        manager.addDocument(doc);
        assertEquals(1, manager.getActiveManagedVSR().getRowCount());
        manager.flush();
    }

}
