/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

public class NativeParquetWriterTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testFullLifecycle() throws Exception {
        String filePath = createTempDir().resolve("full.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        try (
            ArrowExport export = exportData(
                new int[] { 1, 2, 3 },
                new String[] { "alice", "bob", "carol" },
                new long[] { 100L, 200L, 300L }
            )
        ) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        writer.flush();
        assertNotNull(writer.getMetadata());
        assertEquals(3, writer.getMetadata().numRows());

        writer.sync();
        assertTrue("Parquet file should exist after flush", Files.exists(Path.of(filePath)));
    }

    public void testMultipleBatchesThenCloseAndFlush() throws Exception {
        String filePath = createTempDir().resolve("multi-batch.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        try (ArrowExport batch1 = exportData(new int[] { 1, 2 }, new String[] { "alice", "bob" }, new long[] { 10L, 20L })) {
            writer.write(batch1.getArrayAddress(), batch1.getSchemaAddress());
        }

        try (
            ArrowExport batch2 = exportData(new int[] { 3, 4, 5 }, new String[] { "carol", "dave", "eve" }, new long[] { 30L, 40L, 50L })
        ) {
            writer.write(batch2.getArrayAddress(), batch2.getSchemaAddress());
        }

        writer.flush();
        assertEquals(5, writer.getMetadata().numRows());
        writer.sync();
        assertTrue("Parquet file should exist after flush", Files.exists(Path.of(filePath)));
    }

    public void testFlushWithoutWrite() throws Exception {
        String filePath = createTempDir().resolve("close-only.parquet").toString();
        NativeParquetWriter writer = new NativeParquetWriter(filePath);
        ParquetFileMetadata metadata = writer.flush();
        // Writer was never initialized, so flush returns null
        assertNull(metadata);
    }

    public void testFlushIsIdempotent() throws Exception {
        String filePath = createTempDir().resolve("idempotent.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        // Write some data first so the writer gets initialized
        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "alice" }, new long[] { 10L })) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        writer.flush();
        ParquetFileMetadata first = writer.getMetadata();
        writer.flush();
        assertSame(first, writer.getMetadata());
    }

    public void testSyncAutoFlushesIfNotFlushed() throws Exception {
        String filePath = createTempDir().resolve("auto-flush.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "alice" }, new long[] { 10L })) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        // sync without explicit close — should auto-close first
        assertNull(writer.getMetadata());
        writer.sync();
        assertNotNull(writer.getMetadata());
        assertEquals(1, writer.getMetadata().numRows());
        assertTrue("Parquet file should exist after flush", Files.exists(Path.of(filePath)));
    }

    public void testWriteAfterFlushThrows() throws Exception {
        String filePath = createTempDir().resolve("write-after-flush.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        // Initialize writer with a write, then flush
        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "a" }, new long[] { 1L })) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }
        writer.flush();

        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "alice" }, new long[] { 10L })) {
            expectThrows(IOException.class, () -> writer.write(export.getArrayAddress(), export.getSchemaAddress()));
        }
    }

    public void testCreateWriterWithNonExistentDirectory() throws Exception {
        // Constructor is lightweight — error surfaces on initialize
        NativeParquetWriter writer = new NativeParquetWriter("/nonexistent/dir/file.parquet");
        try (ArrowExport export = exportSchema()) {
            expectThrows(
                IOException.class,
                () -> writer.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L)
            );
        }
    }

    public void testCreateWriterWithInvalidSchemaAddress() throws Exception {
        String filePath = createTempDir().resolve("bad-schema.parquet").toString();
        NativeParquetWriter writer = new NativeParquetWriter(filePath);
        assertFalse(writer.isInitialized());
        expectThrows(Exception.class, () -> writer.initialize("test-index", 0L, ParquetSortConfig.empty(), 0L));
    }

    public void testWriteWithSchemaMismatch() throws Exception {
        String filePath = createTempDir().resolve("mismatch.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        // First write initializes the writer with the correct schema
        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "a" }, new long[] { 1L })) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        // Second write with mismatched schema
        try (
            ArrowExport export = exportDataWithSchema(
                new Schema(List.of(new Field("other_field", FieldType.nullable(new ArrowType.Int(32, true)), null))),
                root -> {
                    ((IntVector) root.getVector("other_field")).setSafe(0, 99);
                    root.setRowCount(1);
                }
            )
        ) {
            // Rust side may or may not throw on schema mismatch during write;
            // the mismatch is detected at flush time
            try {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            } catch (IOException e) {
                // Expected — some Rust versions detect mismatch at write time
                return;
            }
        }

        // If write didn't throw, flush should detect the mismatch
        expectThrows(IOException.class, writer::flush);
    }

    public void testCreateDuplicateWriterForSameFile() throws Exception {
        String filePath = createTempDir().resolve("duplicate.parquet").toString();
        NativeParquetWriter writer1 = createWriter(filePath);

        // Initialize writer1 by writing data
        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "a" }, new long[] { 1L })) {
            writer1.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        // Native side rejects creating a second writer for the same file
        NativeParquetWriter writer2 = new NativeParquetWriter(filePath);
        try (ArrowExport export = exportSchema()) {
            expectThrows(
                IOException.class,
                () -> writer2.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L)
            );
        }

        writer1.flush();
    }

    public void testSyncCalledTwice() throws Exception {
        String filePath = createTempDir().resolve("double-sync.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "alice" }, new long[] { 10L })) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        writer.flush();
        writer.sync();
        assertTrue("Parquet file should exist after flush", Files.exists(Path.of(filePath)));
        // Second sync fails — native side removed file from FILE_MANAGER after first fsync
        expectThrows(IOException.class, writer::sync);
    }

    public void testWriteEmptyBatch() throws Exception {
        String filePath = createTempDir().resolve("empty-batch.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        try (ArrowExport export = exportData(new int[] {}, new String[] {}, new long[] {})) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        writer.flush();
        assertNotNull(writer.getMetadata());
    }

    public void testWriteWithNullAddresses() throws Exception {
        String filePath = createTempDir().resolve("null-addr.parquet").toString();
        NativeParquetWriter writer = createWriter(filePath);

        // Both null
        expectThrows(IOException.class, () -> writer.write(0L, 0L));

        // Null array only
        try (ArrowExport schemaExport = exportSchema()) {
            expectThrows(IOException.class, () -> writer.write(0L, schemaExport.getSchemaAddress()));
        }

        // Null schema only
        try (ArrowExport dataExport = exportData(new int[] { 1 }, new String[] { "a" }, new long[] { 1L })) {
            expectThrows(IOException.class, () -> writer.write(dataExport.getArrayAddress(), 0L));
        }
    }

    private NativeParquetWriter createWriter(String filePath) throws Exception {
        NativeParquetWriter writer = new NativeParquetWriter(filePath);
        try (ArrowExport export = exportSchema()) {
            writer.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        return writer;
    }

    private ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(int[] ids, String[] names, long[] scores) {
        assert ids.length == names.length && names.length == scores.length;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            BigIntVector scoreVec = (BigIntVector) root.getVector("score");
            for (int i = 0; i < ids.length; i++) {
                idVec.setSafe(i, ids[i]);
                nameVec.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
                scoreVec.setSafe(i, scores[i]);
            }
            root.setRowCount(ids.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }

    private ArrowExport exportDataWithSchema(Schema customSchema, Consumer<VectorSchemaRoot> populator) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(customSchema, allocator)) {
            populator.accept(root);
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }
}
