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
    private NativeObjectStore objectStore;

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
        if (objectStore != null) {
            objectStore.close();
        }
        allocator.close();
        super.tearDown();
    }

    public void testFullLifecycle() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("full.parquet");

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
        writer.close();
        assertTrue("Parquet file should exist after flush", Files.exists(dir.resolve("full.parquet")));
    }

    public void testMultipleBatchesThenFlush() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("multi-batch.parquet");

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
        writer.close();
        assertTrue("Parquet file should exist after flush", Files.exists(dir.resolve("multi-batch.parquet")));
    }

    public void testFlushWithoutWrite() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("close-only.parquet");
        writer.flush();
        assertNotNull(writer.getMetadata());
        assertEquals(0, writer.getMetadata().numRows());
        writer.close();
    }

    public void testFlushIsIdempotent() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("idempotent.parquet");
        writer.flush();
        ParquetFileMetadata first = writer.getMetadata();
        writer.flush();
        assertSame(first, writer.getMetadata());
        writer.close();
    }

    public void testWriteAfterFlushThrows() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("write-after-flush.parquet");
        writer.flush();

        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "alice" }, new long[] { 10L })) {
            expectThrows(IOException.class, () -> writer.write(export.getArrayAddress(), export.getSchemaAddress()));
        }
        writer.close();
    }

    public void testCreateWriterWithInvalidSchemaAddress() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        expectThrows(Exception.class, () -> new NativeParquetWriter(objectStore, "bad-schema.parquet", 0L));
    }

    public void testWriteEmptyBatch() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("empty-batch.parquet");

        try (ArrowExport export = exportData(new int[] {}, new String[] {}, new long[] {})) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        writer.flush();
        assertNotNull(writer.getMetadata());
        assertEquals(0, writer.getMetadata().numRows());
        writer.close();
    }

    public void testWriteWithNullAddresses() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("null-addr.parquet");

        expectThrows(IOException.class, () -> writer.write(0L, 0L));

        try (ArrowExport schemaExport = exportSchema()) {
            expectThrows(IOException.class, () -> writer.write(0L, schemaExport.getSchemaAddress()));
        }

        try (ArrowExport dataExport = exportData(new int[] { 1 }, new String[] { "a" }, new long[] { 1L })) {
            expectThrows(IOException.class, () -> writer.write(dataExport.getArrayAddress(), 0L));
        }

        writer.flush();
        writer.close();
    }

    public void testCloseIsIdempotent() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        NativeParquetWriter writer = createWriter("close-twice.parquet");
        writer.flush();
        writer.close();
        writer.close(); // should not throw
    }

    private NativeParquetWriter createWriter(String path) throws Exception {
        try (ArrowExport export = exportSchema()) {
            return new NativeParquetWriter(objectStore, path, export.getSchemaAddress());
        }
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
