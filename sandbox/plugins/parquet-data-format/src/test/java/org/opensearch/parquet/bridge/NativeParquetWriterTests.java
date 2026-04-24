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

public class NativeParquetWriterTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;
    private long storeHandle;
    private Path storeRoot;

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
        // Create a local FS ObjectStore rooted at a temp dir
        // This simulates what FsNativeObjectStorePlugin does
        storeRoot = createTempDir();
        // We need the native library to create a store handle.
        // For unit tests that mock the native layer, storeHandle would be mocked.
        // For integration tests, we use the real FFM bridge.
        storeHandle = createFsStore(storeRoot.toString());
    }

    @Override
    public void tearDown() throws Exception {
        if (storeHandle > 0) {
            RustBridge.destroyStore(storeHandle);
        }
        allocator.close();
        super.tearDown();
    }

    public void testFullLifecycle() throws Exception {
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

        ParquetFileMetadata metadata = writer.flush();
        assertNotNull(metadata);
        assertEquals(3, metadata.numRows());

        assertTrue("Parquet file should exist", Files.exists(storeRoot.resolve("full.parquet")));
    }

    public void testMultipleBatches() throws Exception {
        NativeParquetWriter writer = createWriter("multi-batch.parquet");

        try (ArrowExport batch1 = exportData(new int[] { 1, 2 }, new String[] { "alice", "bob" }, new long[] { 10L, 20L })) {
            writer.write(batch1.getArrayAddress(), batch1.getSchemaAddress());
        }

        try (
            ArrowExport batch2 = exportData(new int[] { 3, 4, 5 }, new String[] { "carol", "dave", "eve" }, new long[] { 30L, 40L, 50L })
        ) {
            writer.write(batch2.getArrayAddress(), batch2.getSchemaAddress());
        }

        ParquetFileMetadata metadata = writer.flush();
        assertEquals(5, metadata.numRows());
        assertTrue("Parquet file should exist", Files.exists(storeRoot.resolve("multi-batch.parquet")));
    }

    public void testFlushWithoutWrite() throws Exception {
        NativeParquetWriter writer = createWriter("close-only.parquet");
        ParquetFileMetadata metadata = writer.flush();
        assertNotNull(metadata);
        assertEquals(0, metadata.numRows());
    }

    public void testFlushIsIdempotent() throws Exception {
        NativeParquetWriter writer = createWriter("idempotent.parquet");
        writer.flush();
        ParquetFileMetadata first = writer.getMetadata();
        writer.flush();
        assertSame(first, writer.getMetadata());
    }

    public void testWriteAfterFlushThrows() throws Exception {
        NativeParquetWriter writer = createWriter("write-after-flush.parquet");
        writer.flush();

        try (ArrowExport export = exportData(new int[] { 1 }, new String[] { "alice" }, new long[] { 10L })) {
            expectThrows(IOException.class, () -> writer.write(export.getArrayAddress(), export.getSchemaAddress()));
        }
    }

    public void testWriteEmptyBatch() throws Exception {
        NativeParquetWriter writer = createWriter("empty-batch.parquet");

        try (ArrowExport export = exportData(new int[] {}, new String[] {}, new long[] {})) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        ParquetFileMetadata metadata = writer.flush();
        assertNotNull(metadata);
        assertEquals(0, metadata.numRows());
    }

    public void testPrefixStoreWritesToCorrectPath() throws Exception {
        long scopedHandle = RustBridge.createScopedStore(storeHandle, "indices/test-uuid/0/parquet/");
        try {
            NativeParquetWriter writer;
            try (ArrowExport export = exportSchema()) {
                writer = new NativeParquetWriter(scopedHandle, "gen_1.parquet", export.getSchemaAddress());
            }

            try (ArrowExport export = exportData(new int[] { 1, 2 }, new String[] { "a", "b" }, new long[] { 10L, 20L })) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }

            ParquetFileMetadata metadata = writer.flush();
            assertEquals(2, metadata.numRows());

            // Verify file at prefixed path
            Path expectedFile = storeRoot.resolve("indices/test-uuid/0/parquet/gen_1.parquet");
            assertTrue("File should exist at prefixed path: " + expectedFile, Files.exists(expectedFile));
        } finally {
            RustBridge.destroyStore(scopedHandle);
        }
    }

    public void testCrc32IsNonZero() throws Exception {
        NativeParquetWriter writer = createWriter("crc32.parquet");

        try (ArrowExport export = exportData(new int[] { 1, 2, 3 }, new String[] { "a", "b", "c" }, new long[] { 1L, 2L, 3L })) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }

        ParquetFileMetadata metadata = writer.flush();
        assertNotEquals("CRC32 should be non-zero", 0L, metadata.crc32());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates a local FS ObjectStore via the native FFM bridge.
     * This simulates what FsNativeObjectStorePlugin.createNativeStore() does.
     */
    private long createFsStore(String rootPath) throws IOException {
        // The FS native plugin creates a LocalFileSystem store via FFM.
        // For tests, we use the same FFM path through the parquet plugin's
        // scoped store mechanism — create a "root" store by calling the FS plugin's
        // native function, or use a test helper.
        //
        // Since we don't have direct access to fs_create_store from this plugin,
        // we'll create the store via the FS plugin's Java API if available,
        // or use a test-only FFM call.
        //
        // For now, use the FS plugin's native function directly:
        return org.opensearch.repositories.fs.native_store.FsNativeObjectStorePlugin.createTestStore(rootPath);
    }

    private NativeParquetWriter createWriter(String objectPath) throws Exception {
        try (ArrowExport export = exportSchema()) {
            return new NativeParquetWriter(storeHandle, objectPath, export.getSchemaAddress());
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
}
