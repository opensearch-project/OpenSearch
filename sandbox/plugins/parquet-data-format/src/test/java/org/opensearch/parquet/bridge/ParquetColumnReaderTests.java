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
import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;
import org.opensearch.parquet.codec.cache.PageCache;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Unit tests for {@link ParquetColumnReader} (task 2.4). Writes a small Parquet file with
 * the native writer, then exercises single/repeated reads, null handling, error paths, the
 * page index, and page-decode cache population.
 */
public class ParquetColumnReaderTests extends OpenSearchTestCase {

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

    public void testReadIntColumnSingleValues() throws Exception {
        Path file = writeFile(new int[] { 10, 20, 30 }, new String[] { "alice", "bob", "carol" }, new long[] { 100L, 200L, 300L });
        try (BufferPool pool = new BufferPool(); ParquetColumnReader r = open(file, "id", ParquetPhysicalType.INT32, false, pool)) {
            assertEquals(new ParquetColumnReader.Value(true, 10L), r.readValueAtRow(0));
            assertEquals(new ParquetColumnReader.Value(true, 20L), r.readValueAtRow(1));
            assertEquals(new ParquetColumnReader.Value(true, 30L), r.readValueAtRow(2));
        }
    }

    public void testReadBinaryColumnWithNull() throws Exception {
        Path file = writeFileWithNullName(new int[] { 1, 2, 3 }, new String[] { "alice", null, "carol" }, new long[] { 1, 2, 3 });
        try (BufferPool pool = new BufferPool(); ParquetColumnReader r = open(file, "name", ParquetPhysicalType.BYTE_ARRAY, false, pool)) {
            assertEquals("alice", new String(r.readBytesAtRow(0), StandardCharsets.UTF_8));
            assertNull(r.readBytesAtRow(1));
            assertEquals("carol", new String(r.readBytesAtRow(2), StandardCharsets.UTF_8));
        }
    }

    public void testRowOutOfRangeThrows() throws Exception {
        Path file = writeFile(new int[] { 1, 2 }, new String[] { "a", "b" }, new long[] { 1, 2 });
        try (BufferPool pool = new BufferPool(); ParquetColumnReader r = open(file, "id", ParquetPhysicalType.INT32, false, pool)) {
            IOException e = expectThrows(IOException.class, () -> r.readValueAtRow(99));
            assertTrue("error should name the row: " + e.getMessage(), e.getMessage().contains("99"));
        }
    }

    public void testMissingColumnThrows() throws Exception {
        Path file = writeFile(new int[] { 1 }, new String[] { "a" }, new long[] { 1 });
        try (BufferPool pool = new BufferPool()) {
            IOException e = expectThrows(IOException.class, () -> open(file, "nope", ParquetPhysicalType.INT32, false, pool));
            assertTrue("error should name the missing column: " + e.getMessage(), e.getMessage().contains("nope"));
        }
    }

    public void testTypeMismatchThrows() throws Exception {
        Path file = writeFile(new int[] { 1 }, new String[] { "a" }, new long[] { 1 });
        try (BufferPool pool = new BufferPool()) {
            // id is INT32 — asking for BYTE_ARRAY must fail.
            IOException e = expectThrows(IOException.class, () -> open(file, "id", ParquetPhysicalType.BYTE_ARRAY, false, pool));
            assertTrue(
                "error should mention a mismatch: " + e.getMessage(),
                e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("mismatch")
            );
        }
    }

    public void testPageIndexAndDecode() throws Exception {
        Path file = writeFile(new int[] { 10, 20, 30 }, new String[] { "a", "b", "c" }, new long[] { 1, 2, 3 });
        try (BufferPool pool = new BufferPool(); ParquetColumnReader r = open(file, "id", ParquetPhysicalType.INT32, false, pool)) {
            ColumnPageIndex idx = r.pageIndex();
            assertTrue("expected at least one page", idx.pageCount() >= 1);
            assertEquals(0L, idx.firstRowOf(0));
            assertEquals(3L, idx.totalRows());
            assertEquals(0, idx.pageForRow(0));
            assertEquals(0, idx.pageForRow(2));

            r.loadPageContaining(1);
            PageCache cache = r.cache();
            assertNotNull("page should be cached", cache);
            assertEquals(0L, cache.firstRow);
            assertEquals(2L, cache.lastRow);
            assertTrue(cache.isPresent(1));
            assertEquals(20L, cache.valueAt(1));
            assertEquals(10L, cache.valueAt(0));
            assertEquals(30L, cache.valueAt(2));
        }
    }

    public void testCloseIsIdempotentAndHandlesDoNotLeak() throws Exception {
        Path file = writeFile(new int[] { 1, 2 }, new String[] { "a", "b" }, new long[] { 1, 2 });
        long before = RustBridge.openColumnReaderCount();
        try (BufferPool pool = new BufferPool()) {
            ParquetColumnReader r = open(file, "id", ParquetPhysicalType.INT32, false, pool);
            assertEquals(before + 1, RustBridge.openColumnReaderCount());
            r.close();
            r.close(); // idempotent
            assertEquals(before, RustBridge.openColumnReaderCount());
        }
    }

    // ── helpers ──

    private static ParquetColumnReader open(Path file, String col, ParquetPhysicalType type, boolean repeated, BufferPool pool)
        throws IOException {
        return ParquetColumnReader.open(file, col, type, repeated, pool);
    }

    private Path writeFile(int[] ids, String[] names, long[] scores) throws Exception {
        return writeFileWithNullName(ids, names, scores);
    }

    private Path writeFileWithNullName(int[] ids, String[] names, long[] scores) throws Exception {
        Path file = createTempDir().resolve("colreader.parquet");
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema()) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (ArrowExport export = exportData(ids, names, scores)) {
            writer.write(export.getArrayAddress(), export.getSchemaAddress());
        }
        writer.flush();
        return file;
    }

    private ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(int[] ids, String[] names, long[] scores) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            BigIntVector scoreVec = (BigIntVector) root.getVector("score");
            for (int i = 0; i < ids.length; i++) {
                idVec.setSafe(i, ids[i]);
                if (names[i] == null) {
                    nameVec.setNull(i);
                } else {
                    nameVec.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
                }
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
