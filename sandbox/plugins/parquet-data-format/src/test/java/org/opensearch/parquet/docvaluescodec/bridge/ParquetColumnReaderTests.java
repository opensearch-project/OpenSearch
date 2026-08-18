/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.docvaluescodec.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

/**
 * End-to-end coverage for the numeric Parquet doc-values read bridge: writes a real Parquet fixture
 * with {@link NativeParquetWriter}, then walks it through the FFM zero-copy borrow path
 * (Java -> native Rust cursor -> Arrow decode -> borrowed buffers read back in Java).
 */
public class ParquetColumnReaderTests extends OpenSearchTestCase {

    private static final String COLUMN = "value";

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    private static long expected(long row) {
        return row * 7 + 1;
    }

    public void testAscendingWalkReloadsBatches() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("ascending.parquet");
        writeLongColumn(file, rowCount, false, -1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (long row = 0; row < rowCount; row++) {
                DecodedBatch batch = reader.decodedBatch();
                if (batch == null || batch.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    batch = reader.decodedBatch();
                }
                assertTrue("row " + row + " should be in the batch", batch.contains(row));
                assertEquals(DecodedBatch.KIND_LONG, batch.valueKind());
                assertTrue("row " + row + " should be present", batch.isPresent(row));
                assertEquals("value at row " + row, expected(row), batch.valueAt(row));
            }
        }
    }

    public void testForwardJumpAndBackwardReopen() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("jump.parquet");
        writeLongColumn(file, rowCount, false, -1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(400);
            for (long row = 400; row <= 410; row++) {
                DecodedBatch batch = reader.decodedBatch();
                if (batch.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    batch = reader.decodedBatch();
                }
                assertTrue("row " + row + " should be present after forward jump", batch.isPresent(row));
                assertEquals("value at row " + row, expected(row), batch.valueAt(row));
            }

            reader.loadBatchContaining(10);
            DecodedBatch batch = reader.decodedBatch();
            assertTrue("row 10 should be in the batch after backward reopen", batch.contains(10));
            assertTrue("row 10 should be present", batch.isPresent(10));
            assertEquals("value at row 10", expected(10), batch.valueAt(10));
        }
    }

    public void testNullPresenceBitmap() throws Exception {
        int rowCount = 500;
        int nullEvery = 5;
        Path file = createTempDir().resolve("nullable.parquet");
        writeLongColumn(file, rowCount, true, nullEvery);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (long row = 0; row < rowCount; row++) {
                DecodedBatch batch = reader.decodedBatch();
                if (batch == null || batch.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    batch = reader.decodedBatch();
                }
                boolean expectNull = row % nullEvery == 0;
                if (expectNull) {
                    assertFalse("row " + row + " should be null", batch.isPresent(row));
                } else {
                    assertTrue("row " + row + " should be present", batch.isPresent(row));
                    assertEquals("value at row " + row, expected(row), batch.valueAt(row));
                }
            }
        }
    }

    private void writeLongColumn(Path file, int rowCount, boolean nullable, int nullEvery) throws Exception {
        FieldType fieldType = nullable
            ? FieldType.nullable(new ArrowType.Int(64, true))
            : FieldType.notNullable(new ArrowType.Int(64, true));
        Schema schema = new Schema(List.of(new Field(COLUMN, fieldType, null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (ArrowExport dataExport = exportData(schema, rowCount, nullEvery)) {
            writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
        }
        writer.flush();
    }

    private ArrowExport exportSchema(Schema schema) {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(Schema schema, int rowCount, int nullEvery) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector(COLUMN);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nullEvery > 0 && i % nullEvery == 0) {
                    vector.setNull(i);
                } else {
                    vector.setSafe(i, expected(i));
                }
            }
            vector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }
}
