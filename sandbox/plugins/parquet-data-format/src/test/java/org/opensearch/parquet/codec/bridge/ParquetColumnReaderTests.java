/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * End-to-end coverage for the numeric Parquet doc-values read bridge: writes a real Parquet fixture
 * with {@link NativeParquetWriter}, then walks it through the FFM zero-copy borrow path
 * (Java -> native Rust cursor -> Arrow decode -> borrowed buffers read back in Java).
 *
 * <p>Opening a cursor needs the DataFusion runtime manager and the global file-metadata cache the
 * analytics-backend-datafusion plugin owns, so each test starts a runtime rather than the reader
 * falling back to a private pool and cache of its own. Thread-leak detection is off because the
 * Tokio runtime manager is a per-JVM singleton whose threads outlive any one test class.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ParquetColumnReaderTests extends OpenSearchTestCase {

    private static final String COLUMN = "value";

    private BufferAllocator allocator;
    private long globalRuntimePtr;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        // Idempotent: the manager is a OnceLock, so another test class may already have started it.
        // Deliberately never shut down - doing so kills the shared executor for the rest of the JVM.
        DataFusionRuntimeFixture.initRuntimeManager(2);
        globalRuntimePtr = DataFusionRuntimeFixture.createGlobalRuntime(createTempDir("datafusion-spill"));
        assertNotEquals("global runtime must start before a cursor can be opened", 0L, globalRuntimePtr);
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        if (allocator != null) {
            allocator.close();
        }
        if (globalRuntimePtr != 0L) {
            DataFusionRuntimeFixture.closeGlobalRuntime(globalRuntimePtr);
        }
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

    public void testResidentRowIsServedWithoutMovingTheCursor() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("resident.parquet");
        writeLongColumn(file, rowCount, false, -1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(100);
            long firstRow = reader.decodedBatch().firstRow();
            long lastRow = reader.decodedBatch().lastRow();

            // The native cursor parks at lastRow + 1, so each of these would be a backward seek if
            // it reached the native side.
            for (long row = firstRow; row <= lastRow; row++) {
                reader.loadBatchContaining(row);
                DecodedBatch batch = reader.decodedBatch();
                assertEquals("resident row must not reload the batch", firstRow, batch.firstRow());
                assertEquals("resident row must not reload the batch", lastRow, batch.lastRow());
                assertEquals("value at row " + row, expected(row), batch.valueAt(row));
            }
        }
    }

    public void testFailedLoadDoesNotRetainTheStaleBatch() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("stale.parquet");
        writeLongColumn(file, rowCount, false, -1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(0);
            assertNotNull("a batch should be resident after a successful load", reader.decodedBatch());

            // A load past the end fails. The batch it would have replaced must not stay reachable,
            // because a successful native call frees the buffers the old batch borrowed.
            expectThrows(IOException.class, () -> reader.loadBatchContaining(rowCount));
            assertNull("no batch may remain resident after a failed load", reader.decodedBatch());
        }
    }

    public void testPresenceLookupOutsideTheBatchThrows() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("bounds.parquet");
        writeLongColumn(file, rowCount, true, 5);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(64);
            DecodedBatch batch = reader.decodedBatch();

            // The bitmap is byte-granular, so a row just past the batch can still land inside the
            // mapped bytes. It must be rejected rather than answered from a neighbouring bit.
            expectThrows(IndexOutOfBoundsException.class, () -> batch.isPresent(batch.lastRow() + 1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.isPresent(batch.firstRow() - 1));
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

    public void testNegativeDoublesUseSortableEncoding() throws Exception {
        double[] values = { -100.5, -0.5, 0.0, 3.25, -2.75, 42.0, -1.0e300, 1.0e300 };
        Path file = createTempDir().resolve("doubles.parquet");
        writeDoubleColumn(file, values);
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_DOUBLE, batch.valueKind());
                // valueAt returns the sortable long; sortableLongToDouble must recover the original value.
                assertEquals("value at row " + row, values[row], NumericUtils.sortableLongToDouble(batch.valueAt(row)), 0.0);
            }
            // Negatives must sort below positives in the encoded long space (range/skipper consumers).
            DecodedBatch batch = loadRow(reader, 0);
            assertTrue("negative double must sort below positive", batch.valueAt(0) < batch.valueAt(5));
        }
    }

    public void testNegativeFloatsUseSignExtendedSortableEncoding() throws Exception {
        float[] values = { -100.5f, -0.5f, 0.0f, 3.25f, -2.75f, 42.0f, -3.4e38f, 3.4e38f };
        Path file = createTempDir().resolve("floats.parquet");
        writeFloatColumn(file, values);
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_FLOAT, batch.valueKind());
                assertEquals("value at row " + row, values[row], NumericUtils.sortableIntToFloat((int) batch.valueAt(row)), 0.0f);
            }
            // Sign-extended, so a negative float's long compares below a positive float's long.
            DecodedBatch batch = loadRow(reader, 0);
            assertTrue("negative float must sort below positive (sign-extended)", batch.valueAt(0) < batch.valueAt(5));
        }
    }

    private static DecodedBatch loadRow(ParquetColumnReader reader, long row) throws java.io.IOException {
        DecodedBatch batch = reader.decodedBatch();
        if (batch == null || batch.contains(row) == false) {
            reader.loadBatchContaining(row);
            batch = reader.decodedBatch();
        }
        return batch;
    }

    private void writeDoubleColumn(Path file, double[] values) throws Exception {
        Schema schema = new Schema(
            List.of(new Field(COLUMN, FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
        );
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            Float8Vector vector = (Float8Vector) root.getVector(COLUMN);
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.setSafe(i, values[i]);
            }
            vector.setValueCount(values.length);
            root.setRowCount(values.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            try (ArrowExport dataExport = new ArrowExport(array, arrowSchema)) {
                writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
            }
        }
        writer.flush();
    }

    private void writeFloatColumn(Path file, float[] values) throws Exception {
        Schema schema = new Schema(
            List.of(new Field(COLUMN, FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null))
        );
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            Float4Vector vector = (Float4Vector) root.getVector(COLUMN);
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.setSafe(i, values[i]);
            }
            vector.setValueCount(values.length);
            root.setRowCount(values.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            try (ArrowExport dataExport = new ArrowExport(array, arrowSchema)) {
                writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
            }
        }
        writer.flush();
    }
}
