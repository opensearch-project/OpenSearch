/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.be.datafusion.DatafusionSettings;
import org.opensearch.common.settings.Settings;
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

    /**
     * Registering a setting as {@code IndexScope} only makes it *settable* per index; this proves
     * the value is *honoured* per reader: two readers over the same file, opened with different
     * index settings (as two indices would open them), get different decode windows.
     */
    public void testIndexScopedBatchSizeSettingsAreHonouredPerReader() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("scoped.parquet");
        writeLongColumn(file, rowCount, false, -1);

        Settings small = Settings.builder()
            .put(DatafusionSettings.DOCVALUES_INITIAL_BATCH_SIZE.getKey(), 4)
            .put(DatafusionSettings.DOCVALUES_MAX_BATCH_SIZE.getKey(), 8)
            .build();

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN, small)) {
            reader.loadBatchContaining(0);
            DecodedBatch batch = reader.decodedBatch();
            assertTrue(batch.contains(0));
            assertTrue(batch.contains(3));
            assertFalse("initial window of 4 must not include row 4", batch.contains(4));

            // Dense walk: the adaptive window may grow, but never past max_batch_size = 8.
            for (long row = 0; row < rowCount; row++) {
                DecodedBatch current = reader.decodedBatch();
                if (current == null || current.contains(row) == false) {
                    reader.loadBatchContaining(row);
                    current = reader.decodedBatch();
                }
                assertTrue("row " + row + " should be in the batch", current.contains(row));
                assertFalse("window must stay capped at max_batch_size=8 rows", current.contains(row + 8));
            }
        }

        // The same file under default settings (initial 32): row 4 IS resident after the first
        // load — the two readers diverge purely on the Settings they were opened with.
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            reader.loadBatchContaining(0);
            DecodedBatch batch = reader.decodedBatch();
            assertTrue("default initial window (32) must include row 4", batch.contains(4));
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

    // Sign-extension coverage for KIND_INT: a signed 32-bit column must widen each stored int into
    // the identically-signed long. If the codec zero-extended it instead (the u32 path), -1 would
    // surface as 4294967295 and Integer.MIN_VALUE as 2147483648 - this test catches that swap.
    public void testIntColumnSignExtendsNegatives() throws Exception {
        long[] values = { -1L, Integer.MIN_VALUE, -987654L, 0L, 42L, Integer.MAX_VALUE };
        Path file = createTempDir().resolve("int32-signed.parquet");
        writeIntegralColumn(file, new ArrowType.Int(32, true), values);
        assertIntegralColumn(file, DecodedBatch.KIND_INT, values);
    }

    // Zero-extension coverage for KIND_UINT_BITS: an unsigned 32-bit column must widen each stored
    // u32 into a non-negative long. If the codec sign-extended it instead (the i32 path), any value
    // above Integer.MAX_VALUE - here 3000000000 and 4294967295 - would surface as a negative long.
    public void testUnsignedIntColumnZeroExtends() throws Exception {
        long[] values = { 0L, 42L, 3000000000L, 4294967295L };
        Path file = createTempDir().resolve("uint32.parquet");
        writeIntegralColumn(file, new ArrowType.Int(32, false), values);
        assertIntegralColumn(file, DecodedBatch.KIND_UINT_BITS, values);
        // Explicitly guard the sign: zero-extension must never produce a negative long.
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < values.length; row++) {
                assertTrue("u32 value at row " + row + " must be non-negative", loadRow(reader, row).valueAt(row) >= 0L);
            }
        }
    }

    // Sign-extension coverage for KIND_SHORT and KIND_BYTE: a signed 16- or 8-bit column must widen
    // each stored value into the identically-signed long. A zero-extension bug would turn -1 into
    // 65535 (short) / 255 (byte) and the MIN_VALUEs into their positive u16/u8 counterparts.
    public void testShortAndByteColumnsSignExtend() throws Exception {
        long[] shortValues = { -1L, Short.MIN_VALUE, -1234L, 0L, 42L, Short.MAX_VALUE };
        Path shortFile = createTempDir().resolve("int16-signed.parquet");
        writeIntegralColumn(shortFile, new ArrowType.Int(16, true), shortValues);
        assertIntegralColumn(shortFile, DecodedBatch.KIND_SHORT, shortValues);

        long[] byteValues = { -1L, Byte.MIN_VALUE, -50L, 0L, 42L, Byte.MAX_VALUE };
        Path byteFile = createTempDir().resolve("int8-signed.parquet");
        writeIntegralColumn(byteFile, new ArrowType.Int(8, true), byteValues);
        assertIntegralColumn(byteFile, DecodedBatch.KIND_BYTE, byteValues);
    }

    // Zero-extension coverage for KIND_USHORT and KIND_UBYTE: an unsigned 16- or 8-bit column must
    // widen each stored value into a non-negative long. A sign-extension bug would turn 50000 into
    // -15536 (u16) and 200 into -56 (u8) once the top bit is set.
    public void testUnsignedShortAndByteZeroExtend() throws Exception {
        long[] ushortValues = { 0L, 42L, 50000L, 65535L };
        Path ushortFile = createTempDir().resolve("uint16.parquet");
        writeIntegralColumn(ushortFile, new ArrowType.Int(16, false), ushortValues);
        assertIntegralColumn(ushortFile, DecodedBatch.KIND_USHORT, ushortValues);

        long[] ubyteValues = { 0L, 42L, 200L, 255L };
        Path ubyteFile = createTempDir().resolve("uint8.parquet");
        writeIntegralColumn(ubyteFile, new ArrowType.Int(8, false), ubyteValues);
        assertIntegralColumn(ubyteFile, DecodedBatch.KIND_UBYTE, ubyteValues);
    }

    /** Reads every row back and asserts both the mapped value kind and the exact widened long. */
    private void assertIntegralColumn(Path file, int expectedKind, long[] values) throws Exception {
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals("value kind", expectedKind, batch.valueKind());
                assertTrue("row " + row + " should be present", batch.isPresent(row));
                assertEquals("value at row " + row, values[row], batch.valueAt(row));
            }
        }
    }

    /**
     * Writes a single non-nullable integer column of the caller-chosen Arrow width and signedness,
     * reusing the same {@link NativeParquetWriter}/{@link ParquetSortConfig} path as
     * {@link #writeLongColumn}. Each supplied {@code long} is narrowed to the column's width by its
     * low bits (so callers can express both signed negatives and unsigned values above the signed
     * maximum via the same {@code long[]}), matching how the reader widens the stored bits back.
     */
    private void writeIntegralColumn(Path file, ArrowType.Int arrowType, long[] values) throws Exception {
        Schema schema = new Schema(List.of(new Field(COLUMN, FieldType.notNullable(arrowType), null)));
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FieldVector vector = root.getVector(COLUMN);
            vector.setInitialCapacity(values.length);
            vector.allocateNew();
            for (int i = 0; i < values.length; i++) {
                setIntegral(vector, i, values[i]);
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

    /**
     * Stores {@code value}'s low bits into the width-specific Arrow vector. Every vector here exposes
     * {@code setSafe(int, int)}, so the {@code (int)} cast carries the raw bit pattern - the unsigned
     * vectors reinterpret those bits without sign, which is exactly what the read path must reverse.
     */
    private static void setIntegral(FieldVector vector, int index, long value) {
        if (vector instanceof IntVector v) {
            v.setSafe(index, (int) value);
        } else if (vector instanceof UInt4Vector v) {
            v.setSafe(index, (int) value);
        } else if (vector instanceof SmallIntVector v) {
            v.setSafe(index, (int) value);
        } else if (vector instanceof UInt2Vector v) {
            v.setSafe(index, (int) value);
        } else if (vector instanceof TinyIntVector v) {
            v.setSafe(index, (int) value);
        } else if (vector instanceof UInt1Vector v) {
            v.setSafe(index, (int) value);
        } else {
            throw new IllegalArgumentException("unsupported integral vector " + vector.getClass().getName());
        }
    }
}
