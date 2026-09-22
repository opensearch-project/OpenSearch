/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.be.datafusion.DatafusionSettings;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetNumericDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Conformance matrix for the Parquet doc-values format, mirroring the shape of Lucene's
 * {@code BaseDocValuesFormatTestCase}. The format exposes no Lucene {@code fieldsConsumer} or
 * SPI-loaded {@code fieldsProducer} - writes go through {@link NativeParquetWriter} and reads through
 * the reader wrapper - so it cannot extend that base directly; the matrix drives the same cells
 * through the Parquet read/write path instead.
 *
 * <p>Cells:
 * <ul>
 *   <li><b>Numeric type coverage</b> - every mapped numeric kind (signed/unsigned int/short/byte,
 *       long, float, double, half_float, boolean) written as a Parquet fixture and read back through
 *       the native cursor / {@link DecodedBatch}, asserting the exact widened/sortable value.</li>
 *   <li><b>Numeric iterator semantics</b> - {@link ParquetNumericDocValues} over a fixture:
 *       ascending scan, null handling, {@code nextDoc}/{@code advance}/{@code advanceExact}, random
 *       access, and batch-boundary behaviour.</li>
 *   <li><b>Producer lifecycle / no-leak</b> - the segment-lifetime producer is shared across requests
 *       and closed by the segment core, while each request closes only the cursors it opened. Native
 *       FFM cursors are invisible to the base class's {@code MockDirectory} leak gate, so the cursor
 *       close chain is asserted directly.</li>
 *   <li><b>Unsupported kinds</b> - {@code SORTED}, {@code SORTED_SET}, {@code BINARY}, and plain
 *       {@code NUMERIC} are asserted unsupported, stating the SORTED_NUMERIC-only scope.</li>
 * </ul>
 *
 * <p>The per-file format-version and writer-generation admission checks are not part of the Lucene
 * doc-values format contract and live in {@code ParquetDocValuesProducerTests}.
 *
 * <p>The DataFusion runtime and Arrow allocator a cursor needs come from {@link DataFusionBackedTestCase}.
 */
public class ParquetDocValuesFormatTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";
    private static final int ROWS = 300;
    private static final int NULL_EVERY = 5;
    /** Fixed decode window for the boundary-sensitive tests: any gap above this crosses a batch. */
    private static final int FIXED_WINDOW = 16;

    private static long expected(long row) {
        return row * 7 + 1;
    }

    // ------------------------------------------------------------------------------------------------
    // Numeric type coverage: real Parquet fixtures read back through the native cursor / DecodedBatch.
    // ------------------------------------------------------------------------------------------------

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

    /**
     * Boolean is the only bit-packed kind: one bit per row rather than a whole byte, so a wrong bit
     * order or a byte-width assumption would surface as shifted values. The pattern is deliberately
     * not alternating and spans several bytes, so an off-by-one bit would change a read value.
     */
    public void testBooleanColumnReadsBitPackedValues() throws Exception {
        int rowCount = 20; // spans 3 bytes, so the read cannot stay inside a single byte
        Path file = createTempDir().resolve("booleans.parquet");
        writeBooleanColumn(file, rowCount, -1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rowCount; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_BOOL, batch.valueKind());
                assertTrue("row " + row + " should be present", batch.isPresent(row));
                // BooleanFieldMapper stores doc values as 0 or 1, so that is what valueAt must yield.
                assertEquals("value at row " + row, expectedBoolean(row) ? 1L : 0L, batch.valueAt(row));
            }
        }
    }

    /** A null boolean has both a bit-packed values buffer and a bit-packed presence bitmap. */
    public void testBooleanColumnWithNullsUsesPresenceBitmap() throws Exception {
        int rowCount = 24;
        int nullEvery = 5;
        Path file = createTempDir().resolve("booleans-nulls.parquet");
        writeBooleanColumn(file, rowCount, nullEvery);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rowCount; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_BOOL, batch.valueKind());
                if (row % nullEvery == 0) {
                    assertFalse("row " + row + " should be null", batch.isPresent(row));
                } else {
                    assertTrue("row " + row + " should be present", batch.isPresent(row));
                    assertEquals("value at row " + row, expectedBoolean(row) ? 1L : 0L, batch.valueAt(row));
                }
            }
        }
    }

    /** Forces several batch reloads, so the borrow is re-established repeatedly for a bit-packed column. */
    public void testBooleanColumnAcrossManyBatches() throws Exception {
        int rowCount = 500;
        Path file = createTempDir().resolve("booleans-many.parquet");
        writeBooleanColumn(file, rowCount, -1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < rowCount; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_BOOL, batch.valueKind());
                assertEquals("value at row " + row, expectedBoolean(row) ? 1L : 0L, batch.valueAt(row));
            }
        }
    }

    /**
     * half_float is stored as raw fp16 bits but must come back as the sortable short Lucene holds, so
     * {@code HalfFloatPoint.sortableShortToHalfFloat} has to recover the original value. Negatives are
     * the case a missing sign flip would break, so they must sort below positives.
     */
    public void testHalfFloatColumnUsesSortableShortEncoding() throws Exception {
        float[] values = { -100.5f, -0.5f, 0.0f, 3.25f, -2.75f, 42.0f, 1.0f, -1.0f };
        Path file = createTempDir().resolve("halffloats.parquet");
        writeHalfFloatColumn(file, values);
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < values.length; row++) {
                DecodedBatch batch = loadRow(reader, row);
                assertEquals(DecodedBatch.KIND_HALF_FLOAT, batch.valueKind());
                short sortable = (short) batch.valueAt(row);
                // The sign flip is an involution, so applying it again recovers the raw fp16 bits.
                short rawBits = (short) (sortable ^ ((sortable >> 15) & 0x7fff));
                assertEquals("value at row " + row, values[row], Float.float16ToFloat(rawBits), 0.0f);
            }
            // Sign-flipped, so a negative's encoded short compares below a positive's.
            DecodedBatch batch = loadRow(reader, 0);
            assertTrue("negative half_float must sort below positive", batch.valueAt(0) < batch.valueAt(5));
        }
    }

    /**
     * Pins the half_float encoding to Lucene's, not merely to itself: the round-trip above holds
     * for ANY involution, so it cannot detect the encode drifting from what
     * {@code HalfFloatPoint.sortableShortToHalfFloat} expects. The expected shorts are
     * {@code HalfFloatPoint.halfFloatToSortableShort} outputs, hard-coded to avoid a lucene-sandbox
     * dependency: fp16 bits with the sign-flip transform applied.
     */
    public void testHalfFloatSortableShortsMatchLucene() throws Exception {
        float[] values = { 1.0f, -1.0f, 0.0f, 42.0f };
        short[] expectedSortable = { 15360, -15361, 0, 20800 };
        Path file = createTempDir().resolve("halffloat-lucene.parquet");
        writeHalfFloatColumn(file, values);
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            for (int row = 0; row < values.length; row++) {
                assertEquals("sortable short for " + values[row], expectedSortable[row], (short) loadRow(reader, row).valueAt(row));
            }
        }
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

    // Full 64-bit range coverage for KIND_LONG: the signed edges (Long.MIN_VALUE/MAX_VALUE and their
    // neighbours) must round-trip exactly. A truncating or narrowing bug on the long path would fold
    // the extremes toward zero (base class: testBigNumericRange/testZeroOrMin).
    public void testLongColumnRoundTripsFullRange() throws Exception {
        long[] values = {
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            -987654321012345L,
            -1L,
            0L,
            1L,
            987654321012345L,
            Long.MAX_VALUE - 1,
            Long.MAX_VALUE };
        Path file = createTempDir().resolve("int64-fullrange.parquet");
        writeIntegralColumn(file, new ArrowType.Int(64, true), values);
        assertIntegralColumn(file, DecodedBatch.KIND_LONG, values);
    }

    // Constant all-zeros column: every row is a present zero, not a null. Parquet uses its own encoding
    // rather than Lucene's GCD/constant compression, so this covers the all-zeros semantic (base class:
    // testZeros), not that codec path.
    public void testAllZeroLongColumnReadsBackAsZero() throws Exception {
        long[] values = new long[64]; // defaults to all zeros
        Path file = createTempDir().resolve("int64-allzero.parquet");
        writeIntegralColumn(file, new ArrowType.Int(64, true), values);
        assertIntegralColumn(file, DecodedBatch.KIND_LONG, values);
    }

    // ------------------------------------------------------------------------------------------------
    // Numeric iterator semantics: ParquetNumericDocValues over a real fixture.
    // ------------------------------------------------------------------------------------------------

    public void testFullAscendingScanAllPresent() throws Exception {
        Path file = createTempDir().resolve("scan.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            for (int doc = 0; doc < ROWS; doc++) {
                assertTrue("row " + doc + " should be present", dv.advanceExact(doc));
                assertEquals("value at row " + doc, LongColumnFixture.valueAt(doc), dv.longValue());
            }
            assertFalse("no doc at maxDoc", dv.advanceExact(ROWS));
        }
    }

    public void testNullRowsAreAbsent() throws Exception {
        Path file = createTempDir().resolve("nullable-iter.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            for (int doc = 0; doc < ROWS; doc++) {
                boolean present = dv.advanceExact(doc);
                if (doc % NULL_EVERY == 0) {
                    assertFalse("row " + doc + " should be null", present);
                } else {
                    assertTrue("row " + doc + " should be present", present);
                    assertEquals("value at row " + doc, LongColumnFixture.valueAt(doc), dv.longValue());
                }
            }
        }
    }

    public void testNextDocAndAdvanceSkipNulls() throws Exception {
        Path file = createTempDir().resolve("skip.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            // Row 0 is null, so the first live doc is 1.
            assertEquals(1, dv.nextDoc());
            assertEquals(LongColumnFixture.valueAt(1), dv.longValue());
            // advance onto a null row (200 % 5 == 0) lands on the next live doc, 201.
            assertEquals(201, dv.advance(200));
            assertEquals(LongColumnFixture.valueAt(201), dv.longValue());
            // Running off the end returns NO_MORE_DOCS.
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(ROWS));
        }
    }

    public void testAdvanceOverAllNullColumnExhausts() throws Exception {
        Path file = createTempDir().resolve("allnull.parquet");
        // nullEvery == 1 leaves every row null, so advance must drain each batch's bitmap
        // without finding a present row and land on NO_MORE_DOCS.
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(0));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        }
    }

    public void testBackwardAdvanceExactReopensCursor() throws Exception {
        Path file = createTempDir().resolve("backward.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            assertTrue(dv.advanceExact(250));
            assertEquals(LongColumnFixture.valueAt(250), dv.longValue());
            // A lower target than the current batch forces the forward-only cursor to reopen.
            assertTrue(dv.advanceExact(10));
            assertEquals(LongColumnFixture.valueAt(10), dv.longValue());
        }
    }

    /** Random increasing {@code advance} targets over a dense column land on the target itself with its value. */
    public void testRandomIncreasingAdvanceMatchesOracleDense() throws Exception {
        Path file = createTempDir().resolve("advance-dense.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            int target = randomIntBetween(0, 5);
            while (target < ROWS) {
                int expected = nextPresent(target, ROWS, 0);
                int result = dv.advance(target);
                assertEquals("advance(" + target + ")", expected, result);
                assertEquals("value at " + result, LongColumnFixture.valueAt(result), dv.longValue());
                target = result + randomIntBetween(1, 30);
            }
        }
    }

    /** Random increasing {@code advance} targets over a sparse column land on the next present row with its value. */
    public void testRandomIncreasingAdvanceMatchesOracleSparse() throws Exception {
        Path file = createTempDir().resolve("advance-sparse.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            int target = randomIntBetween(0, 5);
            while (target < ROWS) {
                int expected = nextPresent(target, ROWS, NULL_EVERY);
                int result = dv.advance(target);
                assertEquals("advance(" + target + ")", expected, result);
                assertEquals("value at " + result, LongColumnFixture.valueAt(result), dv.longValue());
                target = result + randomIntBetween(1, 30);
            }
        }
    }

    /** Large random forward gaps, and a single jump to the last doc, each land on the target across batch boundaries. */
    public void testSkipHeavyAdvanceAcrossBatchBoundaries() throws Exception {
        Path file = createTempDir().resolve("skip-heavy.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN, FIXED_WINDOW, FIXED_WINDOW)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            int doc = -1;
            while (true) {
                // A gap wider than the fixed window guarantees the target is in a later batch.
                int target = doc + randomIntBetween(FIXED_WINDOW + 1, 4 * FIXED_WINDOW);
                if (target >= ROWS) {
                    break;
                }
                assertEquals("advance(" + target + ")", target, dv.advance(target));
                assertEquals("value at " + target, LongColumnFixture.valueAt(target), dv.longValue());
                doc = target;
            }
        }

        // A single jump from the start to the last doc crosses many fixed-size batches.
        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN, FIXED_WINDOW, FIXED_WINDOW)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            assertEquals(ROWS - 1, dv.advance(ROWS - 1));
            assertEquals(LongColumnFixture.valueAt(ROWS - 1), dv.longValue());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(ROWS));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        }
    }

    /** Repeated {@code advanceExact} on the same present doc returns present every time with a stable value. */
    public void testRepeatedAdvanceExactIsStable() throws Exception {
        Path file = createTempDir().resolve("repeat.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            int doc = randomIntBetween(0, ROWS - 1);
            long expected = LongColumnFixture.valueAt(doc);
            int repeats = randomIntBetween(2, 5);
            for (int i = 0; i < repeats; i++) {
                assertTrue("repeat " + i + " on doc " + doc, dv.advanceExact(doc));
                assertEquals("stable value on repeat " + i, expected, dv.longValue());
            }
        }
    }

    /** {@code advance} at or beyond maxDoc returns NO_MORE_DOCS and pins docID() at NO_MORE_DOCS. */
    public void testAdvanceBeyondMaxDocReturnsNoMoreDocs() throws Exception {
        Path file = createTempDir().resolve("beyond.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(ROWS));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(randomIntBetween(ROWS, ROWS * 4)));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        }
    }

    /** {@code nextDoc} visits every doc in order on a dense column and ends at NO_MORE_DOCS. */
    public void testNextDocVisitsEveryDocDense() throws Exception {
        Path file = createTempDir().resolve("nextdoc-dense.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            for (int doc = 0; doc < ROWS; doc++) {
                assertEquals(doc, dv.nextDoc());
                assertEquals(LongColumnFixture.valueAt(doc), dv.longValue());
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
        }
    }

    /** {@code nextDoc} visits exactly the present docs in order on a sparse column and ends at NO_MORE_DOCS. */
    public void testNextDocVisitsPresentDocsSparse() throws Exception {
        Path file = createTempDir().resolve("nextdoc-sparse.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            int expected = nextPresent(0, ROWS, NULL_EVERY);
            while (expected != DocIdSetIterator.NO_MORE_DOCS) {
                assertEquals(expected, dv.nextDoc());
                assertEquals(LongColumnFixture.valueAt(expected), dv.longValue());
                expected = nextPresent(expected + 1, ROWS, NULL_EVERY);
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
        }
    }

    /** A null row at a batch's last position is absent, and the next present doc is found in the following batch with its value. */
    public void testAdvanceExactOnNullFindsNextPresentAcrossBatchBoundary() throws Exception {
        Path file = createTempDir().resolve("straddle.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        // Fixture nulls row r iff r % NULL_EVERY == 0; the first such r at a FIXED_WINDOW batch's last row ((r+1) % W == 0) is 15.
        int nullRow = firstNullAtBatchEnd(NULL_EVERY, FIXED_WINDOW, ROWS);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN, FIXED_WINDOW, FIXED_WINDOW)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            // Walk from the start so the resident batch is [0, W-1] and nullRow is its last row, not a seek-aligned first row.
            assertEquals(nextPresent(0, ROWS, NULL_EVERY), dv.advance(0));
            assertEquals("nullRow must be the resident batch's last row", nullRow, (int) reader.decodedBatch().lastRow());

            assertFalse("null row must be absent", dv.advanceExact(nullRow));

            int acrossBoundary = dv.advance(nullRow + 1);
            assertEquals("next present doc must match the oracle", nextPresent(nullRow + 1, ROWS, NULL_EVERY), acrossBoundary);
            assertTrue("next present doc must be past the null row", acrossBoundary > nullRow);
            assertTrue("next present doc must lie in a later batch", reader.decodedBatch().firstRow() > nullRow);
            assertEquals(LongColumnFixture.valueAt(acrossBoundary), dv.longValue());
        }
    }

    /** advanceExact then longValue returns the fixture value for random present docs on a sparse column. */
    public void testAdvanceExactThenLongValueMatchesOracle() throws Exception {
        Path file = createTempDir().resolve("present-values.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(reader, ROWS);
            int doc = 0;
            int probes = randomIntBetween(5, 15);
            for (int i = 0; i < probes; i++) {
                int present = nextPresent(doc, ROWS, NULL_EVERY);
                if (present == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
                assertTrue("present doc " + present, dv.advanceExact(present));
                assertEquals("value at " + present, LongColumnFixture.valueAt(present), dv.longValue());
                doc = present + randomIntBetween(1, 20);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Producer lifecycle / no-leak. Native FFM cursors are invisible to the base class's MockDirectory
    // leak gate, so the cursor close chain is asserted directly.
    // ------------------------------------------------------------------------------------------------

    /**
     * Every accessor call hands out a fresh cursor (dedicated per consumer), and closing the request's
     * registry closes exactly those cursors and leaves the shared producer open. Close is idempotent.
     */
    public void testRequestEndClosesOnlyItsCursorsNotTheProducer() throws Exception {
        int rows = 200;
        Path file = createTempDir().resolve("lifecycle.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1);

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);
        FieldInfo fi = sortedNumericField(COLUMN);

        CursorRegistry request = new CursorRegistry();
        SortedNumericDocValues first = producer.getSortedNumeric(fi, request);
        producer.getSortedNumeric(fi, request);

        List<ParquetColumnReader> opened = request.opened();
        assertEquals("each accessor call opens its own cursor", 2, opened.size());
        assertNotSame("cursors must be dedicated per consumer", opened.get(0), opened.get(1));

        // The cursor reads real values off the fixture before the request ends.
        NumericDocValues single = DocValues.unwrapSingleton(first);
        assertTrue(single.advanceExact(10));
        assertEquals(LongColumnFixture.valueAt(10), single.longValue());

        for (ParquetColumnReader cursor : opened) {
            assertFalse("cursor must be open mid-request", cursor.isClosed());
        }

        request.close();

        for (ParquetColumnReader cursor : opened) {
            assertTrue("every request cursor must be closed at request end", cursor.isClosed());
        }
        assertFalse("producer must outlive the request", producer.isClosed());

        request.close(); // idempotent
    }

    /**
     * Two sequential requests over the same segment core reuse the one cached producer but receive
     * independent cursors, and closing one request does not disturb the other's cursor.
     */
    public void testSequentialRequestsReuseProducerButGetDistinctCursors() throws Exception {
        int rows = 200;
        Path file = createTempDir().resolve("reuse.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1);

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);
        FieldInfo fi = sortedNumericField(COLUMN);

        CursorRegistry firstRequest = new CursorRegistry();
        producer.getSortedNumeric(fi, firstRequest);
        ParquetColumnReader firstCursor = firstRequest.opened().get(0);

        CursorRegistry secondRequest = new CursorRegistry();
        producer.getSortedNumeric(fi, secondRequest);
        ParquetColumnReader secondCursor = secondRequest.opened().get(0);

        assertNotSame("sequential requests must not share a cursor", firstCursor, secondCursor);

        firstRequest.close();
        assertTrue("first request's cursor must close with it", firstCursor.isClosed());
        assertFalse("second request's cursor must be untouched", secondCursor.isClosed());

        secondRequest.close();
        assertTrue(secondCursor.isClosed());
    }

    /**
     * Many threads reading the same column at once each receive their own dedicated cursor from the one
     * shared request registry (which is why that registry is synchronized), and every thread reads the
     * full dense column correctly. The cursor is per-consumer and forward-only, so correctness depends
     * on each getSortedNumeric call handing out an independent cursor rather than sharing one (base
     * class: testThreads*). Worker-thread assertions are collected and re-asserted on the test thread so
     * a failure is not swallowed.
     */
    public void testConcurrentReadersOverOneColumnEachSeeFullColumn() throws Exception {
        int rows = 300;
        Path file = createTempDir().resolve("concurrent.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1); // dense: every doc present

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);
        FieldInfo fi = sortedNumericField(COLUMN);
        CursorRegistry request = new CursorRegistry();

        int threadCount = 8;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(threadCount);
        List<Throwable> failures = new CopyOnWriteArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            Thread worker = new Thread(() -> {
                try {
                    startGate.await(); // release all readers together to maximise contention
                    SortedNumericDocValues dv = producer.getSortedNumeric(fi, request);
                    NumericDocValues single = DocValues.unwrapSingleton(dv);
                    for (int doc = 0; doc < rows; doc++) {
                        assertTrue("row " + doc + " present", single.advanceExact(doc));
                        assertEquals("value at row " + doc, LongColumnFixture.valueAt(doc), single.longValue());
                    }
                } catch (Throwable e) {
                    failures.add(e);
                } finally {
                    doneGate.countDown();
                }
            }, "parquet-dv-reader-" + t);
            worker.start();
        }

        startGate.countDown();
        doneGate.await();

        assertTrue("no reader thread should fail: " + failures, failures.isEmpty());
        assertEquals("each thread opened its own dedicated cursor", threadCount, request.opened().size());
        for (ParquetColumnReader cursor : request.opened()) {
            assertFalse("cursors stay open until the request closes", cursor.isClosed());
        }

        request.close();
        for (ParquetColumnReader cursor : request.opened()) {
            assertTrue("request close closes every cursor opened by every thread", cursor.isClosed());
        }
    }

    /**
     * A cursor whose open fails leaves the registry holding exactly the cursors opened before it, and
     * the request still closes those at its end.
     */
    public void testFailedCursorOpenLeavesEarlierCursorsRegisteredAndClosable() throws Exception {
        int rows = 200;
        Path file = createTempDir().resolve("openfail.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1);

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);

        CursorRegistry request = new CursorRegistry();
        producer.getSortedNumeric(sortedNumericField(COLUMN), request);
        List<ParquetColumnReader> afterGood = request.opened();
        assertEquals("the good field opens one cursor", 1, afterGood.size());

        // The column is absent from the file, so ParquetColumnReader.open throws before registering.
        FieldInfo missing = sortedNumericField("no_such_column");
        expectThrows(IOException.class, () -> producer.getSortedNumeric(missing, request));

        List<ParquetColumnReader> afterFailure = request.opened();
        assertEquals("a failed open registers no cursor", 1, afterFailure.size());
        assertSame("the surviving cursor is the one opened before the failure", afterGood.get(0), afterFailure.get(0));

        ParquetColumnReader survivor = afterFailure.get(0);
        assertFalse("the survivor stays open until request end", survivor.isClosed());
        request.close();
        assertTrue("request close still closes the survivor", survivor.isClosed());
    }

    /**
     * The producer's singleton view reports exactly one value per present doc, equal to the fixture's valueAt.
     */
    public void testSingletonViewReportsOneValueEqualToLongValue() throws Exception {
        int rows = 200;
        Path file = createTempDir().resolve("singleton.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1); // dense column: every doc is present

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);

        CursorRegistry request = new CursorRegistry();
        SortedNumericDocValues singleton = producer.getSortedNumeric(sortedNumericField(COLUMN), request);
        try {
            int doc = randomIntBetween(0, 20);
            while (doc < rows) {
                assertTrue("doc " + doc + " is present on a dense column", singleton.advanceExact(doc));
                assertEquals("a singleton has exactly one value per present doc", 1, singleton.docValueCount());
                assertEquals("value at " + doc, LongColumnFixture.valueAt(doc), singleton.nextValue());
                doc += randomIntBetween(1, 40);
            }
        } finally {
            request.close();
        }
    }

    /**
     * A second wrap over the same segment core reuses the same {@link ParquetSegmentResources} instance and
     * caches no additional entry; the resources built on the first wrap are not rebuilt.
     */
    public void testSecondWrapOverSameCoreReusesResources() throws Exception {
        ParquetDocValuesProducer producer = newFixtureProducer("identity.parquet");
        ParquetSegmentResources resources = new ParquetSegmentResources(producer, Map.of(), new FieldInfos(new FieldInfo[0]));

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        ParquetSegmentResourceCache cache = new ParquetSegmentResourceCache(null);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();

            int before = cache.size();
            ParquetSegmentResources first = cache.cacheForTesting(leaf, resources);
            assertSame("first wrap installs the resources", resources, first);
            assertEquals(before + 1, cache.size());

            // The second wrap passes distinct resources, but the cache returns the first instance and
            // records nothing new: the per-core resources are resolved once.
            ParquetSegmentResources second = cache.cacheForTesting(
                leaf,
                new ParquetSegmentResources(producer, Map.of(), new FieldInfos(new FieldInfo[0]))
            );
            assertSame("second wrap over the same core reuses the same resources instance", resources, second);
            assertEquals("second wrap adds no cache entry", before + 1, cache.size());
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /**
     * The cache holds one resources instance per core cache key and closes its producer from the core's
     * closed-listener: closing the segment reader that owns the core closes the producer and drops the
     * entry.
     */
    public void testSegmentCoreCloseClosesTheProducer() throws Exception {
        ParquetDocValuesProducer producer = newFixtureProducer("core.parquet");
        ParquetSegmentResources resources = new ParquetSegmentResources(producer, Map.of(), new FieldInfos(new FieldInfo[0]));

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        ParquetSegmentResourceCache cache = new ParquetSegmentResourceCache(null);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNotNull("segment leaf must expose a core cache helper", leaf.getCoreCacheHelper());

            int before = cache.size();
            cache.cacheForTesting(leaf, resources);
            assertEquals(before + 1, cache.size());
            assertFalse(producer.isClosed());

            reader.close(); // drops the core -> fires the closed-listener
            assertTrue("core close must close the producer", producer.isClosed());
            assertEquals("closed resources must be dropped from the cache", before, cache.size());
        } finally {
            if (reader.getRefCount() > 0) {
                reader.close();
            }
            writer.close();
            dir.close();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Unsupported doc-values kinds. The format serves only SORTED_NUMERIC; the base DocValuesFormat
    // matrix also exercises SORTED, SORTED_SET and BINARY. The producer is asserted to reject them.
    // ------------------------------------------------------------------------------------------------

    public void testSortedIsUnsupported() {
        ParquetDocValuesProducer producer = newFixtureProducer("unsupported-sorted.parquet");
        expectThrows(UnsupportedOperationException.class, () -> producer.getSorted(sortedNumericField(COLUMN)));
    }

    public void testSortedSetIsUnsupported() {
        ParquetDocValuesProducer producer = newFixtureProducer("unsupported-sortedset.parquet");
        expectThrows(UnsupportedOperationException.class, () -> producer.getSortedSet(sortedNumericField(COLUMN)));
    }

    public void testBinaryIsUnsupported() {
        ParquetDocValuesProducer producer = newFixtureProducer("unsupported-binary.parquet");
        expectThrows(UnsupportedOperationException.class, () -> producer.getBinary(sortedNumericField(COLUMN)));
    }

    /** Everything is SORTED_NUMERIC, so the plain NumericDocValues accessor is never valid for a served field. */
    public void testNumericIsUnsupported() {
        ParquetDocValuesProducer producer = newFixtureProducer("unsupported-numeric.parquet");
        expectThrows(UnsupportedOperationException.class, () -> producer.getNumeric(sortedNumericField(COLUMN)));
    }

    /** The registry-less accessor is fenced off so a cursor can never be opened without a request-scoped owner to close it. */
    public void testSortedNumericWithoutRegistryIsUnsupported() {
        ParquetDocValuesProducer producer = newFixtureProducer("unsupported-noregistry.parquet");
        expectThrows(UnsupportedOperationException.class, () -> producer.getSortedNumeric(sortedNumericField(COLUMN)));
    }

    // ------------------------------------------------------------------------------------------------
    // Oracles and fixtures.
    // ------------------------------------------------------------------------------------------------

    /** First present row at or after {@code from}, or NO_MORE_DOCS; {@code nullEvery <= 0} means every row is present. */
    private static int nextPresent(int from, int maxDoc, int nullEvery) {
        for (int d = from; d < maxDoc; d++) {
            if (nullEvery <= 0 || d % nullEvery != 0) {
                return d;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /** First null row {@code > 0} that is also a window's last row ({@code (r+1) % window == 0}); fixture nulls row r iff r % nullEvery == 0. */
    private static int firstNullAtBatchEnd(int nullEvery, int window, int maxDoc) {
        for (int r = 1; r < maxDoc; r++) {
            if (r % nullEvery == 0 && (r + 1) % window == 0) {
                return r;
            }
        }
        throw new AssertionError("no null row lands on a batch boundary for nullEvery=" + nullEvery + " window=" + window);
    }

    private static DecodedBatch loadRow(ParquetColumnReader reader, long row) throws java.io.IOException {
        DecodedBatch batch = reader.decodedBatch();
        if (batch == null || batch.contains(row) == false) {
            reader.loadBatchContaining(row);
            batch = reader.decodedBatch();
        }
        return batch;
    }

    /** Deliberately not an alternating pattern, so a shifted bit read changes a value. */
    private static boolean expectedBoolean(long row) {
        return row % 3 == 0 || row % 7 == 2;
    }

    private ParquetDocValuesProducer newFixtureProducer(String name) {
        return new ParquetDocValuesProducer(createTempDir().resolve(name), ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, 1, null);
    }

    private static IndexWriter singleDocWriter(Directory dir) throws Exception {
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("id", "1", org.apache.lucene.document.Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        return writer;
    }

    /** A synthetic SORTED_NUMERIC field info, matching what the resources builder synthesizes. */
    private static FieldInfo sortedNumericField(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED_NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
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

    private void writeBooleanColumn(Path file, int rowCount, int nullEvery) throws Exception {
        FieldType fieldType = nullEvery > 0 ? FieldType.nullable(new ArrowType.Bool()) : FieldType.notNullable(new ArrowType.Bool());
        Schema schema = new Schema(List.of(new Field(COLUMN, fieldType, null)));
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BitVector vector = (BitVector) root.getVector(COLUMN);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nullEvery > 0 && i % nullEvery == 0) {
                    vector.setNull(i);
                } else {
                    vector.setSafe(i, expectedBoolean(i) ? 1 : 0);
                }
            }
            vector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            try (ArrowExport dataExport = new ArrowExport(array, arrowSchema)) {
                writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
            }
        }
        writer.flush();
    }

    private void writeHalfFloatColumn(Path file, float[] values) throws Exception {
        Schema schema = new Schema(
            List.of(new Field(COLUMN, FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)), null))
        );
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("test-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            Float2Vector vector = (Float2Vector) root.getVector(COLUMN);
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.setSafeWithPossibleTruncate(i, values[i]);
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
     * Stores {@code value} into the width-specific Arrow vector. The 32/16/8-bit vectors expose
     * {@code setSafe(int, int)}, so the {@code (int)} cast carries the raw low bits - the unsigned
     * vectors reinterpret those bits without sign, which is exactly what the read path must reverse.
     * The 64-bit {@link BigIntVector} takes the full {@code long} unchanged.
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
        } else if (vector instanceof BigIntVector v) {
            v.setSafe(index, value);
        } else {
            throw new IllegalArgumentException("unsupported integral vector " + vector.getClass().getName());
        }
    }
}
