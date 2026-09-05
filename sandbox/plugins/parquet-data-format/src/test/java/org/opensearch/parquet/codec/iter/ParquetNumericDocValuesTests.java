/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.parquet.codec.LongColumnFixture;
import org.opensearch.parquet.codec.bridge.DataFusionBackedTestCase;
import org.opensearch.parquet.codec.bridge.ParquetColumnReader;

import java.nio.file.Path;

/**
 * Drives {@link ParquetNumericDocValues} over a real Parquet fixture read through the native cursor,
 * covering the ascending hot path, null handling, forward jumps, {@code nextDoc}, and the backward
 * {@code advanceExact} that reopens the forward-only cursor.
 */
public class ParquetNumericDocValuesTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";
    private static final int ROWS = 300;
    private static final int NULL_EVERY = 5;

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
        Path file = createTempDir().resolve("nullable.parquet");
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
}
