/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.be.datafusion.docvalues.LongColumnFixture;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;

import java.nio.file.Path;

/**
 * Random-access and DocValues-contract coverage for {@link ParquetNumericDocValues} over a real
 * Parquet fixture, checking every result against a from-scratch oracle of the fixture's values and
 * null pattern.
 *
 * <p>The cursor's default decode window starts at 32 rows and grows adaptively to at most 8192
 * (DatafusionSettings defaults). Opening a reader with {@code initialBatchSize == maxBatchSize == W}
 * pins every batch to exactly {@code W} rows, so a gap wider than {@code W} necessarily spans a batch
 * boundary; the boundary tests below rely on that.
 */
public class ParquetNumericDocValuesRandomAccessTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";
    private static final int ROWS = 300;
    private static final int NULL_EVERY = 5;
    /** Fixed decode window for the boundary-sensitive tests: any gap above this crosses a batch. */
    private static final int FIXED_WINDOW = 16;

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

    /** First null row {@code > 0} that is also a window's last row ({@code (r+1) % window == 0}); fixture nulls row r iff r % nullEvery == 0. */
    private static int firstNullAtBatchEnd(int nullEvery, int window, int maxDoc) {
        for (int r = 1; r < maxDoc; r++) {
            if (r % nullEvery == 0 && (r + 1) % window == 0) {
                return r;
            }
        }
        throw new AssertionError("no null row lands on a batch boundary for nullEvery=" + nullEvery + " window=" + window);
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

    /** First present row at or after {@code from}, or NO_MORE_DOCS; {@code nullEvery <= 0} means every row is present. */
    private static int nextPresent(int from, int maxDoc, int nullEvery) {
        for (int d = from; d < maxDoc; d++) {
            if (nullEvery <= 0 || d % nullEvery != 0) {
                return d;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }
}
