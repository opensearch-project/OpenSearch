/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LiveDocsFilteredDocValuesProducer} in both remap and skip-only modes.
 */
public class LiveDocsFilteredDocValuesProducerTests extends OpenSearchTestCase {

    // --- Remap Mode Tests ---

    public void testRemapMode_allDocsLive() throws IOException {
        // 5 docs, all live
        int maxDoc = 5;
        FixedBitSet liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc); // all bits set

        long[] values = { 10, 20, 30, 40, 50 };
        DocValuesProducer delegate = createSortedNumericProducer(values);
        FieldInfo fieldInfo = createFieldInfo("price");

        LiveDocsFilteredDocValuesProducer filtered = new LiveDocsFilteredDocValuesProducer(delegate, liveDocs, maxDoc);
        SortedNumericDocValues dv = filtered.getSortedNumeric(fieldInfo);

        // All 5 docs should be accessible with same IDs
        for (int i = 0; i < 5; i++) {
            assertTrue("advanceExact(" + i + ") should succeed", dv.advanceExact(i));
            assertEquals(values[i], dv.nextValue());
        }
        filtered.close();
    }

    public void testRemapMode_someDocsDeleted() throws IOException {
        // 10 docs, docs 2, 5, 7 deleted → 7 live docs with remapped IDs 0-6
        int maxDoc = 10;
        FixedBitSet liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);
        liveDocs.clear(2);
        liveDocs.clear(5);
        liveDocs.clear(7);

        // Original values for docs 0-9
        long[] values = { 100, 110, 120, 130, 140, 150, 160, 170, 180, 190 };
        DocValuesProducer delegate = createSortedNumericProducer(values);
        FieldInfo fieldInfo = createFieldInfo("price");

        LiveDocsFilteredDocValuesProducer filtered = new LiveDocsFilteredDocValuesProducer(delegate, liveDocs, maxDoc);
        SortedNumericDocValues dv = filtered.getSortedNumeric(fieldInfo);

        // Remapped: new doc 0 → original 0, new doc 1 → original 1, new doc 2 → original 3 (skipped 2)
        // new doc 3 → original 4, new doc 4 → original 6 (skipped 5), new doc 5 → original 8 (skipped 7)
        // new doc 6 → original 9
        long[] expectedValues = { 100, 110, 130, 140, 160, 180, 190 };
        for (int i = 0; i < 7; i++) {
            assertTrue("advanceExact(" + i + ") should succeed", dv.advanceExact(i));
            assertEquals("value mismatch at remapped doc " + i, expectedValues[i], dv.nextValue());
        }
        // Doc 7 should not exist (only 7 live docs)
        assertFalse("advanceExact(7) should fail — only 7 live docs", dv.advanceExact(7));
        filtered.close();
    }

    public void testRemapMode_firstAndLastDeleted() throws IOException {
        // 5 docs, first and last deleted
        int maxDoc = 5;
        FixedBitSet liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);
        liveDocs.clear(0);
        liveDocs.clear(4);

        long[] values = { 10, 20, 30, 40, 50 };
        DocValuesProducer delegate = createSortedNumericProducer(values);
        FieldInfo fieldInfo = createFieldInfo("price");

        LiveDocsFilteredDocValuesProducer filtered = new LiveDocsFilteredDocValuesProducer(delegate, liveDocs, maxDoc);
        SortedNumericDocValues dv = filtered.getSortedNumeric(fieldInfo);

        // 3 live docs: original 1, 2, 3 → remapped 0, 1, 2
        assertTrue(dv.advanceExact(0));
        assertEquals(20, dv.nextValue());
        assertTrue(dv.advanceExact(1));
        assertEquals(30, dv.nextValue());
        assertTrue(dv.advanceExact(2));
        assertEquals(40, dv.nextValue());
        assertFalse(dv.advanceExact(3));
        filtered.close();
    }

    // --- Skip-Only Mode Tests ---

    public void testSkipOnlyMode_someDocsDeleted() throws IOException {
        // 10 docs, docs 2, 5, 7 deleted
        int maxDoc = 10;
        FixedBitSet liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);
        liveDocs.clear(2);
        liveDocs.clear(5);
        liveDocs.clear(7);

        long[] values = { 100, 110, 120, 130, 140, 150, 160, 170, 180, 190 };
        DocValuesProducer delegate = createSequentialSortedNumericProducer(values);
        FieldInfo fieldInfo = createFieldInfo("price");

        // Skip-only mode: no maxDoc parameter
        LiveDocsFilteredDocValuesProducer filtered = new LiveDocsFilteredDocValuesProducer(delegate, liveDocs);
        SortedNumericDocValues dv = filtered.getSortedNumeric(fieldInfo);

        // nextDoc() should skip docs 2, 5, 7
        int[] expectedDocs = { 0, 1, 3, 4, 6, 8, 9 };
        long[] expectedVals = { 100, 110, 130, 140, 160, 180, 190 };
        for (int i = 0; i < expectedDocs.length; i++) {
            int doc = dv.nextDoc();
            assertEquals("wrong doc at position " + i, expectedDocs[i], doc);
            assertEquals("wrong value at doc " + doc, expectedVals[i], dv.nextValue());
        }
        assertEquals(NO_MORE_DOCS, dv.nextDoc());
        filtered.close();
    }

    public void testSkipOnlyMode_consecutiveDeletes() throws IOException {
        // 8 docs, docs 3, 4, 5 deleted (consecutive)
        int maxDoc = 8;
        FixedBitSet liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);
        liveDocs.clear(3);
        liveDocs.clear(4);
        liveDocs.clear(5);

        long[] values = { 10, 20, 30, 40, 50, 60, 70, 80 };
        DocValuesProducer delegate = createSequentialSortedNumericProducer(values);
        FieldInfo fieldInfo = createFieldInfo("quantity");

        LiveDocsFilteredDocValuesProducer filtered = new LiveDocsFilteredDocValuesProducer(delegate, liveDocs);
        SortedNumericDocValues dv = filtered.getSortedNumeric(fieldInfo);

        // Should get docs 0, 1, 2, 6, 7 (skipping 3, 4, 5)
        assertEquals(0, dv.nextDoc());
        assertEquals(10, dv.nextValue());
        assertEquals(1, dv.nextDoc());
        assertEquals(20, dv.nextValue());
        assertEquals(2, dv.nextDoc());
        assertEquals(30, dv.nextValue());
        assertEquals(6, dv.nextDoc());
        assertEquals(70, dv.nextValue());
        assertEquals(7, dv.nextDoc());
        assertEquals(80, dv.nextValue());
        assertEquals(NO_MORE_DOCS, dv.nextDoc());
        filtered.close();
    }

    public void testSkipOnlyMode_allDocsLive() throws IOException {
        int maxDoc = 5;
        FixedBitSet liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);

        long[] values = { 1, 2, 3, 4, 5 };
        DocValuesProducer delegate = createSequentialSortedNumericProducer(values);
        FieldInfo fieldInfo = createFieldInfo("x");

        LiveDocsFilteredDocValuesProducer filtered = new LiveDocsFilteredDocValuesProducer(delegate, liveDocs);
        SortedNumericDocValues dv = filtered.getSortedNumeric(fieldInfo);

        for (int i = 0; i < 5; i++) {
            assertEquals(i, dv.nextDoc());
            assertEquals(values[i], dv.nextValue());
        }
        assertEquals(NO_MORE_DOCS, dv.nextDoc());
        filtered.close();
    }

    // --- Test Helpers ---

    /**
     * Creates a DocValuesProducer that supports advanceExact() (for remap mode).
     */
    private DocValuesProducer createSortedNumericProducer(long[] values) {
        return new DocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                return new SortedNumericDocValues() {
                    private int doc = -1;

                    @Override
                    public boolean advanceExact(int target) {
                        if (target >= 0 && target < values.length) {
                            doc = target;
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public int nextDoc() { return NO_MORE_DOCS; }

                    @Override
                    public long nextValue() { return values[doc]; }

                    @Override
                    public int docValueCount() { return 1; }

                    @Override
                    public int docID() { return doc; }

                    @Override
                    public int advance(int target) { return NO_MORE_DOCS; }

                    @Override
                    public long cost() { return values.length; }
                };
            }

            @Override
            public NumericDocValues getNumeric(FieldInfo field) { return null; }

            @Override
            public org.apache.lucene.index.BinaryDocValues getBinary(FieldInfo field) { return null; }

            @Override
            public org.apache.lucene.index.SortedDocValues getSorted(FieldInfo field) { return null; }

            @Override
            public org.apache.lucene.index.SortedSetDocValues getSortedSet(FieldInfo field) { return null; }

            @Override
            public void checkIntegrity() {}

            @Override
            public DocValuesSkipper getSkipper(FieldInfo field) { return null; }

            @Override
            public void close() {}
        };
    }

    /**
     * Creates a DocValuesProducer that only supports sequential nextDoc() (for skip-only mode).
     */
    private DocValuesProducer createSequentialSortedNumericProducer(long[] values) {
        return new DocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                return new SortedNumericDocValues() {
                    private int doc = -1;

                    @Override
                    public int nextDoc() {
                        doc++;
                        return doc < values.length ? doc : NO_MORE_DOCS;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        throw new UnsupportedOperationException("sequential only");
                    }

                    @Override
                    public long nextValue() { return values[doc]; }

                    @Override
                    public int docValueCount() { return 1; }

                    @Override
                    public int docID() { return doc; }

                    @Override
                    public int advance(int target) { return NO_MORE_DOCS; }

                    @Override
                    public long cost() { return values.length; }
                };
            }

            @Override
            public NumericDocValues getNumeric(FieldInfo field) { return null; }

            @Override
            public org.apache.lucene.index.BinaryDocValues getBinary(FieldInfo field) { return null; }

            @Override
            public org.apache.lucene.index.SortedDocValues getSorted(FieldInfo field) { return null; }

            @Override
            public org.apache.lucene.index.SortedSetDocValues getSortedSet(FieldInfo field) { return null; }

            @Override
            public void checkIntegrity() {}

            @Override
            public DocValuesSkipper getSkipper(FieldInfo field) { return null; }

            @Override
            public void close() {}
        };
    }

    private FieldInfo createFieldInfo(String name) {
        return new FieldInfo(
            name, 0, false, false, false,
            IndexOptions.NONE, DocValuesType.SORTED_NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(), 0, 0, 0, 0,
            VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false, false
        );
    }
}
