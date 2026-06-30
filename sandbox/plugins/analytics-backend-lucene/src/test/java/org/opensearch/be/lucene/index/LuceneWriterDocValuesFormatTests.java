/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link LuceneWriterDocValuesFormat} — verifies that the row ID rewriting
 * logic correctly produces sequential values and delegates non-row-ID fields unchanged.
 */
public class LuceneWriterDocValuesFormatTests extends OpenSearchTestCase {

    private static final String ROW_ID_FIELD = LuceneDocumentInput.ROW_ID_FIELD;

    public void testSequentialRowIdDocValuesProducesCorrectValues() {
        int maxDoc = randomIntBetween(5, 100);
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues = new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);

        // Test advanceExact
        for (int i = 0; i < maxDoc; i++) {
            assertTrue(docValues.advanceExact(i));
            assertEquals(i, docValues.docID());
            assertEquals(1, docValues.docValueCount());
            assertEquals(i, docValues.nextValue());
        }
    }

    public void testSequentialRowIdDocValuesNextDoc() {
        int maxDoc = randomIntBetween(5, 50);
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues = new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);

        for (int i = 0; i < maxDoc; i++) {
            int doc = docValues.nextDoc();
            assertEquals(i, doc);
            assertEquals(i, docValues.nextValue());
        }
        assertEquals(SortedNumericDocValues.NO_MORE_DOCS, docValues.nextDoc());
    }

    public void testSequentialRowIdDocValuesAdvance() {
        int maxDoc = 20;
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues = new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);

        // Advance to middle
        int target = 10;
        assertEquals(target, docValues.advance(target));
        assertEquals(target, docValues.nextValue());

        // Advance past end
        assertEquals(SortedNumericDocValues.NO_MORE_DOCS, docValues.advance(maxDoc));
    }

    public void testSequentialRowIdDocValuesCost() {
        int maxDoc = randomIntBetween(1, 1000);
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues = new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);
        assertEquals(maxDoc, docValues.cost());
    }

    public void testSequentialRowIdProducerReturnsSortedNumeric() {
        int maxDoc = 10;
        LuceneWriterDocValuesFormat.SequentialRowIdProducer producer = new LuceneWriterDocValuesFormat.SequentialRowIdProducer(maxDoc);

        SortedNumericDocValues values = producer.getSortedNumeric(null);
        assertNotNull(values);

        // Other methods return null
        assertNull(producer.getNumeric(null));
        assertNull(producer.getBinary(null));
        assertNull(producer.getSorted(null));
        assertNull(producer.getSortedSet(null));
        assertNull(producer.getSkipper(null));
    }

    public void testGetDelegateReturnsWrappedFormat() {
        DocValuesFormat defaultFormat = Codec.getDefault().docValuesFormat();
        LuceneWriterDocValuesFormat format = new LuceneWriterDocValuesFormat(defaultFormat);
        assertSame(defaultFormat, format.getDelegate());
    }
}
