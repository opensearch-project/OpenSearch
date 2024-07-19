/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeDocValuesIteratorAdapterTests extends OpenSearchTestCase {

    private StarTreeDocValuesIteratorAdapter adapter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        adapter = new StarTreeDocValuesIteratorAdapter();
    }

    public void testGetDocValuesIterator() throws IOException {
        DocValuesProducer mockProducer = mock(DocValuesProducer.class);
        SortedNumericDocValues mockSortedNumericDocValues = mock(SortedNumericDocValues.class);

        when(mockProducer.getSortedNumeric(any())).thenReturn(mockSortedNumericDocValues);

        SequentialDocValuesIterator iterator = adapter.getDocValuesIterator(DocValuesType.SORTED_NUMERIC, any(), mockProducer);

        assertNotNull(iterator);
        assertEquals(mockSortedNumericDocValues, iterator.getDocIdSetIterator());
    }

    public void testGetDocValuesIteratorWithUnsupportedType() {
        DocValuesProducer mockProducer = mock(DocValuesProducer.class);
        FieldInfo fieldInfo = new FieldInfo(
            "random_field",
            0,
            false,
            false,
            true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
            DocValuesType.SORTED_NUMERIC,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            adapter.getDocValuesIterator(DocValuesType.BINARY, fieldInfo, mockProducer);
        });

        assertEquals("Unsupported DocValuesType: BINARY", exception.getMessage());
    }

    public void testGetNextValue() throws IOException {
        SortedNumericDocValues mockSortedNumericDocValues = mock(SortedNumericDocValues.class);
        SequentialDocValuesIterator iterator = new SequentialDocValuesIterator(mockSortedNumericDocValues);
        iterator.setDocId(1);
        when(mockSortedNumericDocValues.nextValue()).thenReturn(42L);

        Long nextValue = adapter.getNextValue(iterator, 1);

        assertEquals(Long.valueOf(42L), nextValue);
        assertEquals(Long.valueOf(42L), iterator.getDocValue());
    }

    public void testGetNextValueWithInvalidDocId() {
        SortedNumericDocValues mockSortedNumericDocValues = mock(SortedNumericDocValues.class);
        SequentialDocValuesIterator iterator = new SequentialDocValuesIterator(mockSortedNumericDocValues);
        iterator.setDocId(DocIdSetIterator.NO_MORE_DOCS);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> { adapter.getNextValue(iterator, 1); });

        assertEquals("invalid doc id to fetch the next value", exception.getMessage());
    }

    public void testGetNextValueWithUnsupportedIterator() {
        DocIdSetIterator mockIterator = mock(DocIdSetIterator.class);
        SequentialDocValuesIterator iterator = new SequentialDocValuesIterator(mockIterator);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> { adapter.getNextValue(iterator, 1); });

        assertEquals("Unsupported Iterator: " + mockIterator.toString(), exception.getMessage());
    }

    public void testNextDoc() throws IOException {
        SortedNumericDocValues mockSortedNumericDocValues = mock(SortedNumericDocValues.class);
        SequentialDocValuesIterator iterator = new SequentialDocValuesIterator(mockSortedNumericDocValues);
        when(mockSortedNumericDocValues.nextDoc()).thenReturn(2, 3, DocIdSetIterator.NO_MORE_DOCS);
        when(mockSortedNumericDocValues.nextValue()).thenReturn(42L, 32L);

        int nextDocId = adapter.nextDoc(iterator, 1);
        assertEquals(2, nextDocId);
        assertEquals(Long.valueOf(42L), adapter.getNextValue(iterator, nextDocId));

        nextDocId = adapter.nextDoc(iterator, 2);
        assertEquals(3, nextDocId);
        when(mockSortedNumericDocValues.nextValue()).thenReturn(42L, 32L);

    }

    public void testNextDoc_noMoreDocs() throws IOException {
        SortedNumericDocValues mockSortedNumericDocValues = mock(SortedNumericDocValues.class);
        SequentialDocValuesIterator iterator = new SequentialDocValuesIterator(mockSortedNumericDocValues);
        when(mockSortedNumericDocValues.nextDoc()).thenReturn(2, DocIdSetIterator.NO_MORE_DOCS);
        when(mockSortedNumericDocValues.nextValue()).thenReturn(42L, 32L);

        int nextDocId = adapter.nextDoc(iterator, 1);
        assertEquals(2, nextDocId);
        assertEquals(Long.valueOf(42L), adapter.getNextValue(iterator, nextDocId));

        assertThrows(IllegalStateException.class, () -> adapter.nextDoc(iterator, 2));

    }
}
