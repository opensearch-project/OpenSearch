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
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class StarTreeValuesIteratorFactoryTests extends OpenSearchTestCase {

    private static StarTreeDocValuesIteratorAdapter starTreeDocValuesIteratorAdapter;
    private static FieldInfo mockFieldInfo;

    @BeforeClass
    public static void setup() {
        starTreeDocValuesIteratorAdapter = new StarTreeDocValuesIteratorAdapter();
        mockFieldInfo = new FieldInfo(
            "field",
            1,
            false,
            false,
            true,
            IndexOptions.NONE,
            DocValuesType.NONE,
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
    }

    public void testCreateIterator_SortedNumeric() throws IOException {
        DocValuesProducer producer = Mockito.mock(DocValuesProducer.class);
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        when(producer.getSortedNumeric(mockFieldInfo)).thenReturn(iterator);
        SequentialDocValuesIterator result = starTreeDocValuesIteratorAdapter.getDocValuesIterator(
            DocValuesType.SORTED_NUMERIC,
            mockFieldInfo,
            producer
        );
        assertEquals(iterator.getClass(), result.getDocIdSetIterator().getClass());
    }

    public void testCreateIterator_UnsupportedType() {
        DocValuesProducer producer = Mockito.mock(DocValuesProducer.class);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            starTreeDocValuesIteratorAdapter.getDocValuesIterator(DocValuesType.BINARY, mockFieldInfo, producer);
        });
        assertEquals("Unsupported DocValuesType: BINARY", exception.getMessage());
    }

    public void testGetNextValue_SortedNumeric() throws IOException {
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        when(iterator.nextDoc()).thenReturn(0);
        when(iterator.nextValue()).thenReturn(123L);
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator(iterator);
        sequentialDocValuesIterator.getDocIdSetIterator().nextDoc();
        long result = starTreeDocValuesIteratorAdapter.getNextValue(sequentialDocValuesIterator, 0);
        assertEquals(123L, result);
    }

    public void testGetNextValue_UnsupportedIterator() {
        DocIdSetIterator iterator = Mockito.mock(DocIdSetIterator.class);
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator(iterator);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            starTreeDocValuesIteratorAdapter.getNextValue(sequentialDocValuesIterator, 0);
        });
        assertEquals("Unsupported Iterator: " + iterator.toString(), exception.getMessage());
    }

    public void testNextDoc() throws IOException {
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator(iterator);
        when(iterator.nextDoc()).thenReturn(5);

        int result = starTreeDocValuesIteratorAdapter.nextDoc(sequentialDocValuesIterator, 5);
        assertEquals(5, result);
    }

    public void test_multipleCoordinatedDocumentReader() throws IOException {
        SortedNumericDocValues iterator1 = Mockito.mock(SortedNumericDocValues.class);
        SortedNumericDocValues iterator2 = Mockito.mock(SortedNumericDocValues.class);

        SequentialDocValuesIterator sequentialDocValuesIterator1 = new SequentialDocValuesIterator(iterator1);
        SequentialDocValuesIterator sequentialDocValuesIterator2 = new SequentialDocValuesIterator(iterator2);

        when(iterator1.nextDoc()).thenReturn(0);
        when(iterator2.nextDoc()).thenReturn(1);

        when(iterator1.nextValue()).thenReturn(9L);
        when(iterator2.nextValue()).thenReturn(9L);

        starTreeDocValuesIteratorAdapter.nextDoc(sequentialDocValuesIterator1, 0);
        starTreeDocValuesIteratorAdapter.nextDoc(sequentialDocValuesIterator2, 0);
        assertEquals(0, sequentialDocValuesIterator1.getDocId());
        assertEquals(9L, (long) sequentialDocValuesIterator1.getDocValue());
        assertNotEquals(0, sequentialDocValuesIterator2.getDocId());
        assertEquals(1, sequentialDocValuesIterator2.getDocId());
        assertEquals(9L, (long) sequentialDocValuesIterator2.getDocValue());

    }

}
