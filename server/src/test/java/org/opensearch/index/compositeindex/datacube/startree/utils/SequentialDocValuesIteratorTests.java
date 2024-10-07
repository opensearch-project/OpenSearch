/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class SequentialDocValuesIteratorTests extends OpenSearchTestCase {

    private static FieldInfo mockFieldInfo;

    @BeforeClass
    public static void setup() {
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
        SequentialDocValuesIterator result = new SequentialDocValuesIterator(
            new SortedNumericStarTreeValuesIterator(producer.getSortedNumeric(mockFieldInfo))
        );

    }

    public void testGetNextValue_SortedNumeric() throws IOException {
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        when(iterator.nextDoc()).thenReturn(0);
        when(iterator.nextValue()).thenReturn(123L);
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator(
            new SortedNumericStarTreeValuesIterator(iterator)
        );
        sequentialDocValuesIterator.nextEntry(0);
        long result = sequentialDocValuesIterator.value(0);
        assertEquals(123L, result);
    }

    public void testNextEntry() throws IOException {
        SortedNumericDocValues iterator = Mockito.mock(SortedNumericDocValues.class);
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator(
            new SortedNumericStarTreeValuesIterator(iterator)
        );
        when(iterator.nextDoc()).thenReturn(5);

        int result = sequentialDocValuesIterator.nextEntry(5);
        assertEquals(5, result);
    }

    public void test_multipleCoordinatedDocumentReader() throws IOException {
        SortedNumericDocValues iterator1 = Mockito.mock(SortedNumericDocValues.class);
        SortedNumericDocValues iterator2 = Mockito.mock(SortedNumericDocValues.class);

        SequentialDocValuesIterator sequentialDocValuesIterator1 = new SequentialDocValuesIterator(
            new SortedNumericStarTreeValuesIterator(iterator1)
        );
        SequentialDocValuesIterator sequentialDocValuesIterator2 = new SequentialDocValuesIterator(
            new SortedNumericStarTreeValuesIterator(iterator2)
        );

        when(iterator1.nextDoc()).thenReturn(0);
        when(iterator2.nextDoc()).thenReturn(1);

        when(iterator1.nextValue()).thenReturn(9L);
        when(iterator2.nextValue()).thenReturn(9L);

        sequentialDocValuesIterator1.nextEntry(0);
        sequentialDocValuesIterator2.nextEntry(0);
        assertEquals(0, sequentialDocValuesIterator1.getEntryId());
        assertEquals(9L, (long) sequentialDocValuesIterator1.value(0));
        assertNull(sequentialDocValuesIterator2.value(0));
        assertNotEquals(0, sequentialDocValuesIterator2.getEntryId());
        assertEquals(1, sequentialDocValuesIterator2.getEntryId());
        assertEquals(9L, (long) sequentialDocValuesIterator2.value(1));
    }
}
