/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedNumericDocValuesWriterWrapper;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Counter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

public class SortedNumericDocValuesWriterWrapperTests extends OpenSearchTestCase {

    private SortedNumericDocValuesWriterWrapper wrapper;
    private FieldInfo fieldInfo;
    private Counter counter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fieldInfo = new FieldInfo(
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
        counter = Counter.newCounter();
        wrapper = new SortedNumericDocValuesWriterWrapper(fieldInfo, counter);
    }

    public void testAddValue() throws IOException {
        wrapper.addValue(0, 10);
        wrapper.addValue(1, 20);
        wrapper.addValue(2, 30);

        SortedNumericDocValues docValues = wrapper.getDocValues();
        assertNotNull(docValues);

        assertEquals(0, docValues.nextDoc());
        assertEquals(10, docValues.nextValue());
        assertEquals(1, docValues.nextDoc());
        assertEquals(20, docValues.nextValue());
        assertEquals(2, docValues.nextDoc());
        assertEquals(30, docValues.nextValue());
    }

    public void testGetDocValues() {
        SortedNumericDocValues docValues = wrapper.getDocValues();
        assertNotNull(docValues);
    }

    public void testMultipleValues() throws IOException {
        wrapper.addValue(0, 10);
        wrapper.addValue(0, 20);
        wrapper.addValue(1, 30);

        SortedNumericDocValues docValues = wrapper.getDocValues();
        assertNotNull(docValues);

        assertEquals(0, docValues.nextDoc());
        assertEquals(10, docValues.nextValue());
        assertEquals(20, docValues.nextValue());
        assertThrows(IllegalStateException.class, docValues::nextValue);

        assertEquals(1, docValues.nextDoc());
        assertEquals(30, docValues.nextValue());
        assertThrows(IllegalStateException.class, docValues::nextValue);
    }
}
