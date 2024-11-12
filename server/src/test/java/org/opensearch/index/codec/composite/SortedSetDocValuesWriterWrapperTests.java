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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.SortedSetDocValuesWriterWrapper;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

public class SortedSetDocValuesWriterWrapperTests extends OpenSearchTestCase {

    private SortedSetDocValuesWriterWrapper wrapper;
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
        ByteBlockPool.DirectTrackingAllocator byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(counter);
        ByteBlockPool docValuesBytePool = new ByteBlockPool(byteBlockAllocator);
        wrapper = new SortedSetDocValuesWriterWrapper(fieldInfo, counter, docValuesBytePool);
    }

    public void testAddValue() throws IOException {
        wrapper.addValue(0, new BytesRef("text1"));
        wrapper.addValue(1, new BytesRef("text2"));
        wrapper.addValue(2, new BytesRef("text3"));

        SortedSetDocValues docValues = wrapper.getDocValues();
        assertNotNull(docValues);

        assertEquals(0, docValues.nextDoc());
        assertEquals(0, docValues.nextOrd());
        assertEquals(1, docValues.nextDoc());
        assertEquals(1, docValues.nextOrd());
        assertEquals(2, docValues.nextDoc());
        assertEquals(2, docValues.nextOrd());
    }

    public void testGetDocValues() {
        SortedSetDocValues docValues = wrapper.getDocValues();
        assertNotNull(docValues);
    }

    public void testMultipleValues() throws IOException {
        wrapper.addValue(0, new BytesRef("text1"));
        wrapper.addValue(0, new BytesRef("text2"));
        wrapper.addValue(1, new BytesRef("text3"));

        SortedSetDocValues docValues = wrapper.getDocValues();
        assertNotNull(docValues);

        assertEquals(0, docValues.nextDoc());
        assertEquals(0, docValues.nextOrd());
        assertEquals(1, docValues.nextOrd());
        assertEquals(-1, docValues.nextOrd());

        assertEquals(1, docValues.nextDoc());
        assertEquals(2, docValues.nextOrd());
        assertEquals(-1, docValues.nextOrd());
    }
}
