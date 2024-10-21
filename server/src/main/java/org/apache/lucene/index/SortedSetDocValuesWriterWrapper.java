/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * A wrapper class for writing sorted set doc values.
 * <p>
 * This class provides a convenient way to add sorted set doc values to a field
 * and retrieve the corresponding {@link SortedSetDocValues} instance.
 *
 * @opensearch.experimental
 */
public class SortedSetDocValuesWriterWrapper extends DocValuesWriterWrapper<SortedSetDocValues> {

    private final SortedSetDocValuesWriter sortedSetDocValuesWriterWrapper;

    /**
     * Sole constructor. Constructs a new {@link SortedSetDocValuesWriterWrapper} instance.
     *
     * @param fieldInfo the field information for the field being written
     * @param counter a counter for tracking memory usage
     * @param byteBlockPool a byte block pool for allocating byte blocks
     * @see SortedSetDocValuesWriter
     */
    public SortedSetDocValuesWriterWrapper(FieldInfo fieldInfo, Counter counter, ByteBlockPool byteBlockPool) {
        sortedSetDocValuesWriterWrapper = new SortedSetDocValuesWriter(fieldInfo, counter, byteBlockPool);
    }

    /**
     * Adds a bytes ref value to the sorted set doc values for the specified document.
     *
     * @param docID the document ID
     * @param value the value to add
     */
    public void addValue(int docID, BytesRef value) {
        sortedSetDocValuesWriterWrapper.addValue(docID, value);
    }

    /**
     * Returns the {@link SortedSetDocValues} instance containing the sorted numeric doc values
     *
     * @return the {@link SortedSetDocValues} instance
     */
    @Override
    public SortedSetDocValues getDocValues() {
        return sortedSetDocValuesWriterWrapper.getDocValues();
    }
}
