/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.util.Counter;

/**
 * A wrapper class for writing sorted numeric doc values.
 * <p>
 * This class provides a convenient way to add sorted numeric doc values to a field
 * and retrieve the corresponding {@link SortedNumericDocValues} instance.
 *
 * @opensearch.experimental
 */
public class SortedNumericDocValuesWriterWrapper extends DocValuesWriterWrapper<SortedNumericDocValues> {

    private final SortedNumericDocValuesWriter sortedNumericDocValuesWriter;

    /**
     * Sole constructor. Constructs a new {@link SortedNumericDocValuesWriterWrapper} instance.
     *
     * @param fieldInfo the field information for the field being written
     * @param counter a counter for tracking memory usage
     */
    public SortedNumericDocValuesWriterWrapper(FieldInfo fieldInfo, Counter counter) {
        sortedNumericDocValuesWriter = new SortedNumericDocValuesWriter(fieldInfo, counter);
    }

    /**
     * Adds a value to the sorted numeric doc values for the specified document.
     *
     * @param docID the document ID
     * @param value the value to add
     */
    public void addValue(int docID, long value) {
        sortedNumericDocValuesWriter.addValue(docID, value);
    }

    /**
     * Returns the {@link SortedNumericDocValues} instance containing the sorted numeric doc values
     *
     * @return the {@link SortedNumericDocValues} instance
     */
    @Override
    public SortedNumericDocValues getDocValues() {
        return sortedNumericDocValuesWriter.getDocValues();
    }
}
