/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.common.Numbers;

import java.io.IOException;

/**
 * {@link SortedNumericDoubleValues} instance that wraps a {@link SortedNumericDocValues}
 * and converts the unsigned long to double using {@link Numbers#unsignedLongToDouble(long)}.
 *
 * @opensearch.internal
 */
final class UnsignedLongToSortedNumericDoubleValues extends SortedNumericDoubleValues {

    private final SortedNumericDocValues values;

    UnsignedLongToSortedNumericDoubleValues(SortedNumericDocValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public double nextValue() throws IOException {
        return Numbers.unsignedLongToDouble(values.nextValue());
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    /** Return the wrapped values. */
    public SortedNumericDocValues getLongValues() {
        return values;
    }

}
