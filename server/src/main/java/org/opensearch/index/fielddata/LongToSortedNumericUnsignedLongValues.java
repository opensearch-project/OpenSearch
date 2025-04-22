/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

/**
 * Wraps long-based {@link SortedNumericDocValues} as unsigned long ones
 * (primarily used by {@link org.opensearch.search.MultiValueMode}
 *
 * @opensearch.internal
 */
public final class LongToSortedNumericUnsignedLongValues extends SortedNumericUnsignedLongValues {
    private final SortedNumericDocValues values;

    public LongToSortedNumericUnsignedLongValues(SortedNumericDocValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public long nextValue() throws IOException {
        return values.nextValue();
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    public int advance(int target) throws IOException {
        return values.advance(target);
    }

    public int docID() {
        return values.docID();
    }

    /** Return the wrapped values. */
    public SortedNumericDocValues getNumericUnsignedLongValues() {
        return values;
    }
}
