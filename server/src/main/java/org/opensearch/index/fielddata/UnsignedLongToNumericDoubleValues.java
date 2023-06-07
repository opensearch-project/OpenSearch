/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;
import org.opensearch.common.Numbers;

import java.io.IOException;

/**
 * {@link NumericDoubleValues} instance that wraps a {@link NumericDocValues}
 * and converts the unsigned long to double using {@link Numbers#unsignedLongToDouble(long)}.
 *
 * @opensearch.internal
 */
final class UnsignedLongToNumericDoubleValues extends NumericDoubleValues {

    private final NumericDocValues values;

    UnsignedLongToNumericDoubleValues(NumericDocValues values) {
        this.values = values;
    }

    @Override
    public double doubleValue() throws IOException {
        return Numbers.unsignedLongToDouble(values.longValue());
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        return values.advanceExact(doc);
    }

    /** Return the wrapped values. */
    public NumericDocValues getLongValues() {
        return values;
    }

}
