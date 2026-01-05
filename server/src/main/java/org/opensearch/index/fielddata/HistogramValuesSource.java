/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.indices.fielddata.Histogram;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HistogramValuesSource extends ValuesSource.Numeric {
    private final HistogramIndexFieldData indexFieldData;

    public HistogramValuesSource(HistogramIndexFieldData indexFieldData) {
        this.indexFieldData = indexFieldData;
    }

    public HistogramIndexFieldData getHistogramFieldData() {
        return indexFieldData;
    }

    @Override
    public boolean isFloatingPoint() {
        return true;
    }

    @Override
    public boolean isBigInteger() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("Histogram fields only support double values");
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        final HistogramLeafFieldData leafFieldData = indexFieldData.load(context);
        final HistogramValues histogramValues = leafFieldData.getHistogramValues();

        return new SortedNumericDoubleValues() {
            private double[] currentValues;
            private int currentValueIndex;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (histogramValues.advanceExact(doc)) {
                    Histogram histogram = histogramValues.histogram();
                    currentValues = histogram.getValues();
                    currentValueIndex = 0;
                    return currentValues.length > 0;
                }
                currentValues = null;
                return false;
            }

            @Override
            public double nextValue() throws IOException {
                if (currentValues == null || currentValueIndex >= currentValues.length) {
                    throw new IllegalStateException("Cannot call nextValue() when there are no more values");
                }
                return currentValues[currentValueIndex++];
            }

            @Override
            public int docValueCount() {
                return currentValues == null ? 0 : currentValues.length;
            }
        };
    }

    /**
     * Returns the counts values for the histogram buckets as doubles.
     * These represent the frequency/count per bucket.
     */
    public SortedNumericDoubleValues getCounts(LeafReaderContext context) throws IOException {
        final HistogramLeafFieldData leafFieldData = indexFieldData.load(context);
        final HistogramValues histogramValues = leafFieldData.getHistogramValues();

        return new SortedNumericDoubleValues() {
            private long[] currentCounts;
            private int currentIndex;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (histogramValues.advanceExact(doc)) {
                    Histogram histogram = histogramValues.histogram();
                    currentCounts = histogram.getCounts();
                    currentIndex = 0;
                    return currentCounts.length > 0;
                }
                currentCounts = null;
                return false;
            }

            @Override
            public double nextValue() throws IOException {
                if (currentCounts == null || currentIndex >= currentCounts.length) {
                    throw new IllegalStateException("Cannot call nextValue() when there are no more count values");
                }
                return (double) currentCounts[currentIndex++];
            }

            @Override
            public int docValueCount() {
                return currentCounts == null ? 0 : currentCounts.length;
            }
        };
    }

    private double decodeMin(BytesRef bytesRef) {
        ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        int size = buffer.getInt();
        if (size > 0) {
            return buffer.getDouble();
        }
        return Double.NaN;
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        return null;
    }
}
