/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.opensearch.indices.fielddata.Histogram;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HistogramLeafFieldData implements LeafFieldData {
    private final LeafReader reader;
    private final String fieldName;

    public HistogramLeafFieldData(LeafReader reader, String fieldName) {
        this.reader = reader;
        this.fieldName = fieldName;
    }

    public HistogramValues getHistogramValues() throws IOException {
        final BinaryDocValues values = DocValues.getBinary(reader, fieldName);
        return new HistogramValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public Histogram histogram() throws IOException {
                BytesRef bytesRef = values.binaryValue();
                return decodeHistogram(bytesRef);
            }
        };
    }

    private Histogram decodeHistogram(BytesRef bytesRef) {
        ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        int size = buffer.getInt();
        double[] values = new double[size];
        long[] counts = new long[size];

        for (int i = 0; i < size; i++) {
            values[i] = buffer.getDouble();
            counts[i] = buffer.getLong();
        }

        return new Histogram(values, counts);
    }

    @Override
    public void close() {}

    @Override
    public ScriptDocValues<?> getScriptValues() {
        return null;
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return null;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}
