/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

/**
 * A simple iterator that implements the AbstractHyperLogLog.RunLenIterator interface
 * and simply iterates over the raw byte array from DataFusion.
 */
public class DataFusionRunLenIterator implements AbstractHyperLogLog.RunLenIterator {
    private final byte[] sketchBytes;
    private final int m;
    private int pos = 0;

    DataFusionRunLenIterator(byte[] sketchBytes) {
        this.sketchBytes = sketchBytes;
        this.m = sketchBytes.length;
    }

    @Override
    public boolean next() {
        if (pos < m) {
            pos++;
            return true;
        }
        return false;
    }

    @Override
    public byte value() {
        // `next()` moves pos, so `value()` reads the byte at `pos-1`
        return sketchBytes[pos - 1];
    }
}
