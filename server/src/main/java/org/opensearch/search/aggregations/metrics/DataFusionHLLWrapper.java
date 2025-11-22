/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.util.BigArrays;

/**
 * A simple wrapper class that implements the abstract methods from AbstractHyperLogLogPlusPlus which
 * OpenSearch needs to perform a merge. It holds our partial cardinality aggregation hll sketch.
 */
public class DataFusionHLLWrapper extends AbstractHyperLogLogPlusPlus {
    private final byte[] sketchBytes;

    public DataFusionHLLWrapper(int precision, byte[] sketchBytes) {
        super(precision);
        int m = 1 << precision;
        if (sketchBytes.length != m) {
            throw new IllegalArgumentException(
                    "Byte array length " + sketchBytes.length +
                            " does not match precision " + m
            );
        }
        this.sketchBytes = sketchBytes;
    }

    /**
     * Tell OpenSearch this sketch is in HLL mode (not LinearCounting).
     */
    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return HYPERLOGLOG;
    }

    /**
     * Return our custom iterator that reads from the Rust byte array.
     */
    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        return new DataFusionRunLenIterator(this.sketchBytes);
    }

    public static HyperLogLogPlusPlus getHyperLogLogPlusPlus(byte[] hllSketchBytes) {
        HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(
                HyperLogLogPlusPlus.DEFAULT_PRECISION, // setting up same precision in rust
                BigArrays.NON_RECYCLING_INSTANCE,
                1
        );

        // 2. Create our custom wrapper using the bytes from Rust
        DataFusionHLLWrapper rustSketchWrapper = new DataFusionHLLWrapper(HyperLogLogPlusPlus.DEFAULT_PRECISION, hllSketchBytes);

        // 3. Use the HLL 'merge' method to merge bucket 0 from our wrapper into bucket 0 of the real sketch.
        sketch.merge(0, rustSketchWrapper, 0);
        return sketch;
    }

    // --- Unused abstract methods ---
    @Override public long maxOrd() { return 1; }
    @Override public long cardinality(long bucketOrd) { throw new UnsupportedOperationException(); }
    @Override protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) { throw new UnsupportedOperationException(); }
    @Override public void collect(long bucket, long hash) { throw new UnsupportedOperationException(); }
    @Override public void close() {}
}
