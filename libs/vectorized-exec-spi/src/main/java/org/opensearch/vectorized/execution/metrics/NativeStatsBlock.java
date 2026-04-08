/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.common.annotation.InternalApi;

/**
 * Abstract base class for typed views over a positional {@code long[]} array
 * received from the native (Rust) side via JNI.
 *
 * <p>Subclasses define named getters that delegate to {@link #get(int)} using
 * compile-time offset constants, and declare their block size via {@link #size()}.
 */
@InternalApi
public abstract class NativeStatsBlock {

    protected final long[] values;

    /**
     * Copies {@code size} elements from {@code data} starting at {@code offset}
     * into an internal array.
     *
     * @param data   the source array (typically the full JNI payload)
     * @param offset start position within {@code data}
     * @param size   number of elements this block occupies
     * @throws IllegalArgumentException if the requested range exceeds {@code data.length}
     */
    protected NativeStatsBlock(long[] data, int offset, int size) {
        if (offset < 0 || size < 0 || offset + size > data.length) {
            throw new IllegalArgumentException(
                "Invalid range: offset=" + offset + ", size=" + size + ", data.length=" + data.length
            );
        }
        this.values = new long[size];
        System.arraycopy(data, offset, this.values, 0, size);
    }

    /**
     * Returns the number of {@code long} slots this block occupies in the flat array.
     */
    public abstract int size();

    /**
     * Returns the raw {@code long} value at the given index within this block.
     */
    protected long get(int index) {
        return values[index];
    }

    /**
     * Reinterprets the {@code long} value at the given index as a {@code double}
     * via {@link Double#longBitsToDouble(long)}.
     */
    protected double getAsDouble(int index) {
        return Double.longBitsToDouble(values[index]);
    }
}
