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
 * A forward-only cursor over a flat {@code long[]} array received from the
 * native (Rust) side via JNI.
 *
 * <p>Each call to {@link #read(NativeStatsBlockFactory)} creates a typed
 * {@link NativeStatsBlock} starting at the current position and advances
 * the cursor by the block's {@link NativeStatsBlock#size() size}.
 *
 * <p>After all blocks have been read, call {@link #assertFullyConsumed()}
 * to verify that every element in the array was consumed.
 */
@InternalApi
public class ArrayCursor {

    private final long[] data;
    private int pos;

    /**
     * Creates a cursor over the given array, starting at position 0.
     *
     * @param data the source array (typically the full JNI payload)
     */
    public ArrayCursor(long[] data) {
        this.data = data;
        this.pos = 0;
    }

    /**
     * Reads a stats block from the current position and advances the cursor.
     *
     * @param factory a factory (typically a constructor reference such as
     *                {@code RuntimeValues::new}) that creates a block from
     *                the array and an offset
     * @param <T>     the concrete stats block type
     * @return the newly created stats block
     */
    public <T extends NativeStatsBlock> T read(NativeStatsBlockFactory<T> factory) {
        T block = factory.create(data, pos);
        pos += block.size();
        return block;
    }

    /**
     * Asserts that the cursor has consumed every element in the array.
     *
     * @throws IllegalArgumentException if the current position does not equal
     *                                  the array length
     */
    public void assertFullyConsumed() {
        if (pos != data.length) {
            throw new IllegalArgumentException(
                "Array not fully consumed: position=" + pos + ", length=" + data.length
            );
        }
    }
}
