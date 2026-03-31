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
 * Factory for creating {@link NativeStatsBlock} instances from a positional
 * {@code long[]} array received via JNI.
 *
 * <p>Intended for use with {@code ArrayCursor#read(NativeStatsBlockFactory)}
 * and method references such as {@code RuntimeValues::new}.
 *
 * @param <T> the concrete stats block type
 */
@FunctionalInterface
@InternalApi
public interface NativeStatsBlockFactory<T extends NativeStatsBlock> {

    /**
     * Creates a stats block by reading from {@code data} starting at {@code offset}.
     *
     * @param data   the source array (typically the full JNI payload)
     * @param offset start position within {@code data}
     * @return a new stats block instance
     */
    T create(long[] data, int offset);
}
