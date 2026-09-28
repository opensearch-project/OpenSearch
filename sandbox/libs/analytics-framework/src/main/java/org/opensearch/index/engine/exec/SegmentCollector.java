/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;

/**
 * A per-segment document collector returned by
 * {@link IndexFilterProvider#createCollector}.
 * <p>
 * Callers should use try-with-resources to ensure cleanup.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SegmentCollector extends Closeable {

    /**
     * Collect matching document IDs in the given range into the provided
     * {@link MemorySegment}.
     *
     * <p>Bit layout: the {@code out} segment receives a packed bitset where
     * word {@code j} bit {@code i} (LSB-first) represents the doc at
     * relative position {@code j*64 + i} within {@code [minDoc, maxDoc)}.
     * That is, bit {@code k} represents absolute doc id {@code minDoc + k}.
     * The caller must provide a segment of at least
     * {@code ceilDiv(maxDoc - minDoc, 64) * 8} bytes. Implementations
     * MUST NOT skip trailing zero words.
     *
     * <p>Forward-only: successive calls MUST use non-decreasing,
     * non-overlapping {@code [minDoc, maxDoc)} ranges. Backing iterators
     * are one-shot cursors and cannot seek backwards; violating the
     * invariant silently yields wrong results for ranges already passed.
     *
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @param out    destination {@link MemorySegment} to write the packed bitset into
     * @return the number of 64-bit words written into {@code out}
     */
    int collectDocs(int minDoc, int maxDoc, MemorySegment out);

    @Override
    default void close() {}
}
