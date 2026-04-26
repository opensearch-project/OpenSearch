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
     * Collect matching document IDs in the given range.
     *
     * <p>Bit layout: the returned {@code long[]} is a packed bitset where
     * word {@code j} bit {@code i} (LSB-first) represents the doc at
     * relative position {@code j*64 + i} within {@code [minDoc, maxDoc)}.
     * That is, bit {@code k} represents absolute doc id {@code minDoc + k}.
     * Length is {@code ceilDiv(maxDoc - minDoc, 64)} words regardless of
     * how many bits are set (implementations MUST NOT truncate trailing
     * zero words).
     *
     * <p>Forward-only: successive calls MUST use non-decreasing,
     * non-overlapping {@code [minDoc, maxDoc)} ranges. Backing iterators
     * are one-shot cursors and cannot seek backwards; violating the
     * invariant silently yields wrong results for ranges already passed.
     *
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return packed {@code long[]} bitset anchored at {@code minDoc}
     */
    long[] collectDocs(int minDoc, int maxDoc);

    @Override
    default void close() {}
}
