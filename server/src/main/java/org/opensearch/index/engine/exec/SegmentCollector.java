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
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return packed {@code long[]} bitset of matching doc IDs
     */
    long[] collectDocs(int minDoc, int maxDoc);

    @Override
    default void close() {}
}
