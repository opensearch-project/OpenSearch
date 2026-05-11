/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;

/**
 * Callback surface for filter delegation between a driving backend and an accepting backend.
 *
 * <p>One handle per query per shard. The accepting backend implements this interface;
 * the driving backend calls into it via FFM upcalls during execution. Core closes it
 * after execution completes.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Rust calls {@link #createProvider(int)} once per delegated predicate (per annotationId)</li>
 *   <li>Rust calls {@link #createCollector(int, int, int, int)} per (provider × segment)</li>
 *   <li>Rust calls {@link #collectDocs(int, int, int, MemorySegment)} per row group</li>
 *   <li>Rust calls {@link #releaseCollector(int)} when done with a segment</li>
 *   <li>Rust calls {@link #releaseProvider(int)} when the query ends</li>
 * </ol>
 *
 * @opensearch.internal
 */
public interface FilterDelegationHandle extends Closeable {

    /**
     * Create a provider for the given annotation ID. The accepting backend looks up
     * the pre-compiled query for this annotation and prepares it for segment iteration.
     *
     * @param annotationId the annotation ID identifying the delegated predicate
     * @return a provider key {@code >= 0}, or {@code -1} on failure
     */
    int createProvider(int annotationId);

    /**
     * Create a collector for one (segment, [minDoc, maxDoc)) range.
     *
     * @param providerKey key returned by {@link #createProvider(int)}
     * @param segmentOrd the segment ordinal
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return a collector key {@code >= 0}, or {@code -1} on failure
     */
    int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc);

    /**
     * Fill {@code out} with the matching doc-id bitset for the given collector.
     *
     * <p>Bit layout: word {@code i} contains matches for docs
     * {@code [minDoc + i*64, minDoc + (i+1)*64)}, LSB-first within each word.
     *
     * @param collectorKey key returned by {@link #createCollector(int, int, int, int)}
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @param out destination buffer; implementation writes up to {@code out.byteSize() / 8} words
     * @return number of words written, or {@code -1} on error
     */
    int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out);

    /**
     * Release resources for a collector.
     */
    void releaseCollector(int collectorKey);

    /**
     * Release resources for a provider.
     */
    void releaseProvider(int providerKey);

    /**
     * Returns {@code true} if the owning query has been cancelled.
     *
     * @return whether the query is cancelled
     */
    default boolean isCancelled() {
        return false;
    }
}
