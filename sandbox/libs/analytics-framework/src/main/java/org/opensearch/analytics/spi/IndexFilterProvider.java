/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * Produces doc-id bitsets for one index-backed filter leaf.
 *
 * <p>Conceptually a compiled query bound to a shard: built once per query
 * per shard from an opaque serialized query payload, then used to create
 * cheap per-segment matchers that materialize doc-id bitsets on demand.
 * The SPI is backend-agnostic — any index implementation (inverted, sparse
 * vector, columnar, etc.) can satisfy it.
 *
 * <p>Lifecycle is driven by the native engine:
 * <ol>
 *   <li>Native upcalls {@code createProvider(queryBytes)} on the registered
 *       {@link IndexFilterProviderFactory}; this produces a provider and
 *       registers it in the backend's internal provider registry, returning
 *       a {@code providerKey}.</li>
 *   <li>Native upcalls {@code createCollector(providerKey, seg, min, max)}
 *       per (segment, row-group range). Internally this routes to
 *       {@link #createCollector(int, int, int)} on this provider.</li>
 *   <li>Native upcalls {@code collectDocs(collectorKey, min, max, out)}
 *       per row group while iterating.</li>
 *   <li>Native upcalls {@code releaseCollector(collectorKey)} when done with
 *       a segment, {@code releaseProvider(providerKey)} when the query ends.</li>
 * </ol>
 *
 * <p>The SPI intentionally uses a plain {@code long[]} rather than an FFM
 * {@code MemorySegment} so the interface is backend-agnostic and does not
 * require JDK 22+ FFM APIs on the implementer side. The backend-specific FFM
 * callback copies from this array into the caller's native buffer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFilterProvider extends Closeable {

    /**
     * Create a collector for one (segment, [minDoc, maxDoc)) range.
     *
     * @return a provider-internal collector key {@code >= 0}, or {@code -1}
     *         if the collector cannot be created (e.g. empty range).
     */
    int createCollector(int segmentOrd, int minDoc, int maxDoc);

    /**
     * Fill {@code out} with the matching doc-id bitset for the given
     * collector over doc range {@code [minDoc, maxDoc)}.
     *
     * <p>Bit layout: the word at index {@code i} contains matches for docs
     * {@code [minDoc + i*64, minDoc + (i+1)*64)}, LSB-first within each word.
     *
     * @param collectorKey provider-internal collector key returned by
     *                     {@link #createCollector(int, int, int)}.
     * @param minDoc       inclusive lower bound of the doc range.
     * @param maxDoc       exclusive upper bound of the doc range.
     * @param out          destination buffer; implementation may write up to
     *                     {@code out.length} words.
     * @return number of words actually written ({@code 0 <= n <= out.length}),
     *         or {@code -1} on error.
     */
    int collectDocs(int collectorKey, int minDoc, int maxDoc, long[] out);

    /**
     * Release resources for a collector when the native engine is done
     * iterating its segment.
     */
    void releaseCollector(int collectorKey);
}
