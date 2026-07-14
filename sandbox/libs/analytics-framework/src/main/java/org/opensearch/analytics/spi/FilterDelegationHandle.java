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
 *   <li>Rust calls {@link #createCollector(int, long, int, int)} per (provider × segment)</li>
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
     * <p>The segment is identified by its <b>writer generation</b> — the stable
     * identifier that matches {@code Segment.generation()} in the catalog snapshot
     * and the {@code writer_generation} attribute
     *
     * @param providerKey key returned by {@link #createProvider(int)}
     * @param writerGeneration the writer generation for this segment
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return a collector key {@code >= 0}, or {@code -1} on failure (including
     *         "no segment with this generation" in the accepting backend)
     */
    int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc);

    /**
     * Fill {@code out} with the matching doc-id bitset for the given collector.
     *
     * <p>Bit layout: word {@code i} contains matches for docs
     * {@code [minDoc + i*64, minDoc + (i+1)*64)}, LSB-first within each word.
     *
     * @param collectorKey key returned by {@link #createCollector(int, long, int, int)}
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
     * Create a collector and probe row-group boundaries for matches.
     *
     * <p>Creates a collector for the given segment range AND probes each
     * [rgMins[i], rgMaxs[i]) range to determine if any docs match, writing
     * 1 (may match) or 0 (definitely empty) into {@code outMatch}.
     *
     * <p>Default implementation delegates to {@link #createCollector} and
     * fills {@code outMatch} conservatively with all 1s.
     *
     * @param providerKey key from {@link #createProvider}
     * @param writerGeneration segment identifier
     * @param minDoc inclusive lower bound of segment partition
     * @param maxDoc exclusive upper bound of segment partition
     * @param rgMins inclusive lower bounds per row group (ascending order)
     * @param rgMaxs exclusive upper bounds per row group (ascending order)
     * @param outMatch output array: 1 = may match, 0 = definitely empty
     * @return packed long: if >= 0, lower 32 bits = collectorKey, upper 32 bits = firstDoc;
     *         -1 on error; -2 if no docs match in this segment (all RGs empty)
     */
    default long createCollectorWithProbe(
        int providerKey,
        long writerGeneration,
        int minDoc,
        int maxDoc,
        int[] rgMins,
        int[] rgMaxs,
        byte[] outMatch
    ) {
        java.util.Arrays.fill(outMatch, (byte) 1);
        int key = createCollector(providerKey, writerGeneration, minDoc, maxDoc);
        if (key < 0) return key;
        return (long) key & 0xFFFFFFFFL;
    }

    /**
     * Phase 1 of two-phase probe: create a collector and return its cost.
     * Same as {@link #createCollector} but returns a packed long with both
     * the collector key and the scorer's estimated cost.
     *
     * @return packed long: lower 32 bits = collectorKey, upper 32 bits = cost (capped at INT_MAX);
     *         -1 on error; -2 if no docs match (null scorer / segment empty)
     */
    default long createCollectorWithCost(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
        int key = createCollector(providerKey, writerGeneration, minDoc, maxDoc);
        if (key < 0) return key;
        // Default: return cost=INT_MAX (forces gate to fire → no probe)
        return ((long) Integer.MAX_VALUE << 32) | (key & 0xFFFFFFFFL);
    }

    /**
     * Phase 2 of two-phase probe: scan RG boundaries using a probe scorer.
     * Only called when Phase 1 returned a cost below the probe threshold.
     * Creates a lightweight probe scorer from the same Weight and advances
     * through row-group boundaries to identify empty RGs.
     *
     * @param providerKey the provider (Weight) to create the probe scorer from
     * @param writerGeneration identifies the segment
     * @param rgMins inclusive lower bounds per row group
     * @param rgMaxs exclusive upper bounds per row group
     * @param outMatch output: 1 = may match, 0 = definitely empty
     * @return number of RGs skipped (outMatch=0); -1 on error
     */
    default long probeCollector(int providerKey, long writerGeneration, int[] rgMins, int[] rgMaxs, byte[] outMatch) {
        java.util.Arrays.fill(outMatch, (byte) 1);
        return 0;
    }

    /**
     * Returns {@code true} if the owning query has been cancelled.
     *
     * @return whether the query is cancelled
     */
    default boolean isCancelled() {
        return false;
    }
}
