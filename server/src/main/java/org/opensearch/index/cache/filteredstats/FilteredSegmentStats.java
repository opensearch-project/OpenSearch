/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Visible-subset statistics of one segment under one filter: the visible-doc bitset, and per-field and per-term counts
 * over it, computed on first use.
 * <p>
 * A search holds this for the whole request, so lookups after the first touch only lock-free maps. Each value added is
 * charged to the node-level {@link IndicesFilteredStatsCache}; one it cannot charge is returned without being kept.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FilteredSegmentStats {

    /** Stored for a term with no visible occurrences, so that answer is kept too. */
    private static final long[] TERM_ABSENT = new long[0];
    // A map node, its key reference and its value array header, beyond the key and value contents.
    private static final long ITEM_OVERHEAD_BYTES = 64;

    private final BitSet visibleDocs;
    // Null for statistics that are computed on every call and never kept.
    private final IndicesFilteredStatsCache nodeCache;
    final FilteredStatsCache owner;
    private final Map<String, long[]> fields = new ConcurrentHashMap<>();
    private final Map<Term, long[]> terms = new ConcurrentHashMap<>();

    // Set by the node cache when the entry is cached: its key, and the weight the LRU last saw for it.
    IndicesFilteredStatsCache.Key key;
    volatile long weighed;

    // Guarded by this: bytes charged for this entry, and whether it has left the cache.
    private long bytes;
    private boolean removed;

    FilteredSegmentStats(BitSet visibleDocs, IndicesFilteredStatsCache nodeCache, FilteredStatsCache owner) {
        this.visibleDocs = visibleDocs;
        this.nodeCache = nodeCache;
        this.owner = owner;
    }

    /**
     * Returns statistics over the given visible docs that compute every value on each call.
     *
     * @param visibleDocs the segment's visible docs, or {@code null} when none
     * @return statistics that keep nothing
     */
    public static FilteredSegmentStats uncached(BitSet visibleDocs) {
        return new FilteredSegmentStats(visibleDocs, null, null);
    }

    /** Returns the visible docs of the segment, or {@code null} when no doc is visible. */
    public BitSet visibleDocs() {
        return visibleDocs;
    }

    /**
     * Returns the visible-subset {@code [docCount, sumTotalTermFreq, sumDocFreq]} of a field.
     *
     * @param field the field
     * @param compute computes the contribution on a miss
     * @return the contribution
     */
    public long[] field(String field, FilteredStatsCache.Computation<long[]> compute) throws IOException {
        if (nodeCache == null) {
            return compute.compute();
        }
        final long[] cached = fields.get(field);
        if (cached != null) {
            nodeCache.onHit();
            return cached;
        }
        nodeCache.onMiss();
        final long[] computed = compute.compute();
        final long weight = ITEM_OVERHEAD_BYTES + RamUsageEstimator.sizeOf(field) + RamUsageEstimator.sizeOf(computed);
        if (nodeCache.charge(this, weight) && fields.putIfAbsent(field, computed) != null) {
            nodeCache.release(this, weight);
        }
        return computed;
    }

    /**
     * Returns the visible-subset {@code [docFreq, totalTermFreq]} of a term, or {@code null} when no visible doc
     * contains it.
     *
     * @param term the term
     * @param compute computes the contribution on a miss, returning {@code null} when the term has no visible docs
     * @return the contribution, or {@code null}
     */
    public long[] term(Term term, FilteredStatsCache.Computation<long[]> compute) throws IOException {
        if (nodeCache == null) {
            return compute.compute();
        }
        final long[] cached = terms.get(term);
        if (cached != null) {
            nodeCache.onHit();
            return cached == TERM_ABSENT ? null : cached;
        }
        nodeCache.onMiss();
        final long[] computed = compute.compute();
        final long[] stored = computed == null ? TERM_ABSENT : computed;
        // Copied, so the kept key does not hold the caller's term buffer.
        final Term key = new Term(term.field(), BytesRef.deepCopyOf(term.bytes()));
        final long weight = ITEM_OVERHEAD_BYTES + key.ramBytesUsed() + (computed == null ? 0 : RamUsageEstimator.sizeOf(computed));
        if (nodeCache.charge(this, weight) && terms.putIfAbsent(key, stored) != null) {
            nodeCache.release(this, weight);
        }
        return computed;
    }

    /** Adds charged bytes, returning the new total, or -1 when the entry has already left the cache. */
    synchronized long add(long delta) {
        if (removed) {
            return -1;
        }
        bytes += delta;
        return bytes;
    }

    /** Marks the entry as gone and returns the bytes still charged for it. */
    synchronized long remove() {
        removed = true;
        final long charged = bytes;
        bytes = 0;
        return charged;
    }

    synchronized boolean isRemoved() {
        return removed;
    }

    synchronized long bytes() {
        return bytes;
    }

}
