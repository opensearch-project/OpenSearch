/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Format-neutral per-segment live-docs abstraction.
 *
 * <p>Wraps a per-segment bitset indicating which rows are alive (not deleted).
 * Consumers use this at read time (to filter scan results) and at merge time
 * (to drop dead rows from the output).
 *
 * <p>The internal representation uses Lucene's {@code FixedBitSet#getBits()} layout
 * (packed {@code long[]} where bit {@code i} set = row {@code i} alive), but consumers
 * interact through the typed API without knowing the wire format.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LiveDocs {

    /** Singleton representing "all rows alive everywhere" — no filtering needed. */
    public static final LiveDocs ALL_ALIVE = new LiveDocs(Collections.emptyMap());

    private final Map<Long, long[]> perSegment;

    private LiveDocs(Map<Long, long[]> perSegment) {
        this.perSegment = perSegment;
    }

    /**
     * Creates a LiveDocs instance from raw per-segment bitsets.
     * Defensively clones each array to prevent aliasing.
     *
     * @param rawBitsets map of segment generation → packed bitset (Lucene layout)
     * @return a new LiveDocs instance
     */
    public static LiveDocs fromPackedBits(Map<Long, long[]> rawBitsets) {
        if (rawBitsets == null || rawBitsets.isEmpty()) {
            return ALL_ALIVE;
        }
        Map<Long, long[]> copy = new HashMap<>(rawBitsets.size());
        for (Map.Entry<Long, long[]> entry : rawBitsets.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().clone());
        }
        return new LiveDocs(Collections.unmodifiableMap(copy));
    }

    /**
     * Returns true if all rows in all segments are alive (no deletes anywhere).
     */
    public boolean allAlive() {
        return perSegment.isEmpty();
    }

    /**
     * Returns true if all rows in the given segment are alive.
     *
     * @param segmentGeneration the segment generation
     */
    public boolean allAlive(long segmentGeneration) {
        return !perSegment.containsKey(segmentGeneration);
    }

    /**
     * Returns true if the row at the given ID in the given segment is alive.
     *
     * @param segmentGeneration the segment generation
     * @param rowId the row ID within the segment
     */
    public boolean isAlive(long segmentGeneration, long rowId) {
        long[] bits = perSegment.get(segmentGeneration);
        if (bits == null) {
            return true; // no deletes in this segment
        }
        int wordIdx = (int) (rowId / 64);
        long bitIdx = rowId % 64;
        if (wordIdx >= bits.length) {
            return true; // beyond bitset — defensive
        }
        return (bits[wordIdx] & (1L << bitIdx)) != 0;
    }

    /**
     * Returns the raw packed bitset for a segment, for FFI-friendly consumers (e.g., Rust via JNI).
     * Returns null if all rows in the segment are alive.
     *
     * @param segmentGeneration the segment generation
     */
    public long[] packedBits(long segmentGeneration) {
        return perSegment.get(segmentGeneration);
    }

    /**
     * Returns the number of segments that have deletes.
     */
    public int segmentsWithDeletes() {
        return perSegment.size();
    }
}
