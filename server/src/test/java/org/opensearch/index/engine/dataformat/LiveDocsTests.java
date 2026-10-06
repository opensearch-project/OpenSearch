/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link LiveDocs}.
 */
public class LiveDocsTests extends OpenSearchTestCase {

    // ========== fromPackedBits ==========

    public void testFromPackedBitsNullReturnsAllAlive() {
        assertSame(LiveDocs.ALL_ALIVE, LiveDocs.fromPackedBits(null));
    }

    public void testFromPackedBitsEmptyMapReturnsAllAlive() {
        assertSame(LiveDocs.ALL_ALIVE, LiveDocs.fromPackedBits(Map.of()));
    }

    public void testFromPackedBitsDefensivelyClonesArrays() {
        long[] bits = new long[] { 0b101L };
        Map<Long, long[]> raw = new HashMap<>();
        raw.put(1L, bits);
        LiveDocs liveDocs = LiveDocs.fromPackedBits(raw);

        assertTrue(liveDocs.isAlive(1L, 0));
        assertFalse(liveDocs.isAlive(1L, 1));

        // Mutating the caller's array must not affect the LiveDocs instance.
        bits[0] = 0L;
        assertTrue("LiveDocs must clone input arrays", liveDocs.isAlive(1L, 0));
    }

    // ========== allAlive ==========

    public void testAllAliveSingleton() {
        assertTrue(LiveDocs.ALL_ALIVE.allAlive());
        assertTrue(LiveDocs.ALL_ALIVE.allAlive(randomLong()));
        assertEquals(0, LiveDocs.ALL_ALIVE.segmentsWithDeletes());
    }

    public void testAllAliveFalseWhenSegmentHasDeletes() {
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(5L, new long[] { 0b1L }));
        assertFalse(liveDocs.allAlive());
        assertFalse(liveDocs.allAlive(5L));
        assertTrue("other segments have no deletes", liveDocs.allAlive(6L));
        assertEquals(1, liveDocs.segmentsWithDeletes());
    }

    // ========== isAlive ==========

    public void testIsAliveReadsBits() {
        // rows 0 and 2 alive, row 1 dead
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(1L, new long[] { 0b101L }));
        assertTrue(liveDocs.isAlive(1L, 0));
        assertFalse(liveDocs.isAlive(1L, 1));
        assertTrue(liveDocs.isAlive(1L, 2));
    }

    public void testIsAliveForSegmentWithoutDeletesReturnsTrue() {
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(1L, new long[] { 0b101L }));
        // Segment 2 has no entry — everything is alive.
        assertTrue(liveDocs.isAlive(2L, 0));
        assertTrue(liveDocs.isAlive(2L, randomLongBetween(0, 1_000_000)));
    }

    public void testIsAliveAtWordBoundaries() {
        // Word 0: only bit 63 set. Word 1: only bit 0 set (row 64).
        long[] bits = new long[] { 1L << 63, 1L };
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(1L, bits));

        assertTrue(liveDocs.isAlive(1L, 63));
        assertTrue(liveDocs.isAlive(1L, 64));
        assertFalse(liveDocs.isAlive(1L, 62));
        assertFalse(liveDocs.isAlive(1L, 65));
    }

    public void testIsAliveBeyondBitsetReturnsTrueDefensively() {
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(1L, new long[] { 0L }));
        // Word index 1 is beyond the one-word bitset — treated as alive.
        assertTrue(liveDocs.isAlive(1L, 64));
        assertTrue(liveDocs.isAlive(1L, 1_000_000));
        // Within the bitset, all bits are 0 — dead.
        assertFalse(liveDocs.isAlive(1L, 0));
        assertFalse(liveDocs.isAlive(1L, 63));
    }

    // ========== packedBits ==========

    public void testPackedBitsReturnsBitsForSegmentWithDeletes() {
        long[] bits = new long[] { 0b1011L };
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(7L, bits));
        assertArrayEquals(bits, liveDocs.packedBits(7L));
    }

    public void testPackedBitsReturnsNullForSegmentWithoutDeletes() {
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(7L, new long[] { 0b1L }));
        assertNull(liveDocs.packedBits(8L));
        assertNull(LiveDocs.ALL_ALIVE.packedBits(7L));
    }

    // ========== segmentsWithDeletes ==========

    public void testSegmentsWithDeletesCountsEntries() {
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(1L, new long[] { 0b1L }, 2L, new long[] { 0b10L }));
        assertEquals(2, liveDocs.segmentsWithDeletes());
    }
}
