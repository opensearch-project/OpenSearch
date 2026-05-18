/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link PackedRowIdMapping}.
 */
public class PackedRowIdMappingTests extends OpenSearchTestCase {

    /**
     * Basic forward lookup with a simple permutation.
     */
    public void testBasicForwardLookup() {
        long[] mappingArray = { 4, 3, 2, 1, 0 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);

        assertEquals(4L, mapping.getNewRowId(0));
        assertEquals(3L, mapping.getNewRowId(1));
        assertEquals(2L, mapping.getNewRowId(2));
        assertEquals(1L, mapping.getNewRowId(3));
        assertEquals(0L, mapping.getNewRowId(4));
    }

    /**
     * Implements the RowIdMapping interface correctly.
     */
    public void testImplementsInterface() {
        long[] mappingArray = { 10, 20 };
        RowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(10L, mapping.getNewRowId(0));
        assertEquals(20L, mapping.getNewRowId(1));
    }

    /**
     * Out-of-bounds row ID returns -1.
     */
    public void testOutOfBoundsRowIdReturnsNegativeOne() {
        long[] mappingArray = { 5, 6 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(-1L, mapping.getNewRowId(2));
        assertEquals(-1L, mapping.getNewRowId(-1));
    }

    /**
     * Size returns total number of entries.
     */
    public void testSize() {
        long[] mappingArray = { 0, 1, 2, 3, 4 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(5, mapping.size());
    }

    /**
     * Memory usage is reported and positive.
     */
    public void testRamBytesUsed() {
        long[] mappingArray = new long[1000];
        for (int i = 0; i < 1000; i++) {
            mappingArray[i] = i;
        }
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertTrue("RAM bytes used should be positive", mapping.ramBytesUsed() > 0);
    }

    /**
     * Empty mapping works correctly.
     */
    public void testEmptyMapping() {
        long[] mappingArray = {};
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(0, mapping.size());
        assertEquals(-1L, mapping.getNewRowId(0));
    }

    /**
     * Null mapping array throws NullPointerException.
     */
    public void testNullArgumentsThrow() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, false));
    }

    /**
     * Reverse lookup works when reverseSupported=true.
     */
    public void testReverseLookup() {
        // Permutation: 0→2, 1→0, 2→1
        long[] mappingArray = { 2, 0, 1 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, true);

        assertTrue(mapping.isNewToOldSupported());
        // Forward: old→new
        assertEquals(2L, mapping.getNewRowId(0));
        assertEquals(0L, mapping.getNewRowId(1));
        assertEquals(1L, mapping.getNewRowId(2));
        // Reverse: new→old
        assertEquals(1L, mapping.getOldRowId(0));
        assertEquals(2L, mapping.getOldRowId(1));
        assertEquals(0L, mapping.getOldRowId(2));
    }

    /**
     * Reverse lookup throws when reverseSupported=false.
     */
    public void testReverseUnsupportedThrows() {
        long[] mappingArray = { 0, 1, 2 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertFalse(mapping.isNewToOldSupported());
        expectThrows(UnsupportedOperationException.class, () -> mapping.getOldRowId(0));
    }

    /**
     * Reverse lookup with out-of-bounds returns -1.
     */
    public void testReverseOutOfBoundsReturnsNegativeOne() {
        long[] mappingArray = { 1, 0 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, true);
        assertEquals(-1L, mapping.getOldRowId(5));
        assertEquals(-1L, mapping.getOldRowId(-1));
    }

    /**
     * Per-generation mappings can be stored in a Map for merge use cases.
     */
    public void testMultiGenerationViaMap() {
        // gen=1: 3 rows, permutation 0→4, 1→3, 2→2
        long[] gen1Array = { 4, 3, 2 };
        // gen=2: 2 rows, permutation 0→1, 1→0
        long[] gen2Array = { 1, 0 };

        Map<Long, RowIdMapping> mappings = new HashMap<>();
        mappings.put(1L, new PackedRowIdMapping(gen1Array, false));
        mappings.put(2L, new PackedRowIdMapping(gen2Array, false));

        RowIdMapping gen1 = mappings.get(1L);
        assertEquals(4L, gen1.getNewRowId(0));
        assertEquals(3L, gen1.getNewRowId(1));
        assertEquals(2L, gen1.getNewRowId(2));

        RowIdMapping gen2 = mappings.get(2L);
        assertEquals(1L, gen2.getNewRowId(0));
        assertEquals(0L, gen2.getNewRowId(1));
    }

    /**
     * toString includes useful debug info.
     */
    public void testToString() {
        long[] mappingArray = { 0, 1, 2 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        String str = mapping.toString();
        assertTrue(str.contains("size=3"));
    }
}
