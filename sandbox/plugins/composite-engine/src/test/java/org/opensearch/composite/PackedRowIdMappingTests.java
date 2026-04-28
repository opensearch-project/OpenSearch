/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Tests for {@link PackedRowIdMapping}.
 */
public class PackedRowIdMappingTests extends OpenSearchTestCase {

    /**
     * Basic lookup: two generations with known mappings.
     * gen=1 (3 rows): 0→4, 1→3, 2→2
     * gen=2 (2 rows): 0→1, 1→0
     */
    public void testBasicLookup() {
        long[] mappingArray = { 4, 3, 2, 1, 0 };
        Map<Long, Integer> offsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> sizes = Map.of(1L, 3, 2L, 2);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);

        // gen=1 lookups
        assertEquals(4L, mapping.getNewRowId(0, 1L));
        assertEquals(3L, mapping.getNewRowId(1, 1L));
        assertEquals(2L, mapping.getNewRowId(2, 1L));

        // gen=2 lookups
        assertEquals(1L, mapping.getNewRowId(0, 2L));
        assertEquals(0L, mapping.getNewRowId(1, 2L));
    }

    /**
     * Implements the RowIdMapping interface correctly.
     */
    public void testImplementsInterface() {
        long[] mappingArray = { 10, 20 };
        Map<Long, Integer> offsets = Map.of(5L, 0);
        Map<Long, Integer> sizes = Map.of(5L, 2);

        RowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(10L, mapping.getNewRowId(0, 5L));
        assertEquals(20L, mapping.getNewRowId(1, 5L));
    }

    /**
     * Unknown generation returns -1.
     */
    public void testUnknownGenerationReturnsNegativeOne() {
        long[] mappingArray = { 0 };
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 1);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(-1L, mapping.getNewRowId(0, 99L));
    }

    /**
     * Out-of-bounds row ID returns -1.
     */
    public void testOutOfBoundsRowIdReturnsNegativeOne() {
        long[] mappingArray = { 5, 6 };
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 2);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(-1L, mapping.getNewRowId(2, 1L));
        assertEquals(-1L, mapping.getNewRowId(-1, 1L));
    }

    /**
     * Size returns total number of entries.
     */
    public void testSize() {
        long[] mappingArray = { 0, 1, 2, 3, 4 };
        Map<Long, Integer> offsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> sizes = Map.of(1L, 3, 2L, 2);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(5, mapping.size());
    }

    /**
     * Generation size returns correct count per generation.
     */
    public void testGenerationSize() {
        long[] mappingArray = { 0, 1, 2, 3, 4 };
        Map<Long, Integer> offsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> sizes = Map.of(1L, 3, 2L, 2);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(3, mapping.getGenerationSize(1L));
        assertEquals(2, mapping.getGenerationSize(2L));
        assertEquals(0, mapping.getGenerationSize(99L));
    }

    /**
     * Memory usage is reported and positive.
     */
    public void testRamBytesUsed() {
        long[] mappingArray = new long[1000];
        for (int i = 0; i < 1000; i++) {
            mappingArray[i] = i;
        }
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 1000);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertTrue("RAM bytes used should be positive", mapping.ramBytesUsed() > 0);
    }

    /**
     * Empty mapping works correctly.
     */
    public void testEmptyMapping() {
        long[] mappingArray = {};
        Map<Long, Integer> offsets = Map.of();
        Map<Long, Integer> sizes = Map.of();

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(0, mapping.size());
        assertEquals(-1L, mapping.getNewRowId(0, 1L));
    }

    /**
     * Null arguments throw NullPointerException.
     */
    public void testNullArgumentsThrow() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, Map.of(), Map.of()));
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(new long[0], null, Map.of()));
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(new long[0], Map.of(), null));
    }

    /**
     * Generation offsets and sizes maps are unmodifiable.
     */
    public void testMapsAreUnmodifiable() {
        long[] mappingArray = { 0 };
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 1);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        expectThrows(UnsupportedOperationException.class, () -> mapping.getGenerationOffsets().put(2L, 1));
        expectThrows(UnsupportedOperationException.class, () -> mapping.getGenerationSizes().put(2L, 1));
    }

    /**
     * Three generations with non-sequential offsets (simulating real merge order).
     */
    public void testThreeGenerationsNonSequentialOrder() {
        // Merge processes generations in order [5, 0, 3]
        // gen=5 (2 rows): offset=0, mapping[0]=2, mapping[1]=3
        // gen=0 (3 rows): offset=2, mapping[2]=0, mapping[3]=4, mapping[4]=1
        // gen=3 (1 row): offset=5, mapping[5]=5
        long[] mappingArray = { 2, 3, 0, 4, 1, 5 };
        Map<Long, Integer> offsets = Map.of(5L, 0, 0L, 2, 3L, 5);
        Map<Long, Integer> sizes = Map.of(5L, 2, 0L, 3, 3L, 1);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);

        assertEquals(2L, mapping.getNewRowId(0, 5L));
        assertEquals(3L, mapping.getNewRowId(1, 5L));
        assertEquals(0L, mapping.getNewRowId(0, 0L));
        assertEquals(4L, mapping.getNewRowId(1, 0L));
        assertEquals(1L, mapping.getNewRowId(2, 0L));
        assertEquals(5L, mapping.getNewRowId(0, 3L));

        assertEquals(6, mapping.size());
    }

    /**
     * toString includes useful debug info.
     */
    public void testToString() {
        long[] mappingArray = { 0, 1, 2 };
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 3);

        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, offsets, sizes);
        String str = mapping.toString();
        assertTrue(str.contains("size=3"));
        assertTrue(str.contains("generations=1"));
        assertTrue(str.contains("estimatedMemoryBytes="));
    }
}
