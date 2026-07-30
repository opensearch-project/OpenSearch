/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Tests for {@link PackedRowIdMapping}.
 */
public class PackedRowIdMappingTests extends OpenSearchTestCase {

    public void testSingleGenConstructorForwardLookup() {
        long[] mapping = { 2, 0, 1 };
        PackedRowIdMapping m = new PackedRowIdMapping(mapping, false);

        assertEquals(2L, m.getNewRowId(0, RowIdMapping.SINGLE_GEN));
        assertEquals(0L, m.getNewRowId(1, RowIdMapping.SINGLE_GEN));
        assertEquals(1L, m.getNewRowId(2, RowIdMapping.SINGLE_GEN));
        assertEquals(3, m.size());
    }

    public void testSingleGenWithReverseSupport() {
        long[] mapping = { 2, 0, 1 };
        PackedRowIdMapping m = new PackedRowIdMapping(mapping, true);

        assertTrue(m.isNewToOldSupported());
        // forward: old 0 -> new 2, old 1 -> new 0, old 2 -> new 1
        assertEquals(2L, m.getNewRowId(0, RowIdMapping.SINGLE_GEN));
        assertEquals(0L, m.getNewRowId(1, RowIdMapping.SINGLE_GEN));
        assertEquals(1L, m.getNewRowId(2, RowIdMapping.SINGLE_GEN));

        // reverse: new 0 -> old 1, new 1 -> old 2, new 2 -> old 0
        assertEquals(1L, m.getOldRowId(0));
        assertEquals(2L, m.getOldRowId(1));
        assertEquals(0L, m.getOldRowId(2));
    }

    public void testReverseUnsupportedThrows() {
        long[] mapping = { 0, 1 };
        PackedRowIdMapping m = new PackedRowIdMapping(mapping, false);

        assertFalse(m.isNewToOldSupported());
        expectThrows(UnsupportedOperationException.class, () -> m.getOldRowId(0));
    }

    public void testMultiGenerationLookup() {
        // gen=1 (3 rows at offset 0): 0->4, 1->3, 2->2
        // gen=2 (2 rows at offset 3): 0->1, 1->0
        long[] mappingArray = { 4, 3, 2, 1, 0 };
        Map<Long, Integer> offsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> sizes = Map.of(1L, 3, 2L, 2);

        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, offsets, sizes);

        assertEquals(4L, m.getNewRowId(0, 1L));
        assertEquals(3L, m.getNewRowId(1, 1L));
        assertEquals(2L, m.getNewRowId(2, 1L));
        assertEquals(1L, m.getNewRowId(0, 2L));
        assertEquals(0L, m.getNewRowId(1, 2L));
        assertEquals(5, m.size());
    }

    public void testUnknownGenerationReturnsNegativeOne() {
        long[] mappingArray = { 0 };
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, Map.of(1L, 0), Map.of(1L, 1));
        assertEquals(-1L, m.getNewRowId(0, 99L));
    }

    public void testOutOfBoundsRowIdReturnsNegativeOne() {
        long[] mappingArray = { 5, 6 };
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, Map.of(1L, 0), Map.of(1L, 2));
        assertEquals(-1L, m.getNewRowId(2, 1L));
        assertEquals(-1L, m.getNewRowId(-1, 1L));
    }

    public void testReverseOutOfBoundsReturnsNegativeOne() {
        long[] mapping = { 1, 0 };
        PackedRowIdMapping m = new PackedRowIdMapping(mapping, true);
        assertEquals(-1L, m.getOldRowId(-1));
        assertEquals(-1L, m.getOldRowId(2));
    }

    public void testGenerationSize() {
        long[] mappingArray = { 0, 1, 2, 3, 4 };
        Map<Long, Integer> offsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> sizes = Map.of(1L, 3, 2L, 2);

        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, offsets, sizes);
        assertEquals(3, m.getGenerationSize(1L));
        assertEquals(2, m.getGenerationSize(2L));
        assertEquals(0, m.getGenerationSize(99L));
    }

    public void testRamBytesUsedPositive() {
        long[] mappingArray = new long[100];
        for (int i = 0; i < 100; i++) {
            mappingArray[i] = i;
        }
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, false);
        assertTrue(m.ramBytesUsed() > 0);
    }

    public void testRamBytesUsedWithReverse() {
        long[] mappingArray = { 1, 0 };
        PackedRowIdMapping withReverse = new PackedRowIdMapping(mappingArray, true);
        PackedRowIdMapping withoutReverse = new PackedRowIdMapping(mappingArray, false);
        assertTrue(withReverse.ramBytesUsed() > withoutReverse.ramBytesUsed());
    }

    public void testEmptyMapping() {
        long[] mappingArray = {};
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, Map.of(), Map.of());
        assertEquals(0, m.size());
        assertEquals(-1L, m.getNewRowId(0, 1L));
    }

    public void testNullArgumentsThrow() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, Map.of(), Map.of()));
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(new long[0], null, Map.of()));
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(new long[0], Map.of(), null));
    }

    public void testGetGenerationOffsets() {
        long[] mappingArray = { 0, 1 };
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 2);
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, offsets, sizes);

        assertEquals(Map.of(1L, 0), m.getGenerationOffsets());
        expectThrows(UnsupportedOperationException.class, () -> m.getGenerationOffsets().put(2L, 1));
    }

    public void testGetGenerationSizes() {
        long[] mappingArray = { 0, 1 };
        Map<Long, Integer> offsets = Map.of(1L, 0);
        Map<Long, Integer> sizes = Map.of(1L, 2);
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, offsets, sizes);

        assertEquals(Map.of(1L, 2), m.getGenerationSizes());
        expectThrows(UnsupportedOperationException.class, () -> m.getGenerationSizes().put(2L, 1));
    }

    public void testToStringContainsInfo() {
        long[] mappingArray = { 0, 1, 2 };
        PackedRowIdMapping m = new PackedRowIdMapping(mappingArray, Map.of(1L, 0), Map.of(1L, 3));
        String str = m.toString();
        assertTrue(str.contains("size=3"));
        assertTrue(str.contains("generations=1"));
    }
}
