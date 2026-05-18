/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link PackedRowIdMapping}.
 */
public class PackedRowIdMappingTests extends OpenSearchTestCase {

    public void testForwardLookup() {
        long[] mapping = { 2, 0, 1 };
        PackedRowIdMapping m = new PackedRowIdMapping(mapping, false);
        assertEquals(2L, m.getNewRowId(0));
        assertEquals(0L, m.getNewRowId(1));
        assertEquals(1L, m.getNewRowId(2));
        assertEquals(3, m.size());
    }

    public void testWithReverseSupport() {
        long[] mapping = { 2, 0, 1 };
        PackedRowIdMapping m = new PackedRowIdMapping(mapping, true);
        assertTrue(m.isNewToOldSupported());
        assertEquals(2L, m.getNewRowId(0));
        assertEquals(0L, m.getNewRowId(1));
        assertEquals(1L, m.getNewRowId(2));
        assertEquals(1L, m.getOldRowId(0));
        assertEquals(2L, m.getOldRowId(1));
        assertEquals(0L, m.getOldRowId(2));
    }

    public void testReverseUnsupportedThrows() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 0, 1 }, false);
        assertFalse(m.isNewToOldSupported());
        expectThrows(UnsupportedOperationException.class, () -> m.getOldRowId(0));
    }

    public void testOutOfBoundsReturnsNegativeOne() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 5, 6 }, false);
        assertEquals(-1L, m.getNewRowId(2));
        assertEquals(-1L, m.getNewRowId(-1));
    }

    public void testReverseOutOfBoundsReturnsNegativeOne() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 1, 0 }, true);
        assertEquals(-1L, m.getOldRowId(-1));
        assertEquals(-1L, m.getOldRowId(2));
    }

    public void testRamBytesUsedPositive() {
        long[] arr = new long[100];
        for (int i = 0; i < 100; i++) arr[i] = i;
        PackedRowIdMapping m = new PackedRowIdMapping(arr, false);
        assertTrue(m.ramBytesUsed() > 0);
    }

    public void testRamBytesUsedWithReverse() {
        PackedRowIdMapping withReverse = new PackedRowIdMapping(new long[] { 1, 0 }, true);
        PackedRowIdMapping withoutReverse = new PackedRowIdMapping(new long[] { 1, 0 }, false);
        assertTrue(withReverse.ramBytesUsed() > withoutReverse.ramBytesUsed());
    }

    public void testEmptyMapping() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] {}, false);
        assertEquals(0, m.size());
        assertEquals(-1L, m.getNewRowId(0));
    }

    public void testNullForwardThrows() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, false));
    }

    public void testPackedLongValuesConstructorWithReverse() {
        PackedLongValues.Builder fwd = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        fwd.add(2); fwd.add(0); fwd.add(1);
        PackedLongValues.Builder rev = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        rev.add(1); rev.add(2); rev.add(0);
        PackedRowIdMapping m = new PackedRowIdMapping(fwd.build(), rev.build());
        assertTrue(m.isNewToOldSupported());
        assertEquals(3, m.size());
        assertEquals(2L, m.getNewRowId(0));
        assertEquals(1L, m.getOldRowId(0));
    }

    public void testPackedLongValuesConstructorForwardOnly() {
        PackedLongValues.Builder fwd = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        fwd.add(4); fwd.add(3); fwd.add(2);
        PackedRowIdMapping m = new PackedRowIdMapping(fwd.build(), null);
        assertFalse(m.isNewToOldSupported());
        assertEquals(4L, m.getNewRowId(0));
        expectThrows(UnsupportedOperationException.class, () -> m.getOldRowId(0));
    }

    public void testNullPackedLongValuesForwardThrows() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping((PackedLongValues) null, null));
    }

    public void testToString() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 0, 1, 2 }, false);
        assertTrue(m.toString().contains("size=3"));
    }
}
