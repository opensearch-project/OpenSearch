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

public class PackedRowIdMappingTests extends OpenSearchTestCase {

    public void testForwardLookup() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 2, 0, 1 }, false);
        assertEquals(2L, m.getNewRowId(0));
        assertEquals(0L, m.getNewRowId(1));
        assertEquals(1L, m.getNewRowId(2));
        assertEquals(3, m.size());
    }

    public void testWithReverseSupport() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 2, 0, 1 }, true);
        assertTrue(m.isNewToOldSupported());
        assertEquals(2L, m.getNewRowId(0));
        assertEquals(1L, m.getOldRowId(0));
        assertEquals(2L, m.getOldRowId(1));
        assertEquals(0L, m.getOldRowId(2));
    }

    public void testReverseUnsupportedThrows() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 0, 1 }, false);
        assertFalse(m.isNewToOldSupported());
        expectThrows(UnsupportedOperationException.class, () -> m.getOldRowId(0));
    }

    public void testOutOfBounds() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 5, 6 }, false);
        assertEquals(-1L, m.getNewRowId(2));
        assertEquals(-1L, m.getNewRowId(-1));
    }

    public void testReverseOutOfBounds() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 1, 0 }, true);
        assertEquals(-1L, m.getOldRowId(-1));
        assertEquals(-1L, m.getOldRowId(2));
    }

    public void testEmpty() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] {}, false);
        assertEquals(0, m.size());
        assertEquals(-1L, m.getNewRowId(0));
    }

    public void testNullThrows() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, false));
    }

    public void testPackedLongValuesConstructor() {
        PackedLongValues.Builder fwd = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        fwd.add(2);
        fwd.add(0);
        fwd.add(1);
        PackedLongValues.Builder rev = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        rev.add(1);
        rev.add(2);
        rev.add(0);
        PackedRowIdMapping m = new PackedRowIdMapping(fwd.build(), rev.build());
        assertTrue(m.isNewToOldSupported());
        assertEquals(2L, m.getNewRowId(0));
        assertEquals(1L, m.getOldRowId(0));
    }

    public void testPackedLongValuesForwardOnly() {
        PackedLongValues.Builder fwd = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        fwd.add(4);
        fwd.add(3);
        PackedRowIdMapping m = new PackedRowIdMapping(fwd.build(), null);
        assertFalse(m.isNewToOldSupported());
        assertEquals(4L, m.getNewRowId(0));
    }

    public void testRamBytesUsed() {
        PackedRowIdMapping m = new PackedRowIdMapping(new long[] { 1, 0 }, true);
        assertTrue(m.ramBytesUsed() > 0);
    }
}
