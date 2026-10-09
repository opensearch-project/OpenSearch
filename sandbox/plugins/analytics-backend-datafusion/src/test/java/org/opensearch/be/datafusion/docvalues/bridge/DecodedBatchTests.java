/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Bit-addressing tests for {@link DecodedBatch#KIND_BOOL} over hand-built buffers. A cursor batch
 * always starts at {@code valueBitOffset} 0, so the end-to-end tests never drive the non-zero
 * offsets Arrow produces when it slices a bit-packed array mid-byte; an off-by-one in the byte or
 * bit index would pass them. These pin offsets inside a byte, at the last bit of a byte, and past
 * the first byte boundary.
 *
 * <p>Also pins {@link DecodedBatch#nextPresentRow} over hand-built presence bitmaps, where every
 * combination of start mask, all-null byte skip, and batch-tail guard can be driven directly.
 */
public class DecodedBatchTests extends OpenSearchTestCase {

    public void testNextPresentRowOnDenseBatchIsIdentity() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment values = arena.allocate(16 * Long.BYTES);
            // No presence bitmap: every row is present, so the answer is always the query row.
            DecodedBatch batch = new DecodedBatch(100, 115, values, DecodedBatch.KIND_LONG, 0, null, 0);
            assertEquals(100, batch.nextPresentRow(100));
            assertEquals(107, batch.nextPresentRow(107));
            assertEquals(115, batch.nextPresentRow(115));
        }
    }

    public void testNextPresentRowScansSparseBitmapWithBitOffset() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment values = arena.allocate(32 * Long.BYTES);
            // Rows 0..26 map to bits 1..27 (presenceBitOffset 1). Set bits: 1 (row 0), 3 (row 2),
            // 20 (row 19), and 28 -- which is past the batch's last bit and must never be reported.
            MemorySegment presence = arena.allocate(4);
            presence.set(ValueLayout.JAVA_BYTE, 0, (byte) 0x0A);
            presence.set(ValueLayout.JAVA_BYTE, 2, (byte) 0x10);
            presence.set(ValueLayout.JAVA_BYTE, 3, (byte) 0x10);
            DecodedBatch batch = new DecodedBatch(0, 26, values, DecodedBatch.KIND_LONG, 0, presence, 1);

            assertEquals("first set bit in the start byte", 0, batch.nextPresentRow(0));
            assertEquals("start mask must hide the lower set bit", 2, batch.nextPresentRow(1));
            assertEquals("skips the all-null byte between bits 3 and 20", 19, batch.nextPresentRow(3));
            assertEquals("set bit past the batch tail is not reported", -1, batch.nextPresentRow(20));
            assertEquals("query on the last row of an all-null tail", -1, batch.nextPresentRow(26));

            IndexOutOfBoundsException e = expectThrows(IndexOutOfBoundsException.class, () -> batch.nextPresentRow(27));
            assertTrue(e.getMessage().contains("outside batch"));
        }
    }

    public void testBooleanReadsHonourNonZeroBitOffsets() {
        try (Arena arena = Arena.ofConfined()) {
            // Bits 0..15, LSB-first per byte: pattern = bit index i is set iff i % 3 == 0.
            // Byte 0 holds bits 0..7 (0b01001001 = 0x49), byte 1 holds bits 8..15 (0b10010010 = 0x92).
            MemorySegment values = arena.allocate(2);
            values.set(ValueLayout.JAVA_BYTE, 0, (byte) 0x49);
            values.set(ValueLayout.JAVA_BYTE, 1, (byte) 0x92);
            // All rows present: presence reads share the same bit addressing.
            MemorySegment presence = arena.allocate(2);
            presence.set(ValueLayout.JAVA_BYTE, 0, (byte) 0xFF);
            presence.set(ValueLayout.JAVA_BYTE, 1, (byte) 0xFF);

            for (int offset : new int[] { 1, 7, 9 }) {
                int rows = 16 - offset;
                DecodedBatch batch = new DecodedBatch(0, rows - 1, values, DecodedBatch.KIND_BOOL, offset, presence, offset);
                for (int row = 0; row < rows; row++) {
                    long expected = (offset + row) % 3 == 0 ? 1L : 0L;
                    assertEquals("offset " + offset + " row " + row + " (bit " + (offset + row) + ")", expected, batch.valueAt(row));
                }
            }
        }
    }
}
