/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ParquetPhysicalType}. The {@code code()} discriminants are exchanged with
 * the Rust FFM bridge ({@code parquet_open_column_reader}'s {@code expected_type}) and MUST stay in
 * lock-step with the {@code TYPE_*} constants in {@code ffm.rs}; these tests pin the exact integer
 * values so a future reorder of the enum cannot silently desync Java from the native side.
 */
public class ParquetPhysicalTypeTests extends OpenSearchTestCase {

    public void testCodeDiscriminantsMatchNativeContract() {
        // Must match the TYPE_* constants in ffm.rs. Do not change without updating the native side.
        assertEquals(0, ParquetPhysicalType.INT32.code());
        assertEquals(1, ParquetPhysicalType.INT64.code());
        assertEquals(2, ParquetPhysicalType.FLOAT.code());
        assertEquals(3, ParquetPhysicalType.DOUBLE.code());
        assertEquals(4, ParquetPhysicalType.BOOL.code());
        assertEquals(5, ParquetPhysicalType.BYTE_ARRAY.code());
    }

    public void testCodesAreUnique() {
        ParquetPhysicalType[] values = ParquetPhysicalType.values();
        boolean[] seen = new boolean[values.length];
        for (ParquetPhysicalType t : values) {
            int c = t.code();
            assertTrue("code out of expected range: " + c, c >= 0 && c < values.length);
            assertFalse("duplicate code " + c + " for " + t, seen[c]);
            seen[c] = true;
        }
    }

    public void testIsFixedWidth() {
        // Fixed-width types exchange values as raw long bits; only BYTE_ARRAY is variable-width.
        assertTrue(ParquetPhysicalType.INT32.isFixedWidth());
        assertTrue(ParquetPhysicalType.INT64.isFixedWidth());
        assertTrue(ParquetPhysicalType.FLOAT.isFixedWidth());
        assertTrue(ParquetPhysicalType.DOUBLE.isFixedWidth());
        assertTrue(ParquetPhysicalType.BOOL.isFixedWidth());
        assertFalse(ParquetPhysicalType.BYTE_ARRAY.isFixedWidth());
    }
}
