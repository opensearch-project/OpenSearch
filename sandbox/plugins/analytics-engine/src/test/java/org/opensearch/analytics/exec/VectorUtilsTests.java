/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Unit tests for {@link VectorUtils#sanitizeNullViewSlots}.
 *
 * <p>Regression for the QTF late-materialization {@code interleave len=0, index=<garbage>} panic.
 * A Java-built VSR fed across the Arrow C Data Interface can carry view columns (Utf8View /
 * BinaryView) whose null slots were never written — Arrow unsets the validity bit but leaves the
 * 16-byte view as uninitialized garbage. DataFusion's interleave reads the view length prefix
 * unconditionally; a garbage length &gt; 12 dereferences a variadic data buffer that does not exist
 * on an all-null column, panicking. The sanitizer must zero every null view slot before export so
 * it reads back as a valid length-0 inline view.
 */
public class VectorUtilsTests extends OpenSearchTestCase {

    private static final int ELEMENT_SIZE = BaseVariableWidthViewVector.ELEMENT_SIZE;
    private static final long GARBAGE_VIEW_LENGTH = 0x7FFFFFFFL; // far above INLINE_SIZE (12)

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    /** A null Utf8View slot left with a garbage length prefix must be zeroed; the cell stays null. */
    public void testSanitize_zeroesGarbageNullUtf8ViewSlot() {
        try (ViewVarCharVector v = new ViewVarCharVector("v", allocator)) {
            v.allocateNew(64, 3);
            v.setSafe(0, "hi".getBytes(StandardCharsets.UTF_8));
            v.getDataBuffer().setInt((long) 1 * ELEMENT_SIZE, (int) GARBAGE_VIEW_LENGTH);
            v.setNull(1);
            v.getDataBuffer().setInt((long) 2 * ELEMENT_SIZE, (int) GARBAGE_VIEW_LENGTH);
            v.setNull(2);
            v.setValueCount(3);
            assertEquals(GARBAGE_VIEW_LENGTH, Integer.toUnsignedLong(v.getDataBuffer().getInt((long) 1 * ELEMENT_SIZE)));

            try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(v))) {
                VectorUtils.sanitizeNullViewSlots(root);

                assertEquals(0, v.getDataBuffer().getInt((long) 1 * ELEMENT_SIZE));
                assertEquals(0, v.getDataBuffer().getInt((long) 2 * ELEMENT_SIZE));
                assertTrue(v.isNull(1));
                assertTrue(v.isNull(2));
                assertFalse(v.isNull(0));
                assertEquals("hi", new String(v.get(0), StandardCharsets.UTF_8));
            }
        }
    }

    /** BinaryView shares the same 16-byte layout and the same hazard — also covered. */
    public void testSanitize_zeroesGarbageNullBinaryViewSlot() {
        try (ViewVarBinaryVector v = new ViewVarBinaryVector("b", allocator)) {
            v.allocateNew(64, 2);
            v.setSafe(0, new byte[] { 1, 2, 3 });
            v.getDataBuffer().setInt((long) 1 * ELEMENT_SIZE, (int) GARBAGE_VIEW_LENGTH);
            v.setNull(1);
            v.setValueCount(2);

            try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(v))) {
                VectorUtils.sanitizeNullViewSlots(root);

                assertEquals(0, v.getDataBuffer().getInt((long) 1 * ELEMENT_SIZE));
                assertTrue(v.isNull(1));
                assertFalse(v.isNull(0));
            }
        }
    }

    /** Non-view columns (and view columns with no nulls) are left untouched. */
    public void testSanitize_ignoresNonViewAndNonNullColumns() {
        try (BigIntVector ints = new BigIntVector("i", allocator); ViewVarCharVector v = new ViewVarCharVector("v", allocator)) {
            ints.allocateNew(2);
            ints.setSafe(0, 10);
            ints.setSafe(1, 20);
            ints.setValueCount(2);
            v.allocateNew(64, 2);
            v.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
            v.setSafe(1, "bb".getBytes(StandardCharsets.UTF_8));
            v.setValueCount(2);

            try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(ints, v))) {
                VectorUtils.sanitizeNullViewSlots(root); // no nulls anywhere — must be a no-op, no exception

                assertEquals(10, ints.get(0));
                assertEquals("a", new String(v.get(0), StandardCharsets.UTF_8));
                assertEquals("bb", new String(v.get(1), StandardCharsets.UTF_8));
            }
        }
    }
}
