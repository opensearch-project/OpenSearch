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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests for {@link ArrowValues#toJavaValue}.
 *
 * <p>Covers the conversions that matter at the SQL-API boundary, where downstream
 * code only recognises plain Java types ({@link String}, {@link List}, etc.) and
 * blows up on Arrow's {@code Text} / {@code JsonStringArrayList} wrappers.
 */
public class ArrowValuesTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testNullCellReturnsNull() {
        try (IntVector v = new IntVector("x", allocator)) {
            v.allocateNew(1);
            v.setNull(0);
            v.setValueCount(1);
            assertNull(ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testVarCharVectorReturnsPlainString() {
        try (VarCharVector v = new VarCharVector("name", allocator)) {
            v.allocateNew(1);
            v.set(0, "hello".getBytes(StandardCharsets.UTF_8));
            v.setValueCount(1);
            Object result = ArrowValues.toJavaValue(v, 0);
            assertEquals(String.class, result.getClass());
            assertEquals("hello", result);
        }
    }

    /**
     * Regression test for the take() rendering bug: a ListVector wrapping a
     * VarCharVector must come out as a plain {@code List<String>}, not as
     * Arrow's {@code JsonStringArrayList<Text>}. The old code returned the raw
     * {@code getObject()} value, whose elements are {@code Text} (not String);
     * downstream serialization would then either fail or stringify the list to
     * its {@code toString()} (a JSON-encoded string).
     */
    public void testListVectorOfStringReturnsListOfString() {
        try (ListVector listVector = ListVector.empty("take", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
            UnionListWriter writer = listVector.getWriter();
            writer.startList();
            writer.varChar().writeVarChar("Amber JOHnny");
            writer.varChar().writeVarChar("Hattie");
            writer.endList();
            writer.setValueCount(1);

            Object result = ArrowValues.toJavaValue(listVector, 0);

            assertTrue("expected List, got " + result.getClass(), result instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) result;
            assertEquals(2, list.size());
            assertEquals(String.class, list.get(0).getClass());
            assertEquals("Amber JOHnny", list.get(0));
            assertEquals("Hattie", list.get(1));
        }
    }

    /**
     * Empty list elements (e.g. {@code take(col, 0)}) must round-trip as an
     * empty Java list, not as null.
     */
    public void testListVectorEmptyReturnsEmptyList() {
        try (ListVector listVector = ListVector.empty("take", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
            UnionListWriter writer = listVector.getWriter();
            writer.startList();
            writer.endList();
            writer.setValueCount(1);

            Object result = ArrowValues.toJavaValue(listVector, 0);
            assertTrue(result instanceof List);
            assertEquals(0, ((List<?>) result).size());
        }
    }

    /**
     * Lists of integers should also unwrap recursively — the inner Int vector
     * already returns boxed Integers from getObject, so this is a sanity check
     * that the recursion handles non-VarChar inner types correctly.
     */
    public void testListVectorOfIntReturnsListOfInteger() {
        try (ListVector listVector = ListVector.empty("ids", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Int(32, true)));
            UnionListWriter writer = listVector.getWriter();
            writer.startList();
            writer.integer().writeInt(7);
            writer.integer().writeInt(11);
            writer.endList();
            writer.setValueCount(1);

            Object result = ArrowValues.toJavaValue(listVector, 0);
            assertTrue(result instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) result;
            assertEquals(List.of(7, 11), list);
        }
    }
}
