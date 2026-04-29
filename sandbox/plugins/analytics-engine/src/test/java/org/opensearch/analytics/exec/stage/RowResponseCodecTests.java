/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link RowResponseCodec}'s row-Object[] → Arrow conversion.
 *
 * <p>Includes a regression test for the {@code take()} list-rendering bug: when
 * a row cell is a {@link List} (e.g. produced by a list-typed aggregate like
 * {@code take(stringField, n)}), the codec must infer a list-typed Arrow column
 * and populate a {@link ListVector} — NOT fall through to {@code Utf8} and
 * stringify the list via {@code toString()}.
 */
public class RowResponseCodecTests extends OpenSearchTestCase {

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

    /**
     * Regression test for the take() rendering bug. When the cell value is a
     * {@code List<String>}, the codec must produce a {@code ListVector(Utf8)}
     * column whose values, when read back, equal the original list.
     */
    public void testListOfStringsCellDecodesAsListVector() {
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("take"),
            java.util.Arrays.<Object[]>asList(new Object[] { List.of("Amber JOHnny", "Hattie") })
        );

        try (VectorSchemaRoot vsr = RowResponseCodec.INSTANCE.decode(response, allocator)) {
            FieldVector vec = vsr.getVector("take");
            assertTrue(
                "expected ListVector for list-typed cell, got " + vec.getClass().getSimpleName(),
                vec instanceof ListVector
            );
            assertEquals(1, vsr.getRowCount());

            ListVector listVector = (ListVector) vec;
            int start = listVector.getElementStartIndex(0);
            int end = listVector.getElementEndIndex(0);
            assertEquals(2, end - start);

            org.apache.arrow.vector.VarCharVector inner =
                (org.apache.arrow.vector.VarCharVector) listVector.getDataVector();
            assertEquals("Amber JOHnny", new String(inner.get(start), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals("Hattie", new String(inner.get(start + 1), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /**
     * Empty list rows must produce a ListVector with an empty list (no elements
     * for that row), not a null cell. Mirrors {@code take(col, 0)} semantics.
     */
    public void testEmptyListCellDecodesAsEmptyListVectorEntry() {
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("take"),
            java.util.Arrays.<Object[]>asList(new Object[] { List.of() })
        );

        try (VectorSchemaRoot vsr = RowResponseCodec.INSTANCE.decode(response, allocator)) {
            FieldVector vec = vsr.getVector("take");
            assertTrue(vec instanceof ListVector);
            ListVector listVector = (ListVector) vec;
            int start = listVector.getElementStartIndex(0);
            int end = listVector.getElementEndIndex(0);
            assertEquals(0, end - start);
            assertFalse("empty list is not null", listVector.isNull(0));
        }
    }

    /**
     * Regression test for the actual runtime shape: cells produced by
     * {@code DatafusionResultStream.getFieldValue} for a list-typed Arrow
     * column come out as {@code JsonStringArrayList<Text>}, not
     * {@code List<String>}. Arrow's {@code Text} doesn't implement
     * {@link CharSequence}, so the codec must explicitly recognise it.
     */
    public void testListOfArrowTextDecodesAsListVector() {
        org.apache.arrow.vector.util.JsonStringArrayList<org.apache.arrow.vector.util.Text> arrowList =
            new org.apache.arrow.vector.util.JsonStringArrayList<>();
        arrowList.add(new org.apache.arrow.vector.util.Text("Amber JOHnny"));
        arrowList.add(new org.apache.arrow.vector.util.Text("Hattie"));

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("take"),
            java.util.Arrays.<Object[]>asList(new Object[] { arrowList })
        );

        try (VectorSchemaRoot vsr = RowResponseCodec.INSTANCE.decode(response, allocator)) {
            FieldVector vec = vsr.getVector("take");
            assertTrue(vec instanceof ListVector);
            ListVector listVector = (ListVector) vec;
            int start = listVector.getElementStartIndex(0);
            int end = listVector.getElementEndIndex(0);
            assertEquals(2, end - start);

            org.apache.arrow.vector.VarCharVector inner =
                (org.apache.arrow.vector.VarCharVector) listVector.getDataVector();
            assertEquals("Amber JOHnny", new String(inner.get(start), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals("Hattie", new String(inner.get(start + 1), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /**
     * A row whose first value is null but later rows have non-null values
     * should still infer the right element type for the list. Sanity check.
     */
    public void testInferArrowTypeRecognisesListOfStrings() {
        ArrowType inferred = RowResponseCodec.inferArrowType(
            java.util.Arrays.<Object[]>asList(new Object[] { null }, new Object[] { List.of("a", "b") }),
            0
        );
        assertEquals(ArrowType.List.INSTANCE, inferred);
    }
}
