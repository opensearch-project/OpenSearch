/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for reading Arrow vector cells as plain Java values at the
 * external query API edge.
 */
final class ArrowValues {

    private ArrowValues() {}

    /**
     * Returns the cell at {@code index} in {@code vector} as a Java value:
     * <ul>
     *   <li>{@code null} when the cell is null</li>
     *   <li>UTF-8 {@link String} for {@link VarCharVector} cells (rather than
     *       the raw {@code Text} that {@code getObject} returns)</li>
     *   <li>plain {@link List} for {@link ListVector} cells, recursively
     *       unwrapping each element via this method (avoids leaking Arrow's
     *       {@code JsonStringArrayList<Text>} into downstream code that
     *       only recognises standard Java types)</li>
     *   <li>{@link FieldVector#getObject} for every other vector type</li>
     * </ul>
     */
    static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), StandardCharsets.UTF_8);
        }
        if (vector instanceof ListVector listVector) {
            return listToJavaValue(listVector, index);
        }
        return vector.getObject(index);
    }

    private static List<Object> listToJavaValue(ListVector listVector, int index) {
        int start = listVector.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
        int end = listVector.getOffsetBuffer().getInt((long) (index + 1) * ListVector.OFFSET_WIDTH);
        FieldVector inner = listVector.getDataVector();
        List<Object> result = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            result.add(toJavaValue(inner, i));
        }
        return result;
    }
}
