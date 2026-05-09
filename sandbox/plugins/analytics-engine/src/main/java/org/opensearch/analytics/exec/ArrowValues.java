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
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for reading Arrow vector cells as plain Java values at the
 * external query API edge.
 */
public final class ArrowValues {

    private ArrowValues() {}

    /**
     * Returns the cell at {@code index} in {@code vector} as a Java value:
     * {@code null} when the cell is null, a UTF-8 {@link String} for
     * {@link VarCharVector} cells (rather than the raw {@code Text} that
     * {@code getObject} returns), {@link Text#toString()} for any other vector
     * type whose {@code getObject} returns a {@link Text} and
     * {@link FieldVector#getObject} for every other vector type.
     */
    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), StandardCharsets.UTF_8);
        }
        Object value = vector.getObject(index);
        if (vector instanceof ListVector && value instanceof List<?> raw) {
            // ListVector.getObject returns a JsonStringArrayList whose elements are the
            // child vector's typed values. For VarCharVector children that's Arrow's
            // Text, which downstream consumers (e.g. {@code ExprValueUtils.fromObjectValue})
            // don't recognize and reject as "unsupported object class". Mirror the
            // top-level VarCharVector branch above and substitute Java strings.
            List<Object> normalized = new ArrayList<>(raw.size());
            for (Object element : raw) {
                normalized.add(element instanceof Text t ? t.toString() : element);
            }
            return normalized;
        }
        if (value instanceof Text t) {
            return t.toString();
        }
        return value;
    }
}
