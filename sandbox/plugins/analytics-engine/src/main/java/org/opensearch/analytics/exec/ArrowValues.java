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
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;

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
        Object obj = vector.getObject(index);
        if (obj instanceof Text t) {
            return t.toString();
        }
        return obj;
    }
}
