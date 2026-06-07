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
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads Arrow vector cells as plain Java values, unwrapping Arrow {@link Text} recursively. */
public final class ArrowValues {

    private ArrowValues() {}

    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), StandardCharsets.UTF_8);
        }
        // MapVector extends ListVector — must come first.
        if (vector instanceof MapVector && vector.getObject(index) instanceof List<?> entries) {
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            for (Object entry : entries) {
                if (!(entry instanceof Map<?, ?> e)) continue;
                Object k = e.get(MapVector.KEY_NAME);
                Object v = e.get(MapVector.VALUE_NAME);
                map.put(k instanceof Text t ? t.toString() : String.valueOf(k), normalize(v));
            }
            return map;
        }
        Object value = vector.getObject(index);
        if (vector instanceof ListVector && value instanceof List<?> raw) {
            return normalizeList(raw);
        }
        return normalize(value);
    }

    private static Object normalize(Object value) {
        if (value instanceof Text t) {
            return t.toString();
        }
        if (value instanceof List<?> list) {
            return normalizeList(list);
        }
        if (value instanceof Map<?, ?> m) {
            LinkedHashMap<String, Object> out = new LinkedHashMap<>(m.size());
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                Object k = entry.getKey();
                out.put(k instanceof Text t ? t.toString() : String.valueOf(k), normalize(entry.getValue()));
            }
            return out;
        }
        return value;
    }

    private static List<Object> normalizeList(List<?> raw) {
        List<Object> out = new ArrayList<>(raw.size());
        for (Object element : raw) {
            out.add(normalize(element));
        }
        return out;
    }
}
