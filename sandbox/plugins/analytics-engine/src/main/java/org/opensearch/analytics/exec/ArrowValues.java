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
     *
     * <p>Containers ({@link MapVector}, {@link ListVector}) are recursively
     * normalized so no Arrow {@link Text} wrapper escapes this boundary. This
     * matters for nested shapes like {@code Map<String, List<String>>} (e.g.
     * the PPL {@code patterns ... show_numbered_token=true} {@code tokens}
     * column), where Arrow's {@code MapVector.getObject} yields entry structs
     * whose values are themselves {@code List<Text>}; downstream consumers
     * such as {@code ExprValueUtils.fromObjectValue} reject the raw {@code
     * Text} as "unsupported object class".
     */
    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), StandardCharsets.UTF_8);
        }
        // MapVector check must come before the ListVector branch because
        // MapVector extends ListVector. Arrow Map is laid out as
        // List<Struct{keys, values}>, so MapVector.getObject(i) returns a
        // JsonStringArrayList of entry structs rather than a Map. Reassemble
        // entries into a LinkedHashMap (insertion-order preserving) so the
        // downstream ExprValueUtils tuple converter sees the same shape as a
        // legacy v2 Map<String, Object> column. In-tree callers include
        // spath's `json_extract_all`, parse's `parse` UDF, and the BRAIN
        // patterns `tokens` column on the analytics-engine route.
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

    /**
     * Recursively converts Arrow {@link Text} wrappers to {@link String} inside
     * arbitrary nested {@code List} / {@code Map} structures returned by
     * {@code FieldVector#getObject}. Returns primitive values unchanged.
     */
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
