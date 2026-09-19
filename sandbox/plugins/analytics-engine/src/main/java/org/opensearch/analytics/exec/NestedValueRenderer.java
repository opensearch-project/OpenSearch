/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Renders a nested {@code LIST<STRUCT>} Arrow value (e.g. an OTel {@code events} column) into the
 * object shape PPL clients expect, matching vanilla OpenSearch's {@code _source}. Scalar and
 * temporal formatting is delegated to {@link ArrowValues}.
 *
 * <p>Two shape rules, driven by the element struct's Arrow child fields:
 * <ul>
 *   <li>A MAP child (a {@code flat_object} like {@code events.attributes}) becomes a nested object
 *       with <b>dotted keys unflattened</b> ({@code {a:{b:v}}}), not the raw
 *       {@code [{"key":..,"value":..}, …]} entry-list Arrow materializes.</li>
 *   <li>A {@code LIST<STRUCT>} child (nested-in-nested) recurses through this renderer, so the rules
 *       apply at every depth.</li>
 * </ul>
 *
 * @opensearch.internal
 */
final class NestedValueRenderer {

    private static final Logger LOGGER = LogManager.getLogger(NestedValueRenderer.class);

    private NestedValueRenderer() {}

    /**
     * Normalizes a nested {@code LIST<STRUCT>} element list. Each element arrives as a {@code Map};
     * we render every child by its Arrow type (see the class javadoc). All fields are kept — vanilla
     * keeps {@code attributes:{}} on an event with no attributes.
     *
     * @param raw         the element list (each entry a {@code Map} of child-name → materialized value)
     * @param structField the element struct {@link Field}, whose children drive per-child rendering
     */
    @SuppressWarnings("unchecked")
    static List<Object> normalizeStructList(List<?> raw, Field structField) {
        LinkedHashMap<String, Field> childByName = new LinkedHashMap<>();
        for (Field c : structField.getChildren()) {
            childByName.put(c.getName(), c);
        }
        List<Object> out = new ArrayList<>(raw.size());
        for (Object element : raw) {
            if (!(element instanceof Map<?, ?> em)) {
                out.add(ArrowValues.normalize(element));
                continue;
            }
            LinkedHashMap<String, Object> obj = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : em.entrySet()) {
                String name = e.getKey() instanceof Text t ? t.toString() : String.valueOf(e.getKey());
                obj.put(name, renderChild(childByName.get(name), e.getValue()));
            }
            out.add(obj);
        }
        return out;
    }

    /**
     * Renders one struct child by its Arrow type: MAP → unflattened nested object, {@code LIST<STRUCT>}
     * → recurse, other list → element-aware formatting, scalar → temporal or plain normalization. An
     * unknown child ({@code null} field) falls back to field-blind normalization.
     */
    private static Object renderChild(Field cf, Object value) {
        if (value == null) {
            return null;
        }
        if (cf == null) {
            return ArrowValues.normalize(value);
        }
        ArrowType type = cf.getType();
        if (type instanceof ArrowType.Map) {
            return mapEntriesToNestedObject(value);
        }
        if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList || type instanceof ArrowType.FixedSizeList) {
            if (value instanceof List<?> childList && !cf.getChildren().isEmpty()) {
                Field element = cf.getChildren().get(0);
                if (element.getType() instanceof ArrowType.Struct) {
                    return normalizeStructList(childList, element);
                }
                return ArrowValues.normalizeList(childList, element);
            }
            return ArrowValues.normalize(value);
        }
        // Scalar / temporal — the child's Arrow type drives date/time formatting.
        Object temporal = ArrowValues.formatTemporal(type, value);
        return temporal != null ? temporal : ArrowValues.normalize(value);
    }

    /**
     * Converts an Arrow MAP value (a list of {@code {key,value}} entries) into a nested object,
     * <b>unflattening dotted keys</b> so {@code feature_flag.result.reason} becomes
     * {@code {feature_flag:{result:{reason:…}}}} — matching vanilla's object-shaped
     * {@code events[*].attributes}. An empty map yields {@code {}}. Values are strings (parquet
     * {@code MAP<Utf8,Utf8>}).
     */
    static Object mapEntriesToNestedObject(Object raw) {
        LinkedHashMap<String, Object> nested = new LinkedHashMap<>();
        if (raw instanceof List<?> entries) {
            for (Object entry : entries) {
                if (!(entry instanceof Map<?, ?> e)) {
                    continue;
                }
                Object k = e.get(MapVector.KEY_NAME);
                Object v = e.get(MapVector.VALUE_NAME);
                String key = k instanceof Text t ? t.toString() : String.valueOf(k);
                insertDotted(nested, key, ArrowValues.normalize(v));
            }
        }
        return nested;
    }

    /**
     * Inserts {@code value} at a dotted {@code path} into {@code root}, creating intermediate objects.
     * Keys can collide when one is a prefix of another ({@code a} vs {@code a.b}) — OTel maps carry
     * both. On collision we keep the first value (scalar or object) and log; a duplicate leaf is
     * last-writer-wins with a log. Malformed paths (empty, leading/trailing/double dots) go under the
     * literal key rather than splitting into empty segments.
     */
    private static void insertDotted(Map<String, Object> root, String path, Object value) {
        if (path.isEmpty() || path.startsWith(".") || path.endsWith(".") || path.contains("..")) {
            putGuarded(root, path, value);
            return;
        }
        Map<String, Object> cur = root;
        int start = 0;
        int dot = path.indexOf('.');
        while (dot >= 0) {
            String seg = path.substring(start, dot);
            Object next = cur.get(seg);
            if (next == null) {
                LinkedHashMap<String, Object> child = new LinkedHashMap<>();
                cur.put(seg, child);
                cur = child;
            } else if (next instanceof Map<?, ?> m) {
                @SuppressWarnings("unchecked")
                Map<String, Object> typed = (Map<String, Object>) m;
                cur = typed;
            } else {
                // An ancestor segment already holds a scalar (e.g. "a" seen before "a.b").
                LOGGER.debug(
                    "flat_object dotted-key collision: key [{}] conflicts with existing scalar at segment [{}] — keeping the scalar, dropping the deeper key",
                    path,
                    seg
                );
                return;
            }
            start = dot + 1;
            dot = path.indexOf('.', start);
        }
        putGuarded(cur, path.substring(start), value);
    }

    /** Puts {@code value} at {@code key}, refusing to overwrite an existing object with a scalar. */
    private static void putGuarded(Map<String, Object> map, String key, Object value) {
        Object prev = map.get(key);
        if (prev instanceof Map) {
            LOGGER.debug("flat_object dotted-key collision: scalar key [{}] conflicts with existing object — keeping the object", key);
            return;
        }
        if (prev != null) {
            LOGGER.debug("flat_object duplicate key [{}] — overwriting previous value", key);
        }
        map.put(key, value);
    }
}
