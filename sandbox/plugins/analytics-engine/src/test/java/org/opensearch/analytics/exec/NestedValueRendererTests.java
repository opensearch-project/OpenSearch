/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for the nested {@code LIST<STRUCT>} shape rendering (map-unflatten + nested-in-nested + collisions). */
public class NestedValueRendererTests extends OpenSearchTestCase {

    private static Field scalar(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private static Field mapField(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of());
    }

    private static Field struct(String name, List<Field> children) {
        return new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
    }

    private static Field listOfStruct(String name, List<Field> structChildren) {
        Field element = struct("$data$", structChildren);
        return new Field(name, FieldType.nullable(new ArrowType.List()), List.of(element));
    }

    /** Mimics how Arrow's {@code getObject} materializes one MAP entry. */
    private static Map<String, Object> entry(String key, String value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(MapVector.KEY_NAME, key);
        m.put(MapVector.VALUE_NAME, value);
        return m;
    }

    private static Map<String, Object> element(Object... nameValuePairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            m.put((String) nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return m;
    }

    public void testMapChildUnflattensDottedKeys() {
        Field structField = struct("events", List.of(scalar("name"), mapField("attributes")));
        Map<String, Object> el = element(
            "name",
            "message",
            "attributes",
            List.of(entry("http.method", "GET"), entry("http.status", "200"), entry("msg", "hi"))
        );

        List<Object> out = NestedValueRenderer.normalizeStructList(List.of(el), structField);

        assertEquals(1, out.size());
        Map<?, ?> obj = (Map<?, ?>) out.get(0);
        assertEquals("message", obj.get("name"));
        Map<?, ?> attrs = (Map<?, ?>) obj.get("attributes");
        Map<?, ?> http = (Map<?, ?>) attrs.get("http");
        assertEquals("GET", http.get("method"));
        assertEquals("200", http.get("status"));
        assertEquals("hi", attrs.get("msg"));
    }

    public void testNestedInNestedRendersInnerMapAsObject() {
        // events[*].sub[*].attributes — the inner attributes MAP must render as an object, not the raw
        // [{"key":..,"value":..}] entry-list (the depth-1 bug, applied recursively at depth 2).
        Field inner = listOfStruct("sub", List.of(scalar("id"), mapField("attributes")));
        Field structField = struct("events", List.of(scalar("name"), inner));

        Map<String, Object> innerEl = element("id", "1", "attributes", List.of(entry("k", "v")));
        Map<String, Object> el = element("name", "outer", "sub", List.of(innerEl));

        List<Object> out = NestedValueRenderer.normalizeStructList(List.of(el), structField);

        Map<?, ?> obj = (Map<?, ?>) out.get(0);
        List<?> sub = (List<?>) obj.get("sub");
        Map<?, ?> subObj = (Map<?, ?>) sub.get(0);
        Map<?, ?> attrs = (Map<?, ?>) subObj.get("attributes");
        assertEquals("v", attrs.get("k"));
    }

    public void testDottedKeyPrefixCollisionKeepsScalarAndDoesNotThrow() {
        // "a" (scalar) arrives before "a.b" — first-writer-wins: keep the scalar, drop the deeper key.
        Object shaped = NestedValueRenderer.mapEntriesToNestedObject(List.of(entry("a", "scalar"), entry("a.b", "deep")));
        Map<?, ?> m = (Map<?, ?>) shaped;
        assertEquals(1, m.size());
        assertEquals("scalar", m.get("a"));
    }

    public void testObjectThenScalarCollisionKeepsObject() {
        // "a.b" builds object {a:{b:..}} first; a later scalar "a" must not clobber the object.
        Object shaped = NestedValueRenderer.mapEntriesToNestedObject(List.of(entry("a.b", "deep"), entry("a", "scalar")));
        Map<?, ?> m = (Map<?, ?>) shaped;
        Map<?, ?> a = (Map<?, ?>) m.get("a");
        assertEquals("deep", a.get("b"));
    }

    public void testEmptyMapYieldsEmptyObject() {
        assertEquals(Map.of(), NestedValueRenderer.mapEntriesToNestedObject(List.of()));
    }

    private static Field timestampField(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)), null);
    }

    private static Field listOfScalar(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.List()), List.of(scalar("$data$")));
    }

    public void testTemporalStructChildIsFormatted() {
        // A timestamp child is formatted via its Arrow type (the field-aware path), not left raw.
        Field structField = struct("events", List.of(scalar("name"), timestampField("time")));
        Map<String, Object> el = element("name", "message", "time", LocalDateTime.of(2026, 1, 2, 3, 4, 5));

        List<Object> out = NestedValueRenderer.normalizeStructList(List.of(el), structField);

        assertEquals("2026-01-02 03:04:05", ((Map<?, ?>) out.get(0)).get("time"));
    }

    public void testScalarListStructChildKeepsElements() {
        Field structField = struct("events", List.of(scalar("name"), listOfScalar("tags")));
        Map<String, Object> el = element("name", "message", "tags", List.of("a", "b"));

        List<Object> out = NestedValueRenderer.normalizeStructList(List.of(el), structField);

        assertEquals(List.of("a", "b"), ((Map<?, ?>) out.get(0)).get("tags"));
    }

    public void testNullChildValueStaysNull() {
        Field structField = struct("events", List.of(scalar("name")));
        Map<String, Object> el = element("name", null);

        List<Object> out = NestedValueRenderer.normalizeStructList(List.of(el), structField);

        Map<?, ?> obj = (Map<?, ?>) out.get(0);
        assertTrue(obj.containsKey("name"));
        assertNull(obj.get("name"));
    }

    public void testUnknownChildFallsBackToPlainNormalization() {
        // A key not declared in the element struct still renders (field-blind), never throws.
        Field structField = struct("events", List.of(scalar("name")));
        Map<String, Object> el = element("name", "message", "surprise", "v");

        List<Object> out = NestedValueRenderer.normalizeStructList(List.of(el), structField);

        assertEquals("v", ((Map<?, ?>) out.get(0)).get("surprise"));
    }

    public void testMalformedDottedKeyStoredLiterally() {
        // Double-dot / leading / trailing dots aren't split into empty segments — stored verbatim.
        Map<?, ?> m = (Map<?, ?>) NestedValueRenderer.mapEntriesToNestedObject(List.of(entry("a..b", "v")));
        assertEquals("v", m.get("a..b"));
    }
}
