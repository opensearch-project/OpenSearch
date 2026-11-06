/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnResolverTests extends OpenSearchTestCase {

    private final Map<String, Map<String, FieldCapabilities>> capabilities = new HashMap<>();

    private void field(String name, String type, boolean aggregatable) {
        capabilities.computeIfAbsent(name, k -> new HashMap<>())
            .put(type, new FieldCapabilities(name, type, true, aggregatable, null, null, null, Map.of()));
    }

    private List<Column> resolve(String... requested) {
        return resolve(Arrays.stream(requested).map(pattern -> new ColumnResolver.RequestedField(pattern, false)).toList());
    }

    private List<Column> resolve(List<ColumnResolver.RequestedField> requested) {
        return resolve(requested, 100);
    }

    private List<Column> resolve(List<ColumnResolver.RequestedField> requested, int maxColumns) {
        return ColumnResolver.resolve(requested, new FieldCapabilitiesResponse(new String[] { "a", "b" }, capabilities), maxColumns);
    }

    private static Column column(String name, ColumnType type, String... mappingTypes) {
        return new Column(name, type, Set.of(mappingTypes));
    }

    public void testExplicitFieldsKeepRequestOrderWithoutDuplicates() {
        field("a", "long", true);
        field("b", "keyword", true);
        assertEquals(List.of(column("b", ColumnType.UTF8, "keyword"), column("a", ColumnType.INT64, "long")), resolve("b", "a", "b"));
    }

    public void testPatternExpandsInNameOrderAndSkipsUnusableFields() {
        field("m.z", "long", true);
        field("m.a", "ip", true);
        field("m.text", "text", false);
        field("m.nodv", "long", false);
        field("m.conflict", "long", true);
        field("m.conflict", "keyword", true);
        field("m.unmapped", "unmapped", false);
        field("other", "long", true);
        assertEquals(List.of(column("m.a", ColumnType.IP, "ip"), column("m.z", ColumnType.INT64, "long")), resolve("m.*"));
    }

    public void testPatternAfterExplicitFieldDoesNotRepeatIt() {
        field("x", "long", true);
        field("y", "long", true);
        assertEquals(List.of(column("y", ColumnType.INT64, "long"), column("x", ColumnType.INT64, "long")), resolve("y", "*"));
    }

    public void testFieldMappedInSomeIndicesIsAColumn() {
        field("partial", "long", true);
        field("partial", "unmapped", false);
        assertEquals(List.of(column("partial", ColumnType.INT64, "long")), resolve("partial"));
    }

    public void testCompatibleTypesWiden() {
        field("n", "integer", true);
        field("n", "long", true);
        field("t", "date", true);
        field("t", "date_nanos", true);
        assertEquals(
            List.of(column("n", ColumnType.INT64, "integer", "long"), column("t", ColumnType.TIMESTAMP_NANOS, "date", "date_nanos")),
            resolve("n", "t")
        );
    }

    public void testListRequestsMakeListColumns() {
        field("tags", "keyword", true);
        field("m.x", "long", true);
        field("m.y", "ip", true);
        field("plain", "long", true);
        List<Column> columns = resolve(
            List.of(
                new ColumnResolver.RequestedField("tags", true),
                new ColumnResolver.RequestedField("m.*", true),
                new ColumnResolver.RequestedField("plain", false),
                new ColumnResolver.RequestedField("tags", false)
            )
        );
        assertEquals(
            List.of(
                new Column("tags", ColumnType.UTF8, Set.of("keyword"), true),
                new Column("m.x", ColumnType.INT64, Set.of("long"), true),
                new Column("m.y", ColumnType.IP, Set.of("ip"), true),
                new Column("plain", ColumnType.INT64, Set.of("long"), false)
            ),
            columns
        );
    }

    public void testExplicitUnmappedFieldIsRejected() {
        field("a", "long", true);
        field("only_unmapped", "unmapped", false);
        for (String name : new String[] { "missing", "only_unmapped" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve(name));
            assertTrue(e.getMessage(), e.getMessage().contains("[" + name + "] is not mapped in any target index"));
        }
    }

    public void testExplicitIncompatibleFieldIsRejected() {
        field("v", "long", true);
        field("v", "keyword", true);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("v"));
        assertTrue(e.getMessage(), e.getMessage().contains("incompatible types [keyword, long]"));
    }

    public void testExplicitUnsupportedTypeIsRejected() {
        field("t", "text", false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("t"));
        assertTrue(e.getMessage(), e.getMessage().contains("type [text] is not supported"));
    }

    public void testExplicitFieldWithoutDocValuesIsRejected() {
        field("n", "long", false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("n"));
        assertTrue(e.getMessage(), e.getMessage().contains("no doc values"));
    }

    public void testFieldsInsideNestedFieldsAreRejectedOrSkipped() {
        // Field capabilities also report the parent paths of the fields they return, with type nested where an index nests them.
        field("order", "long", true);
        field("items", "nested", false);
        field("items", "object", false);
        field("items.sku", "keyword", true);
        field("items.part", "object", false);
        field("items.part.id", "long", true);
        field("meta", "object", false);
        field("meta.source", "keyword", true);
        for (String name : new String[] { "items.sku", "items.part.id" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve(name));
            assertTrue(e.getMessage(), e.getMessage().contains("inside the nested field [items]"));
        }
        assertEquals(
            "fields of plain objects stay columns",
            List.of(column("meta.source", ColumnType.UTF8, "keyword"), column("order", ColumnType.INT64, "long")),
            resolve("*")
        );
    }

    public void testNoColumnIsRejected() {
        field("t", "text", false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("t*"));
        assertTrue(e.getMessage(), e.getMessage().contains("selected no field"));
    }

    public void testMoreColumnsThanTheDocValueFieldsLimitAreRejected() {
        // Each target index may map only some of the fields, yet every shard reads every column.
        for (int i = 0; i < 5; i++) {
            field("m.f" + i, "long", true);
            field("m.f" + i, "unmapped", false);
        }
        List<ColumnResolver.RequestedField> pattern = List.of(new ColumnResolver.RequestedField("m.*", false));
        assertEquals(5, resolve(pattern, 5).size());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve(pattern, 4));
        assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains("select [5] fields across the target indices, more than the [4] allowed by [index.max_docvalue_fields_search]")
        );
    }
}
