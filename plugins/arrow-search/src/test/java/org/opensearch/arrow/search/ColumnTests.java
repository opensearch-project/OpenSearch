/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnTests extends OpenSearchTestCase {

    public void testArrowTypeOfEachMappingType() {
        Map<String, ArrowType> expected = Map.ofEntries(
            Map.entry("byte", new ArrowType.Int(8, true)),
            Map.entry("short", new ArrowType.Int(16, true)),
            Map.entry("integer", new ArrowType.Int(32, true)),
            Map.entry("long", new ArrowType.Int(64, true)),
            Map.entry("unsigned_long", new ArrowType.Int(64, false)),
            Map.entry("half_float", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            Map.entry("float", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            Map.entry("double", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Map.entry("scaled_float", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Map.entry("boolean", ArrowType.Bool.INSTANCE),
            Map.entry("date", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            Map.entry("date_nanos", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
            Map.entry("keyword", ArrowType.Utf8.INSTANCE),
            Map.entry("ip", new ArrowType.FixedSizeBinary(16))
        );
        for (Map.Entry<String, ArrowType> entry : expected.entrySet()) {
            assertTrue(entry.getKey(), Column.isSupported(entry.getKey()));
            Column column = new Column("f", Column.columnType(entry.getKey()), Set.of(entry.getKey()));
            Field field = column.toArrowField();
            assertEquals(entry.getKey(), entry.getValue(), field.getType());
            assertTrue("doc values may be missing", field.isNullable());
            assertEquals(entry.getKey(), field.getMetadata().get(Column.MAPPING_TYPES_METADATA_KEY));
        }
    }

    public void testUnsupportedMappingTypes() {
        for (String type : new String[] { "text", "object", "nested", "geo_point", "wildcard", "_id", "unmapped" }) {
            assertFalse(type, Column.isSupported(type));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Column.columnType(type));
            assertTrue(e.getMessage(), e.getMessage().contains("[" + type + "]"));
        }
    }

    public void testWideningAcrossIndices() {
        assertEquals(ColumnType.INT32, Column.widen(Set.of("integer")));
        assertEquals(ColumnType.INT64, Column.widen(Set.of("byte", "integer")));
        assertEquals(ColumnType.INT64, Column.widen(Set.of("short", "long")));
        assertEquals(ColumnType.FLOAT32, Column.widen(Set.of("half_float", "float")));
        assertEquals(ColumnType.FLOAT64, Column.widen(Set.of("float", "double")));
        assertEquals(ColumnType.FLOAT64, Column.widen(Set.of("float", "scaled_float")));
        assertEquals(ColumnType.FLOAT32, Column.widen(Set.of("half_float")));
        assertEquals(ColumnType.TIMESTAMP_NANOS, Column.widen(Set.of("date", "date_nanos")));
        assertEquals(ColumnType.TIMESTAMP_MILLIS, Column.widen(Set.of("date")));
    }

    public void testIncompatibleTypesDoNotWiden() {
        assertNull(Column.widen(Set.of("long", "double")));
        assertNull(Column.widen(Set.of("long", "unsigned_long")));
        assertNull(Column.widen(Set.of("keyword", "ip")));
        assertNull(Column.widen(Set.of("date", "long")));
        assertNull(Column.widen(Set.of("boolean", "byte")));
    }

    public void testMetadataListsMappingTypesInOrder() {
        Column column = new Column("t", ColumnType.TIMESTAMP_NANOS, Set.of("date_nanos", "date"));
        assertEquals("date,date_nanos", column.toArrowField().getMetadata().get(Column.MAPPING_TYPES_METADATA_KEY));
    }

    public void testListColumnIsANullableListOfValues() {
        Field field = new Column("tags", ColumnType.UTF8, Set.of("keyword"), true).toArrowField();
        assertEquals(ArrowType.List.INSTANCE, field.getType());
        assertTrue("a document without values reads as null", field.isNullable());
        assertEquals("keyword", field.getMetadata().get(Column.MAPPING_TYPES_METADATA_KEY));
        Field item = field.getChildren().get(0);
        assertEquals(Column.LIST_ITEM_NAME, item.getName());
        assertEquals(ArrowType.Utf8.INSTANCE, item.getType());
        assertFalse("doc values hold no null values", item.isNullable());
        assertFalse(new Column("t", ColumnType.UTF8, Set.of("keyword")).list());
    }

    public void testSchemaKeepsColumnOrder() {
        List<Column> columns = List.of(
            new Column("b", ColumnType.UTF8, Set.of("keyword")),
            new Column("a", ColumnType.INT64, Set.of("long"))
        );
        Schema schema = Column.toSchema(columns);
        assertEquals(List.of("b", "a"), schema.getFields().stream().map(Field::getName).toList());
    }

    public void testSerialization() throws IOException {
        Column column = new Column(randomAlphaOfLength(5), ColumnType.INT64, Set.of("long", "integer"), randomBoolean());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            column.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(column, new Column(in));
            }
        }
    }
}
