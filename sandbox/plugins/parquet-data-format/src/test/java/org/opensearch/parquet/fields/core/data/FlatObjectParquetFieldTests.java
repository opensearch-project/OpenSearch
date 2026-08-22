/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlatObjectParquetFieldTests extends OpenSearchTestCase {

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

    public void testArrowFieldIsMapOfUtf8() {
        FlatObjectParquetField field = new FlatObjectParquetField();
        assertTrue(field.getArrowType() instanceof ArrowType.Map);
        Field arrowField = field.toArrowField("attrs", false);
        assertEquals(1, arrowField.getChildren().size());

        Field entries = arrowField.getChildren().get(0);
        assertEquals(MapVector.DATA_VECTOR_NAME, entries.getName());
        assertFalse("entries struct must be non-nullable per the Arrow MAP spec", entries.getFieldType().isNullable());
        assertEquals(2, entries.getChildren().size());

        Field key = entries.getChildren().get(0);
        assertEquals(MapVector.KEY_NAME, key.getName());
        assertEquals(new ArrowType.Utf8(), key.getType());
        assertFalse("map keys must be non-nullable per the Arrow MAP spec", key.getFieldType().isNullable());

        Field value = entries.getChildren().get(1);
        assertEquals(MapVector.VALUE_NAME, value.getName());
        assertEquals(new ArrowType.Utf8(), value.getType());
        assertTrue(value.getFieldType().isNullable());
    }

    /**
     * Without {@code multi_value} the column stays a single MAP cell per document, which already holds
     * any number of entries. The LIST wrapper is only added when arity is declared.
     */
    public void testSingleArityIsNotWrappedInList() {
        Field arrowField = new FlatObjectParquetField().toArrowField("attrs", false);
        assertTrue(arrowField.getType() instanceof ArrowType.Map);
    }

    public void testWritesEntriesPreservingOrderAndDuplicates() {
        FlatObjectParquetField field = new FlatObjectParquetField();
        MappedFieldType ft = flatObjectType("attrs");
        ManagedVSR vsr = createVSR(field);

        // Row 0: three entries including a duplicate key ({"a": [1, 2]} shape) in document order.
        field.createField(ft, vsr, List.of(Map.entry("http.status", "500"), Map.entry("a", "1"), Map.entry("a", "2")));
        vsr.setRowCount(1);
        // Row 1: absent field → null cell.
        field.createField(ft, vsr, null);
        vsr.setRowCount(2);
        // Row 2: explicit empty object → zero-entry, non-null map.
        field.createField(ft, vsr, List.of());
        vsr.setRowCount(3);
        // Row 3: a null value inside an entry stays null, key is preserved.
        field.createField(ft, vsr, List.of(new AbstractMap.SimpleEntry<String, String>("missing", null)));
        vsr.setRowCount(4);

        MapVector mapVector = (MapVector) vsr.getVector("attrs");
        assertEquals(
            List.of(Map.entry("http.status", "500"), Map.entry("a", "1"), Map.entry("a", "2")),
            mapEntries(mapVector, 0).stream().map(e -> Map.entry(e.getKey(), e.getValue())).toList()
        );
        assertTrue("absent field must read back as a null map", mapVector.isNull(1));
        assertFalse("empty object must read back as a non-null map", mapVector.isNull(2));
        assertEquals(0, mapEntries(mapVector, 2).size());
        List<Map.Entry<String, String>> row3 = mapEntries(mapVector, 3);
        assertEquals(1, row3.size());
        assertEquals("missing", row3.get(0).getKey());
        assertNull(row3.get(0).getValue());

        cleanupVSR(vsr);
    }

    private static MappedFieldType flatObjectType(String name) {
        return new FlatObjectFieldMapper.FlatObjectFieldType(name, null, true, true);
    }

    /** Reads back one row of a map vector as (key, value) pairs, preserving order and duplicates. */
    private static List<Map.Entry<String, String>> mapEntries(MapVector mapVector, int row) {
        int start = mapVector.getOffsetBuffer().getInt((long) row * 4);
        int end = mapVector.getOffsetBuffer().getInt((long) (row + 1) * 4);
        StructVector entries = (StructVector) mapVector.getDataVector();
        VarCharVector keys = (VarCharVector) entries.getChild(MapVector.KEY_NAME);
        VarCharVector values = (VarCharVector) entries.getChild(MapVector.VALUE_NAME);
        List<Map.Entry<String, String>> result = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            String key = new String(keys.get(i), StandardCharsets.UTF_8);
            String value = values.isNull(i) ? null : new String(values.get(i), StandardCharsets.UTF_8);
            result.add(new AbstractMap.SimpleEntry<>(key, value));
        }
        return result;
    }

    // ---- LIST<MAP>: one map per array element (OTel Events.Attributes) ----

    /**
     * The list element must carry the {@code entries} struct. Describing a MAP element by its ArrowType
     * alone yields a childless MAP that allocates as an empty vector, so writes land nowhere and the
     * column reads back empty — the failure this asserts against.
     */
    public void testMultiValueArrowFieldIsListOfMap() {
        FlatObjectParquetField field = new FlatObjectParquetField();
        Field arrowField = field.toArrowField("Events.Attributes", true);

        assertEquals(ArrowType.List.INSTANCE, arrowField.getType());
        assertEquals(1, arrowField.getChildren().size());

        Field element = arrowField.getChildren().get(0);
        assertTrue("element must be a MAP", element.getType() instanceof ArrowType.Map);
        assertTrue("element must be nullable: [{...}, null] is a legal document", element.getFieldType().isNullable());

        assertEquals("element must carry its entries struct", 1, element.getChildren().size());
        Field entries = element.getChildren().get(0);
        assertEquals(MapVector.DATA_VECTOR_NAME, entries.getName());
        assertFalse(entries.getFieldType().isNullable());
        assertEquals(2, entries.getChildren().size());
        assertEquals(MapVector.KEY_NAME, entries.getChildren().get(0).getName());
        assertEquals(MapVector.VALUE_NAME, entries.getChildren().get(1).getName());
    }

    public void testSupportsMultiValue() {
        assertTrue(new FlatObjectParquetField().supportsMultiValue());
    }

    /**
     * Each array element keeps its own attribute set. Merging them into one map would lose which event
     * an attribute belonged to, which is the whole reason this column is a LIST.
     */
    public void testMultiValueWritesOneMapPerElement() {
        FlatObjectParquetField field = new FlatObjectParquetField();
        ManagedVSR vsr = createListVSR(field);
        MappedFieldType ft = flatObjectType("Events.Attributes");

        // Row 0: two events, each with its own attributes.
        field.createField(
            ft,
            vsr,
            List.of(List.of(Map.entry("exception.type", "IOError")), List.of(Map.entry("http.method", "GET"), Map.entry("retry", "1")))
        );
        vsr.setRowCount(1);
        // Row 1: a single event.
        field.createField(ft, vsr, List.of(List.of(Map.entry("k", "v"))));
        vsr.setRowCount(2);

        ListVector listVector = (ListVector) vsr.getVector("Events.Attributes");
        MapVector mapVector = (MapVector) listVector.getDataVector();

        assertEquals(2, elementCount(listVector, 0));
        int row0 = elementStart(listVector, 0);
        assertEquals(List.of(Map.entry("exception.type", "IOError")), mapEntries(mapVector, row0));
        assertEquals(List.of(Map.entry("http.method", "GET"), Map.entry("retry", "1")), mapEntries(mapVector, row0 + 1));

        assertEquals(1, elementCount(listVector, 1));
        assertEquals(List.of(Map.entry("k", "v")), mapEntries(mapVector, elementStart(listVector, 1)));

        cleanupVSR(vsr);
    }

    /**
     * An absent field is a null list and an explicit empty array is a zero-length non-null list, so
     * {@code "Attributes": []} stays distinguishable from no attributes at all.
     */
    public void testMultiValueNullAndEmptyList() {
        FlatObjectParquetField field = new FlatObjectParquetField();
        ManagedVSR vsr = createListVSR(field);
        MappedFieldType ft = flatObjectType("Events.Attributes");

        field.createField(ft, vsr, null);
        vsr.setRowCount(1);
        field.createField(ft, vsr, List.of());
        vsr.setRowCount(2);

        ListVector listVector = (ListVector) vsr.getVector("Events.Attributes");
        assertTrue("absent field must be a null list", listVector.isNull(0));
        assertFalse("an explicit empty array must be non-null", listVector.isNull(1));
        assertEquals(0, elementCount(listVector, 1));

        cleanupVSR(vsr);
    }

    /** An empty attribute set on one event must not collapse that element away. */
    public void testMultiValuePreservesEmptyElement() {
        FlatObjectParquetField field = new FlatObjectParquetField();
        ManagedVSR vsr = createListVSR(field);
        MappedFieldType ft = flatObjectType("Events.Attributes");

        field.createField(ft, vsr, List.of(List.of(), List.of(Map.entry("a", "1"))));
        vsr.setRowCount(1);

        ListVector listVector = (ListVector) vsr.getVector("Events.Attributes");
        MapVector mapVector = (MapVector) listVector.getDataVector();
        assertEquals("both elements must survive", 2, elementCount(listVector, 0));
        int start = elementStart(listVector, 0);
        assertEquals(0, mapEntries(mapVector, start).size());
        assertEquals(List.of(Map.entry("a", "1")), mapEntries(mapVector, start + 1));

        cleanupVSR(vsr);
    }

    private static int elementStart(ListVector listVector, int row) {
        return listVector.getOffsetBuffer().getInt((long) row * ListVector.OFFSET_WIDTH);
    }

    private static int elementCount(ListVector listVector, int row) {
        int start = elementStart(listVector, row);
        int end = listVector.getOffsetBuffer().getInt((long) (row + 1) * ListVector.OFFSET_WIDTH);
        return end - start;
    }

    private ManagedVSR createListVSR(FlatObjectParquetField field) {
        Schema schema = new Schema(List.of(field.toArrowField("Events.Attributes", true)));
        BufferAllocator child = allocator.newChildAllocator("flat-object-list-test", 0, Long.MAX_VALUE);
        return new ManagedVSR("flat-object-list-test", schema, child);
    }

    private ManagedVSR createVSR(FlatObjectParquetField field) {
        Schema schema = new Schema(List.of(field.toArrowField("attrs", false)));
        BufferAllocator child = allocator.newChildAllocator("flat-object-test", 0, Long.MAX_VALUE);
        return new ManagedVSR("flat-object-test", schema, child);
    }

    private void cleanupVSR(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }
}
