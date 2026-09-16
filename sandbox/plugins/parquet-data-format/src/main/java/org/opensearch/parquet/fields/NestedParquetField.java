/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.core.data.FlatObjectParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.parquet.writer.MismatchedInputException;
import org.opensearch.parquet.writer.ParquetDocumentInput;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parquet field for {@code nested} — one repeating group (a {@code nested} array) stored as a single
 * {@code LIST<STRUCT>} column, registered under content type {@code "nested"} (see
 * {@link org.opensearch.index.mapper.ObjectMapper#NESTED_CONTENT_TYPE}).
 *
 * <p>Unlike every other registered {@link ParquetField}, {@code nested} has no {@link MappedFieldType}
 * of its own — an {@code ObjectMapper} is never a {@code FieldMapper}, so nothing ever looks this type up
 * by {@code fieldType.typeName()}. Callers (schema building in {@code ArrowSchemaBuilder}, and document
 * writing in {@code VSRManager}) look this handler up directly by the literal content-type string
 * instead, once they've identified a nested object mapper by {@code ObjectMapper#nested()}.
 *
 * <p>{@link #buildField(String)} is unsupported: unlike a scalar leaf, a nested field's Arrow shape is
 * never knowable from just its name — it always needs its struct children, supplied by the caller (which
 * knows the mapper tree) via {@link #buildField(String, List)} instead.
 */
public class NestedParquetField extends ParquetField {

    /** Creates a new NestedParquetField. */
    public NestedParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        // nested values arrive as an element tree (ParquetDocumentInput#getNestedChildren), written via
        // writeNestedChildren below, not through the scalar createField path.
        throw new UnsupportedOperationException("nested [" + fieldType.name() + "] is written via writeNestedChildren, not addToGroup");
    }

    @Override
    public ArrowType getArrowType() {
        return ArrowType.List.INSTANCE;
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE);
    }

    @Override
    public FieldType getFieldType() {
        // Nominal type only — the real LIST<STRUCT<...>> field (with its struct children) is built by
        // buildField(String, List<Field>) below.
        return FieldType.nullable(getArrowType());
    }

    @Override
    public Field buildField(String name) {
        throw new UnsupportedOperationException(
            "nested field [" + name + "] has no fixed shape — use buildField(String, List<Field>) with its struct children"
        );
    }

    /**
     * Builds the {@code LIST<STRUCT<children>>} field for a nested object mapper named {@code name} —
     * one element per array entry. Struct children are matched BY POSITION downstream (Substrait /
     * DataFusion), so they're sorted deterministically by name to match whatever read schema the query
     * engine builds (typically sorted, e.g. via a {@code TreeMap}) — the same reason
     * {@code ArrowSchemaBuilder} sorts every other struct it builds.
     *
     * @param name     the nested field's name — a full dotted path at the document root, or a leaf name
     *                 relative to its parent struct when nested inside another nested field
     * @param children the struct's children: the mapper's own direct leaf/flat_object/nested-in-nested
     *                 fields, already built by the caller
     */
    public Field buildField(String name, List<Field> children) {
        List<Field> sorted = new ArrayList<>(children);
        sorted.sort(Comparator.comparing(Field::getName));
        Field element = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), sorted);
        return new Field(name, FieldType.nullable(getArrowType()), List.of(element));
    }

    /**
     * Groups the document's top-level nested elements by path and delegates each field value to
     * {@link #writeNestedValue}. Rows without nested elements leave their LIST columns null.
     *
     * @throws MismatchedInputException if the active VSR is missing a nested LIST vector
     */
    public void writeNestedChildren(ParquetDocumentInput doc, ManagedVSR activeVSR, int rowIndex) {
        if (doc.getNestedChildren().isEmpty()) {
            return;
        }
        Map<String, List<ParquetDocumentInput.NestedChild>> byPath = new LinkedHashMap<>();
        for (ParquetDocumentInput.NestedChild child : doc.getNestedChildren()) {
            byPath.computeIfAbsent(child.path, k -> new ArrayList<>()).add(child);
        }
        for (Map.Entry<String, List<ParquetDocumentInput.NestedChild>> entry : byPath.entrySet()) {
            writeNestedValue(activeVSR.getVector(entry.getKey()), rowIndex, entry.getValue());
        }
    }

    /**
     * Writes one nested field value as a LIST of STRUCT elements at {@code rowIndex}. Recursive
     * nested fields re-enter this same hook, so every Parquet field owns its vector write through
     * {@link #writeNestedValue}.
     */
    @SuppressWarnings("unchecked")
    void writeNestedValue(FieldVector vector, int rowIndex, Object value) {
        List<ParquetDocumentInput.NestedChild> children = (List<ParquetDocumentInput.NestedChild>) value;
        if (children.isEmpty()) {
            throw new IllegalArgumentException("nested field value must contain at least one child element");
        }
        String path = children.getFirst().path;
        if (vector instanceof ListVector == false) {
            throw new MismatchedInputException(
                "No LIST vector for nested path ["
                    + path
                    + "] — schema reconciliation must run via updateMappingVersion before this document is written"
            );
        }
        ListVector listVector = (ListVector) vector;

        int startOffset = listVector.startNewValue(rowIndex);
        StructVector structVector = (StructVector) listVector.getDataVector();
        for (int i = 0; i < children.size(); i++) {
            int elemIndex = startOffset + i;
            ParquetDocumentInput.NestedChild child = children.get(i);
            structVector.setIndexDefined(elemIndex);
            // leaf fields of this element — name is already relative to this struct (computed once by
            // ParquetDocumentInput when the field was buffered, not re-derived here).
            for (ParquetDocumentInput.NestedLeaf leaf : child.fields) {
                FieldVector leafVector = structVector.getChild(leaf.name);
                if (leafVector == null) {
                    throw new MismatchedInputException(
                        "Struct ["
                            + path
                            + "] has no child vector ["
                            + leaf.name
                            + "] — schema reconciliation must run via updateMappingVersion before this document is written"
                    );
                }
                if (leaf.value != null) {
                    writeLeafValue(leafVector, elemIndex, leaf.value);
                }
            }
            // map children of this element (e.g. a flat_object `attributes`). Write every map child
            // so each element's offset is explicit and deterministic.
            for (FieldVector childVector : structVector.getChildrenFromFields()) {
                if (childVector instanceof MapVector mapVector) {
                    String mapFullName = path + "." + mapVector.getName();
                    FlatObjectParquetField flatObjectField = (FlatObjectParquetField) ArrowFieldRegistry.getParquetField(
                        FlatObjectFieldMapper.CONTENT_TYPE
                    );
                    flatObjectField.writeMapChild(mapVector, elemIndex, child.mapEntries.getOrDefault(mapFullName, List.of()));
                }
            }
            if (child.children.isEmpty() == false) {
                Map<String, List<ParquetDocumentInput.NestedChild>> innerByPath = new LinkedHashMap<>();
                for (ParquetDocumentInput.NestedChild inner : child.children) {
                    innerByPath.computeIfAbsent(inner.path, k -> new ArrayList<>()).add(inner);
                }
                for (Map.Entry<String, List<ParquetDocumentInput.NestedChild>> entry : innerByPath.entrySet()) {
                    String innerLeaf = entry.getKey().substring(path.length() + 1);
                    writeNestedValue(structVector.getChild(innerLeaf), elemIndex, entry.getValue());
                }
            }
        }
        listVector.endValue(rowIndex, children.size());
    }

    /** Writes one supported scalar value into a nested struct child. */
    static void writeLeafValue(FieldVector vector, int index, Object value) {
        switch (vector) {
            case VarCharVector typed -> typed.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
            case IntVector typed -> typed.setSafe(index, (Integer) value);
            case BigIntVector typed -> typed.setSafe(index, (Long) value);
            case Float8Vector typed -> typed.setSafe(index, (Double) value);
            case Float4Vector typed -> typed.setSafe(index, (Float) value);
            case BitVector typed -> typed.setSafe(index, (Boolean) value ? 1 : 0);
            case SmallIntVector typed -> typed.setSafe(index, (Short) value);
            case TinyIntVector typed -> typed.setSafe(index, ((Number) value).byteValue());
            case TimeStampMilliVector typed -> typed.setSafe(index, (long) value);
            case TimeStampNanoVector typed -> typed.setSafe(index, (long) value);
            case Float2Vector typed -> typed.setSafeWithPossibleTruncate(index, ((Number) value).floatValue());
            case UInt8Vector typed -> typed.setSafe(index, ((Number) value).longValue());
            case VarBinaryVector typed -> {
                if (value instanceof InetAddress address) {
                    BytesRef encoded = new BytesRef(InetAddressPoint.encode(address));
                    typed.setSafe(index, encoded.bytes, encoded.offset, encoded.length);
                } else {
                    typed.setSafe(index, (byte[]) value);
                }
            }
            default -> throw new IllegalArgumentException(
                "Unsupported struct-leaf vector type [" + vector.getClass().getSimpleName() + "] for nested field"
            );
        }
    }
}
