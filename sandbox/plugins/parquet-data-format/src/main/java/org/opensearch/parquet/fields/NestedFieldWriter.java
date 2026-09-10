/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.parquet.writer.MismatchedInputException;
import org.opensearch.parquet.writer.ParquetDocumentInput;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes a document's nested (LIST&lt;STRUCT&gt;) and map (MAP&lt;Utf8,Utf8&gt;) fields into the active
 * VSR, on behalf of {@link org.opensearch.parquet.vsr.VSRManager}. Kept separate so VSRManager's own
 * job stays "how the VSR is managed" rather than field-ingestion detail.
 *
 * <p>Every leaf value is written through its registered {@link ParquetField#writeValue}, the same
 * conversion the top-level (non-nested) write path uses — one canonical implementation per type,
 * not a second one maintained here independently.
 */
public final class NestedFieldWriter {

    private NestedFieldWriter() {}

    /**
     * Writes the document's buffered nested children into their LIST&lt;STRUCT&gt; vectors at
     * {@code rowIndex}. Children of the same path form one list; each child becomes one struct
     * element in parse order. Rows without a nested field leave the list null.
     *
     * @throws MismatchedInputException if the active VSR is missing a vector {@code doc} needs —
     *         a schema-reconciliation bug, not a case to silently drop data for.
     */
    public static void writeNestedChildren(ParquetDocumentInput doc, ManagedVSR activeVSR, int rowIndex) {
        if (doc.getNestedChildren().isEmpty()) {
            return;
        }
        // Group top-level children by nested path, preserving parse order within each path.
        Map<String, List<ParquetDocumentInput.NestedChild>> byPath = new LinkedHashMap<>();
        for (ParquetDocumentInput.NestedChild child : doc.getNestedChildren()) {
            byPath.computeIfAbsent(child.path, k -> new ArrayList<>()).add(child);
        }
        for (Map.Entry<String, List<ParquetDocumentInput.NestedChild>> entry : byPath.entrySet()) {
            FieldVector vector = activeVSR.getVector(entry.getKey());
            if (vector instanceof ListVector listVector) {
                writeChildList(listVector, rowIndex, entry.getKey(), entry.getValue());
            } else {
                throw new MismatchedInputException(
                    "No LIST vector for nested path ["
                        + entry.getKey()
                        + "] — schema reconciliation must run via "
                        + "updateMappingVersion before this document is written"
                );
            }
        }
    }

    /** Writes one list of child elements at {@code rowIndex} of {@code listVector}, recursing into inner lists. */
    private static void writeChildList(ListVector listVector, int rowIndex, String path, List<ParquetDocumentInput.NestedChild> children) {
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
                    ParquetField parquetField = ArrowFieldRegistry.getParquetField(leaf.fieldType.typeName());
                    if (parquetField == null) {
                        throw new MismatchedInputException(
                            "No ParquetField mapping for field [" + leaf.fieldType.name() + "] of type [" + leaf.fieldType.typeName() + "]"
                        );
                    }
                    parquetField.writeValue(leafVector, elemIndex, leaf.value);
                }
            }
            // map children of this element (e.g. a flat_object `attributes`). Write EVERY map child of
            // the struct — even when this element has no entries for it — so each element's offset is
            // written explicitly and deterministically rather than relying on Arrow's implicit
            // back-fill for skipped indices.
            for (FieldVector childVector : structVector.getChildrenFromFields()) {
                if (childVector instanceof MapVector mapVector) {
                    String mapFullName = path + "." + mapVector.getName();
                    writeMapChild(mapVector, elemIndex, child.mapEntries.getOrDefault(mapFullName, List.of()));
                }
            }
            // deeper nested elements (e.g. replies inside a comment), grouped by their path
            if (child.children.isEmpty() == false) {
                Map<String, List<ParquetDocumentInput.NestedChild>> innerByPath = new LinkedHashMap<>();
                for (ParquetDocumentInput.NestedChild inner : child.children) {
                    innerByPath.computeIfAbsent(inner.path, k -> new ArrayList<>()).add(inner);
                }
                for (Map.Entry<String, List<ParquetDocumentInput.NestedChild>> entry : innerByPath.entrySet()) {
                    String innerLeaf = entry.getKey().substring(path.length() + 1);
                    FieldVector innerVector = structVector.getChild(innerLeaf);
                    if (innerVector instanceof ListVector innerList) {
                        writeChildList(innerList, elemIndex, entry.getKey(), entry.getValue());
                    } else {
                        throw new MismatchedInputException(
                            "Struct ["
                                + path
                                + "] has no inner LIST child ["
                                + innerLeaf
                                + "] — schema reconciliation must run via updateMappingVersion before this document is written"
                        );
                    }
                }
            }
        }
        listVector.endValue(rowIndex, children.size());
    }

    /**
     * Writes document-root MAP columns (a top-level {@code flat_object}) at {@code rowIndex}. Iterates
     * every top-level MAP vector so each row's offset is set explicitly — empty when the document has no
     * entries for that field — instead of leaving skipped rows to Arrow's implicit offset back-fill.
     * <p>
     * Consequence: a document that omits the field yields an EMPTY (non-null) map, which is
     * indistinguishable from an explicit {@code "attributes": {}}. That is a deliberate simplification.
     */
    public static void writeTopLevelMaps(ParquetDocumentInput doc, ManagedVSR activeVSR, int rowIndex) {
        for (Field field : activeVSR.getSchema().getFields()) {
            if (activeVSR.getVector(field.getName()) instanceof MapVector mapVector) {
                writeMapChild(mapVector, rowIndex, doc.getTopLevelMapEntries().getOrDefault(mapVector.getName(), List.of()));
            }
        }
    }

    /**
     * Writes one {@code MAP<Utf8,Utf8>} value at {@code index}: each buffered (key,value) becomes one map
     * entry (a {@code key_value} struct). A null value leaves the entry's value null; an empty list writes
     * an empty (non-null) map. Keys/values are stringified to UTF-8.
     */
    private static void writeMapChild(MapVector mapVector, int index, List<Map.Entry<String, Object>> entries) {
        int start = mapVector.startNewValue(index);
        StructVector entriesStruct = (StructVector) mapVector.getDataVector();
        VarCharVector keyVector = (VarCharVector) entriesStruct.getChild(MapVector.KEY_NAME);
        VarCharVector valueVector = (VarCharVector) entriesStruct.getChild(MapVector.VALUE_NAME);
        for (int i = 0; i < entries.size(); i++) {
            int pos = start + i;
            entriesStruct.setIndexDefined(pos);
            Map.Entry<String, Object> entry = entries.get(i);
            keyVector.setSafe(pos, entry.getKey().getBytes(StandardCharsets.UTF_8));
            Object value = entry.getValue();
            if (value != null) {
                valueVector.setSafe(pos, value.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        mapVector.endValue(index, entries.size());
    }
}
