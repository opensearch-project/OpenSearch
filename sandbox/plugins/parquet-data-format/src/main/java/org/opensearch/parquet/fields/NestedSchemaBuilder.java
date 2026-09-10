/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.ObjectMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds the Arrow {@code LIST<STRUCT>} schema for {@code nested} fields, on behalf of
 * {@link ArrowSchemaBuilder}. Kept separate so {@code ArrowSchemaBuilder}'s own loop stays focused on
 * flat, top-level columns.
 *
 * <p>A {@code flat_object} field's {@code MAP<Utf8,Utf8>} schema — whether at the document root or
 * nested inside an element's struct — is built by the registered {@link ParquetField#buildField}
 * (see {@link org.opensearch.parquet.fields.core.data.FlatObjectParquetField}), consulted the same way
 * as every other registered type; this class has no flat_object-specific logic of its own.
 */
final class NestedSchemaBuilder {

    private NestedSchemaBuilder() {}

    /** Returns the full dotted path of every {@code nested}-typed object mapper in {@code documentMapper}. */
    static Set<String> nestedPaths(DocumentMapper documentMapper) {
        return documentMapper.objectMappers()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().nested().isNested())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Returns the deepest nested path that strictly contains {@code name} (i.e. {@code name} starts
     * with {@code path + "."}), or null if none.
     */
    static String owningNestedPath(String name, Set<String> nestedPaths) {
        String best = null;
        for (String path : nestedPaths) {
            if (name.length() > path.length() && name.startsWith(path) && name.charAt(path.length()) == '.') {
                if (best == null || path.length() > best.length()) {
                    best = path;
                }
            }
        }
        return best;
    }

    /**
     * Builds one Arrow {@code LIST<STRUCT>} field per TOP-LEVEL nested mapper in {@code nestedPaths}
     * (a nested mapper whose own path is not itself owned by another nested path), recursing for
     * nested-in-nested.
     */
    static List<Field> buildTopLevelNestedFields(DocumentMapper documentMapper, Set<String> nestedPaths) {
        List<Field> fields = new ArrayList<>();
        Map<String, ObjectMapper> objectMappersByPath = documentMapper.objectMappers();
        for (String path : nestedPaths) {
            if (owningNestedPath(path, nestedPaths) == null) {
                Field nestedField = buildNestedListField(path, documentMapper, objectMappersByPath);
                if (nestedField != null) {
                    fields.add(nestedField);
                }
            }
        }
        return fields;
    }

    /**
     * Returns true if {@code candidate} is a DIRECT child of {@code parentPath} — i.e. it starts with
     * {@code parentPath + "."} and has no further "." after that prefix. OpenSearch's mapper tree
     * always creates one dotted path segment per object-mapper level, so this dot-count check is
     * exactly the mapper tree's own parent/child relationship — no separate tree walk needed.
     */
    private static boolean isDirectChild(String candidate, String parentPath) {
        if (candidate.length() <= parentPath.length() || candidate.startsWith(parentPath + ".") == false) {
            return false;
        }
        return candidate.substring(parentPath.length() + 1).indexOf('.') < 0;
    }

    /**
     * Returns the struct-child leaf name {@code candidate} should use directly under {@code parentPath},
     * or {@code null} if it belongs elsewhere. A field with no further dot after stripping the prefix is
     * always a direct leaf. A field WITH a further dot is a multi-field sibling (e.g. {@code author.raw}
     * next to {@code author}) — not real nesting, since a plain object can never be a descendant of a
     * nested field — UNLESS that dot's first segment is itself a registered nested-in-nested object
     * mapper, in which case the field belongs to that mapper's own subtree and is handled by the
     * object-mapper loop instead. Multi-fields keep their full dotted name as a flat leaf, exactly like
     * the top-level (non-nested) schema path already does.
     */
    private static String directLeafName(String candidate, String parentPath, Map<String, ObjectMapper> objectMappersByPath) {
        if (candidate.length() <= parentPath.length() || candidate.startsWith(parentPath + ".") == false) {
            return null;
        }
        String remainder = candidate.substring(parentPath.length() + 1);
        int dot = remainder.indexOf('.');
        if (dot < 0) {
            return remainder;
        }
        String firstSegment = remainder.substring(0, dot);
        if (objectMappersByPath.containsKey(parentPath + "." + firstSegment)) {
            return null;
        }
        return remainder;
    }

    /**
     * Builds the Arrow LIST&lt;STRUCT&gt; field for the nested mapper at {@code path} (one element per
     * array entry). Struct children are the mapper's direct leaf fields — including multi-fields, kept
     * as their own flat sibling leaf (see {@link #directLeafName}) — plus, recursively, one
     * LIST&lt;STRUCT&gt; per directly-contained nested mapper. A plain (non-nested) object can never be
     * a descendant of a nested field — see the call site's note — so every descendant object mapper
     * found here is itself nested.
     *
     * <p>Nested inside nested: a {@code comments} field with a {@code replies} field nested inside it
     * builds {@code comments: LIST<STRUCT<author: Utf8, replies: LIST<STRUCT<text: Utf8>>>>} — one
     * recursive call per nesting level, mirroring {@code comments[].replies[].text}.
     */
    static Field buildNestedListField(String path, DocumentMapper documentMapper, Map<String, ObjectMapper> objectMappersByPath) {
        List<Field> structChildren = new ArrayList<>();
        for (Mapper mapper : documentMapper.mappers()) {
            if (ArrowSchemaBuilder.isUnsupportedMetadataField(mapper)) {
                continue;
            }
            String leafName = directLeafName(mapper.name(), path, objectMappersByPath);
            if (leafName == null) {
                continue;
            }
            ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
            if (parquetField != null) {
                structChildren.add(parquetField.buildField(leafName));
            }
        }
        for (Map.Entry<String, ObjectMapper> entry : objectMappersByPath.entrySet()) {
            String subPath = entry.getKey();
            if (isDirectChild(subPath, path) == false) {
                continue;
            }
            Field subField = buildNestedListField(subPath, documentMapper, objectMappersByPath);
            if (subField != null) {
                String leafName = subPath.substring(path.length() + 1);
                structChildren.add(new Field(leafName, subField.getFieldType(), subField.getChildren()));
            }
        }
        if (structChildren.isEmpty()) {
            return null;
        }
        // Struct fields are matched BY POSITION downstream (Substrait / DataFusion). Order struct
        // children deterministically by field name so the write schema matches whatever read schema
        // the query engine builds (typically sorted, e.g. via a TreeMap).
        structChildren.sort(Comparator.comparing(Field::getName));
        Field element = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), structChildren);
        return new Field(path, FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    }
}
