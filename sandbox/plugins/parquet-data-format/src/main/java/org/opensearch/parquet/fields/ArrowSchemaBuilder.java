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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings.
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

    /** OpenSearch mapper type whose open key space is stored as one Arrow {@code MAP<Utf8,Utf8>} column. */
    private static final String FLAT_OBJECT_TYPE = "flat_object";

    private ArrowSchemaBuilder() {}

    /**
     * Creates an Arrow Schema from the MapperService.
     * @param mapperService the mapper service containing field mappings
     * TODO - Get the mapping version while creating the schema
     */
    public static Schema getSchema(MapperService mapperService) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");
        List<Field> fields = new ArrayList<>();
        DocumentMapper documentMapper = mapperService.documentMapperWithAutoCreate().getDocumentMapper();
        if (documentMapper != null) {
            // Nested: fields under a nested object mapper are packed into a LIST<STRUCT> column on
            // the parent row instead of flat leaf columns.
            Set<String> nestedPaths = documentMapper.objectMappers()
                .entrySet()
                .stream()
                .filter(e -> e.getValue().nested().isNested())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            for (Mapper mapper : documentMapper.mappers()) {
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }
                if (owningNestedPath(mapper.name(), nestedPaths) != null) {
                    // handled below as part of the nested LIST<STRUCT> tree
                    continue;
                }

                if (FLAT_OBJECT_TYPE.equals(mapper.typeName())) {
                    // Open key space -> one MAP<Utf8,Utf8> column (top-level flat_object).
                    fields.add(buildMapField(mapper.name()));
                    continue;
                }

                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField != null) {
                    fields.add(new Field(mapper.name(), parquetField.getFieldType(), null));
                    handleNormalizedField(mapper, documentMapper, fields, parquetField);
                } else {
                    logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                }
            }

            // Emit one LIST<STRUCT> field per TOP-LEVEL nested mapper (nested mappers inside another
            // nested mapper become list fields inside the parent's struct).
            for (String path : nestedPaths) {
                if (owningNestedPath(path, nestedPaths) == null) {
                    Field nestedField = buildNestedListField(path, documentMapper, nestedPaths);
                    if (nestedField != null) {
                        fields.add(nestedField);
                    }
                }
            }
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    /**
     * Returns the deepest nested path that strictly contains {@code name} (i.e. {@code name} starts
     * with {@code path + "."}), or null if none.
     */
    private static String owningNestedPath(String name, Set<String> nestedPaths) {
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
     * Builds the Arrow LIST&lt;STRUCT&gt; field for the nested mapper at {@code path}. Struct
     * children are the mapper's direct leaf fields (named by leaf segment) plus, recursively, one
     * LIST&lt;STRUCT&gt; per directly-contained nested mapper.
     */
    private static Field buildNestedListField(String path, DocumentMapper documentMapper, Set<String> nestedPaths) {
        List<Field> structChildren = new ArrayList<>();
        for (Mapper mapper : documentMapper.mappers()) {
            if (isUnsupportedMetadataField(mapper)) {
                continue;
            }
            if (path.equals(owningNestedPath(mapper.name(), nestedPaths))) {
                String leafName = mapper.name().substring(path.length() + 1);
                if (FLAT_OBJECT_TYPE.equals(mapper.typeName())) {
                    // A flat_object inside a nested field (e.g. events.attributes) becomes a
                    // MAP<Utf8,Utf8> child of the element struct — the open attribute key space.
                    structChildren.add(buildMapField(leafName));
                    continue;
                }
                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField != null) {
                    structChildren.add(new Field(leafName, parquetField.getFieldType(), null));
                }
            }
        }
        for (String subPath : nestedPaths) {
            if (path.equals(owningNestedPath(subPath, nestedPaths))) {
                Field subField = buildNestedListField(subPath, documentMapper, nestedPaths);
                if (subField != null) {
                    String leafName = subPath.substring(path.length() + 1);
                    structChildren.add(new Field(leafName, subField.getFieldType(), subField.getChildren()));
                }
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

    /**
     * Builds an Arrow {@code MAP<Utf8,Utf8>} field for a flat_object's open key space. Matches the
     * canonical parquet MAP layout — {@code Map("key_value": non-null Struct("key": non-null Utf8,
     * "value": nullable Utf8), unsorted)} — rather than Arrow Java's default {@code entries} group name,
     * so the arrow-rs writer and the DataFusion read path see the group name the parquet spec prescribes.
     * <p>
     * Shared by the top-level column path and {@link #buildNestedListField} so a flat_object gets the
     * same shape whether it sits at the document root or inside a nested element's struct.
     */
    private static Field buildMapField(String name) {
        Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
        Field value = new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field entries = new Field("key_value", new FieldType(false, ArrowType.Struct.INSTANCE, null), List.of(key, value));
        return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
    }

    private static void handleNormalizedField(Mapper mapper, DocumentMapper documentMapper, List<Field> fields, ParquetField parquetField) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (!documentMapper.mappers().isMultiField(mapper.name()) && keywordFieldMapper.getRawValueFieldType() != null) {
                KeywordFieldMapper.KeywordFieldType rawValueField = keywordFieldMapper.getRawValueFieldType();
                fields.add(new Field(rawValueField.name(), parquetField.getFieldType(), null));
            }
        }
    }

    private static boolean isUnsupportedMetadataField(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }
}
