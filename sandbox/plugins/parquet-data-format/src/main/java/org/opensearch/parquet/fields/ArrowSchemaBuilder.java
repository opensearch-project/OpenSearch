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
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings.
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

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
        // Engine-4: a nested leaf becomes a LIST<primitive> column on the parent row (tagged with its
        // nested path in field metadata); a non-nested leaf stays a flat column. The element→row mapping
        // is the element index's __parent_row__ doc-value, so no per-row bridge columns are emitted.
        final Set<String> nestedPaths = nestedPaths(documentMapper);
        if (documentMapper != null) {
            for (Mapper mapper : documentMapper.mappers()) {
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }

                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField == null) {
                    logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }
                String nestedPath = enclosingNestedPath(mapper.name(), nestedPaths);
                if (nestedPath != null) {
                    fields.add(nestedLeafListField(mapper.name(), nestedPath, parquetField));
                    // A nested keyword's raw-value sibling is skipped in v1: the analytics path filters
                    // nested keyword leaves via the element index, not a parent raw-value column.
                } else {
                    fields.add(new Field(mapper.name(), parquetField.getFieldType(), null));
                    handleNormalizedField(mapper, documentMapper, fields, parquetField);
                }
            }
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    /** All {@code nested} object paths declared in the mapping. Empty when none / no mapper. */
    private static Set<String> nestedPaths(DocumentMapper documentMapper) {
        Set<String> paths = new LinkedHashSet<>();
        if (documentMapper == null) {
            return paths;
        }
        for (ObjectMapper objectMapper : documentMapper.mappers().objectMappers().values()) {
            if (objectMapper.nested().isNested()) {
                paths.add(objectMapper.fullPath());
            }
        }
        return paths;
    }

    /**
     * Returns the deepest nested path that encloses {@code leafName} (i.e. {@code leafName} starts with
     * {@code path + "."}), or null if the leaf is not under any nested object. Deepest-wins keeps a leaf
     * assigned to its nearest nested ancestor if paths ever nest (rejected in v1, but harmless here).
     */
    private static String enclosingNestedPath(String leafName, Set<String> nestedPaths) {
        String match = null;
        for (String path : nestedPaths) {
            if (leafName.startsWith(path + ".") && (match == null || path.length() > match.length())) {
                match = path;
            }
        }
        return match;
    }

    /** A {@code LIST<primitive>} field for a nested leaf, tagged with its nested path in field metadata. */
    private static Field nestedLeafListField(String leafName, String nestedPath, ParquetField parquetField) {
        Field element = new Field("element", parquetField.getFieldType(), null);
        FieldType listType = new FieldType(true, ArrowType.List.INSTANCE, null, Map.of(NestedColumns.NESTED_PATH_META_KEY, nestedPath));
        return new Field(leafName, listType, List.of(element));
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
