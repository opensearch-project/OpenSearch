/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.Field;
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
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings. Nested-field schema
 * building (the {@code LIST<STRUCT>} tree) is delegated to {@link NestedSchemaBuilder} so this class
 * stays focused on flat, top-level columns.
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
        if (documentMapper != null) {
            // Nested: fields under a nested object mapper are packed into a LIST<STRUCT> column on
            // the parent row instead of flat leaf columns — built by NestedSchemaBuilder below.
            Set<String> nestedPaths = NestedSchemaBuilder.nestedPaths(documentMapper);

            for (Mapper mapper : documentMapper.mappers()) {
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }
                if (NestedSchemaBuilder.owningNestedPath(mapper.name(), nestedPaths) != null) {
                    // handled below as part of the nested LIST<STRUCT> tree
                    continue;
                }

                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField != null) {
                    fields.add(parquetField.buildField(mapper.name()));
                    handleNormalizedField(mapper, documentMapper, fields, parquetField);
                } else {
                    logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                }
            }

            fields.addAll(NestedSchemaBuilder.buildTopLevelNestedFields(documentMapper, nestedPaths));
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    private static void handleNormalizedField(Mapper mapper, DocumentMapper documentMapper, List<Field> fields, ParquetField parquetField) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (!documentMapper.mappers().isMultiField(mapper.name()) && keywordFieldMapper.getRawValueFieldType() != null) {
                KeywordFieldMapper.KeywordFieldType rawValueField = keywordFieldMapper.getRawValueFieldType();
                fields.add(new Field(rawValueField.name(), parquetField.getFieldType(), null));
            }
        }
    }

    /** Package-visible so {@link NestedSchemaBuilder} applies the same metadata-field exclusions. */
    static boolean isUnsupportedMetadataField(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }
}
