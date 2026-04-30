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
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings.
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

    private ArrowSchemaBuilder() {}

    /**
     * Creates an Arrow Schema from the MapperService.
     * @param mapperService the mapper service containing field mappings
     */
    public static Schema getSchema(MapperService mapperService) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");
        if (mapperService.documentMapper() == null) {
            throw new IllegalStateException("DocumentMapper is not initialized");
        }
        List<Field> fields = new ArrayList<>();
        for (Mapper mapper : mapperService.documentMapper().mappers()) {
            if (isUnsupportedMetadataField(mapper)) {
                logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                continue;
            }
            ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
            if (parquetField != null) {
                fields.add(new Field(mapper.name(), parquetField.getFieldType(), null));
            } else {
                logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
            }
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField();
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        return new Schema(fields);
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
