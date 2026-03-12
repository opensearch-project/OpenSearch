/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility class for creating Apache Arrow schemas from OpenSearch mapper services.
 * This class provides methods to convert OpenSearch field mappings into Arrow schema definitions
 * that can be used for Parquet data format operations.
 */
public final class ArrowSchemaBuilder {

    // Private constructor to prevent instantiation of utility class
    private ArrowSchemaBuilder() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    /**
     * Creates an Apache Arrow Schema from the provided MapperService.
     * This method extracts all non-metadata field mappers and converts them to Arrow fields.
     *
     * @param mapperService the OpenSearch mapper service containing field definitions
     * @return a new Schema containing Arrow field definitions for all mapped fields
     * @throws IllegalArgumentException if mapperService is null
     * @throws IllegalStateException if no valid fields are found or if a field type is not supported
     */
    public static Schema getSchema(final MapperService mapperService, boolean isPrimary) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");

        final List<Field> fields = extractFieldsFromMappers(mapperService, isPrimary);

        if (fields.isEmpty()) {
            throw new IllegalStateException("No valid fields found in mapper service");
        }

        return new Schema(fields);
    }

    /**
     * Extracts Arrow fields from the mapper service, filtering out metadata fields.
     *
     * @param mapperService the mapper service to extract fields from
     * @param isPrimary whether this is a primary engine context
     * @return a list of Arrow fields
     */
    private static List<Field> extractFieldsFromMappers(final MapperService mapperService, boolean isPrimary) {
        final List<Field> fields = new ArrayList<>();

        for (final Mapper mapper : mapperService.documentMapper().mappers()) {
            if (notSupportedMetadataField(mapper)) {
                continue;
            }

            final Field arrowField = createArrowField(mapper, isPrimary);
            if (arrowField != null) {
                fields.add(arrowField);
            }
        }

        LongParquetField longField = new LongParquetField();
        fields.add(new Field(CompositeDataFormatWriter.ROW_ID, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, longField.getFieldType(), null));

        return fields;
    }

    /**
     * Checks if the given mapper represents a not supported metadata field.
     *
     * @param mapper the mapper to check
     * @return true if the mapper is a not supported metadata field, false otherwise
     */
    private static boolean notSupportedMetadataField(final Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }

    /**
     * Creates an Arrow Field from an OpenSearch Mapper using the ArrowFieldRegistry.
     * For non-primary contexts, returns null if the field type has no eligible ParquetField,
     * allowing the caller to skip the field. For primary contexts, throws IllegalStateException
     * if no ParquetField is found.
     *
     * @param mapper the mapper to convert
     * @param isPrimary whether this is a primary engine context
     * @return a new Arrow Field, or null if the field is not eligible for the role
     * @throws IllegalStateException if the mapper type is not supported in primary context
     */
    private static Field createArrowField(final Mapper mapper, boolean isPrimary) {
        final ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());

        if (parquetField == null) {
            if (isPrimary) {
                throw new IllegalStateException(
                    String.format("Unsupported field type '%s' for field '%s'",
                        mapper.typeName(), mapper.name())
                );
            }
            return null;
        }

        return new Field(mapper.name(), parquetField.getFieldType(), null);
    }
}
