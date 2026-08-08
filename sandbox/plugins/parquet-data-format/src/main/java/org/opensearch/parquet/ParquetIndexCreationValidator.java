/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;
import org.opensearch.parquet.fields.ParquetField;

import java.util.Map;
import java.util.Set;

/**
 * Validates Parquet field-level encoding settings against index mappings at index creation time.
 */
public class ParquetIndexCreationValidator implements IndexCreationValidator {

    @Override
    public void validate(MapperService mapperService, IndexSettings indexSettings) {
        Map<String, String> fieldEncodings = ParquetSettings.getFieldEncodings(indexSettings.getSettings());
        Map<String, String> fieldCompressions = ParquetSettings.getFieldCompressions(indexSettings.getSettings());
        Map<String, Boolean> fieldBloomFilterEnabled = ParquetSettings.getFieldBloomFilterEnabled(indexSettings.getSettings());
        Set<String> multiValueFields = ParquetSettings.getMultiValueFields(indexSettings.getSettings());

        boolean hasParquetSettings = !fieldEncodings.isEmpty()
            || !fieldCompressions.isEmpty()
            || !fieldBloomFilterEnabled.isEmpty()
            || !multiValueFields.isEmpty();

        boolean isParquetIndex = indexSettings.getSettings().getAsBoolean("index.pluggable.dataformat.enabled", false)
            && "parquet".equals(indexSettings.getSettings().get("index.composite.primary_data_format"));

        if (!isParquetIndex && hasParquetSettings) {
            throw new IllegalArgumentException(
                "Parquet field-level settings are configured but the index does not use parquet data format"
            );
        }

        if (!isParquetIndex || !hasParquetSettings) {
            return;
        }

        // Validate multi-value declarations before building the schema: getSchema would otherwise
        // throw a less specific error for a type that has no list support.
        validateMultiValueFields(mapperService, multiValueFields);

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService, multiValueFields);
        ParquetSettings.validateFieldConfigurations(fieldEncodings, fieldCompressions, fieldBloomFilterEnabled, schema);
    }

    /**
     * Fails index creation when a field declared multi-valued is absent from the mapping or has a
     * type that cannot be stored as a Parquet LIST column. Catching this at creation time turns
     * what would otherwise be a per-document indexing failure into an immediate, actionable error.
     */
    private static void validateMultiValueFields(MapperService mapperService, Set<String> multiValueFields) {
        for (String fieldName : multiValueFields) {
            MappedFieldType fieldType = mapperService.fieldType(fieldName);
            if (fieldType == null) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] declared in ["
                        + ParquetSettings.MULTI_VALUE_FIELD_SETTING.getKey()
                        + "] does not exist in the index mapping"
                );
            }
            ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldType.typeName());
            if (parquetField == null) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] of type ["
                        + fieldType.typeName()
                        + "] is not stored by the parquet data format and cannot be declared in ["
                        + ParquetSettings.MULTI_VALUE_FIELD_SETTING.getKey()
                        + "]"
                );
            }
            if (parquetField.supportsMultiValue() == false) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] of type ["
                        + fieldType.typeName()
                        + "] does not support multi-valued storage in the parquet data format"
                );
            }
        }
    }
}
