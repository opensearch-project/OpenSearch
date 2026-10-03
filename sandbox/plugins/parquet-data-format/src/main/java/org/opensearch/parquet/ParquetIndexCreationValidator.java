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
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;

import java.util.Map;
import java.util.Set;

/**
 * Validates per-field Parquet column configuration against index mappings at index creation time. Covers both the
 * mapping-level {@code codec} / {@code bloom_filter} / {@code cardinality} / {@code low_cardinality} parameters and the deprecated
 * per-field index settings; either style is only permitted when Parquet is the index's primary data format.
 */
public class ParquetIndexCreationValidator implements IndexCreationValidator {

    @Override
    @SuppressWarnings("deprecation")
    public void validate(MapperService mapperService, IndexSettings indexSettings) {
        Map<String, String> fieldEncodings = ParquetSettings.getFieldEncodings(indexSettings.getSettings());
        Map<String, String> fieldCompressions = ParquetSettings.getFieldCompressions(indexSettings.getSettings());
        Map<String, Boolean> fieldBloomFilterEnabled = ParquetSettings.getFieldBloomFilterEnabled(indexSettings.getSettings());
        Set<String> lowCardinalityEnabledFields = ParquetSettings.getLowCardinalityEnabledFields(mapperService);
        boolean hasMappingStorageParameters = ParquetFieldCodecs.mappingCodecs(mapperService).isEmpty() == false
            || ParquetFieldCodecs.mappingBloomFilterFields(mapperService).isEmpty() == false
            || ParquetFieldCodecs.mappingCardinalities(mapperService).isEmpty() == false;

        boolean hasParquetSettings = !fieldEncodings.isEmpty()
            || !fieldCompressions.isEmpty()
            || !fieldBloomFilterEnabled.isEmpty()
            || !lowCardinalityEnabledFields.isEmpty()
            || hasMappingStorageParameters;

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

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        // Deprecated per-field settings are validated as declared; mapping parameters are validated in their
        // translated, physical form so the same type-compatibility rules apply to both declaration styles.
        ParquetFieldCodecs.FieldStorageConfig effective = ParquetFieldCodecs.resolve(indexSettings.getSettings(), mapperService);
        ParquetSettings.validateFieldConfigurations(
            effective.encodings(),
            effective.compressions(),
            effective.bloomFilterEnabled(),
            lowCardinalityEnabledFields,
            schema,
            mapperService
        );
    }
}
