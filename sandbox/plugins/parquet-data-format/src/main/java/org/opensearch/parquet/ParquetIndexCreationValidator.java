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

/**
 * Validates Parquet field-level encoding settings against index mappings at index creation time.
 */
public class ParquetIndexCreationValidator implements IndexCreationValidator {

    @Override
    public void validate(MapperService mapperService, IndexSettings indexSettings) {
        if (indexSettings.getSettings().getAsBoolean("index.pluggable.dataformat.enabled", false) == false) {
            return;
        }
        if ("parquet".equals(indexSettings.getSettings().get("index.composite.primary_data_format")) == false) {
            return;
        }
        if (mapperService.documentMapper() == null) {
            return;
        }

        // Get all field configurations
        Map<String, String> fieldEncodings = ParquetSettings.getFieldEncodings(indexSettings.getSettings());
        Map<String, String> fieldCompressions = ParquetSettings.getFieldCompressions(indexSettings.getSettings());
        Map<String, Boolean> fieldBloomFilterEnabled = ParquetSettings.getFieldBloomFilterEnabled(indexSettings.getSettings());

        if (fieldEncodings.isEmpty() && fieldCompressions.isEmpty() && fieldBloomFilterEnabled.isEmpty()) {
            return;
        }

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        ParquetSettings.validateFieldConfigurations(fieldEncodings, fieldCompressions, fieldBloomFilterEnabled, schema);
    }
}
