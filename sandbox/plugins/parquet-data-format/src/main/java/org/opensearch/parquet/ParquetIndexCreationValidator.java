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
        if (!indexSettings.getSettings().getAsBoolean("index.pluggable.dataformat.enabled", false)) {
            return;
        }
        if (!"parquet".equals(indexSettings.getSettings().get("index.composite.primary_data_format"))) {
            return;
        }
        if (mapperService.documentMapper() == null) {
            return;
        }
        Map<String, String> fieldEncodings = ParquetSettings.getFieldEncodings(indexSettings.getSettings());
        if (fieldEncodings.isEmpty()) {
            return;
        }
        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        ParquetSettings.validateEncodingTypeCompatibility(fieldEncodings, schema);
    }
}
