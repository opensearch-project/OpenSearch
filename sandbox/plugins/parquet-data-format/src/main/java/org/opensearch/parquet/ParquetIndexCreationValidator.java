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
import org.opensearch.index.IndexSortConfig;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;

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
        Set<String> lowCardinalityEnabledFields = ParquetSettings.getLowCardinalityEnabledFields(mapperService);

        boolean hasParquetSettings = !fieldEncodings.isEmpty()
            || !fieldCompressions.isEmpty()
            || !fieldBloomFilterEnabled.isEmpty()
            || !lowCardinalityEnabledFields.isEmpty();

        boolean isParquetIndex = indexSettings.getSettings().getAsBoolean("index.pluggable.dataformat.enabled", false)
            && "parquet".equals(indexSettings.getSettings().get("index.composite.primary_data_format"));

        if (!isParquetIndex && hasParquetSettings) {
            throw new IllegalArgumentException(
                "Parquet field-level settings are configured but the index does not use parquet data format"
            );
        }

        if (!isParquetIndex) {
            return;
        }

        validateSortFieldsAreSingleValued(mapperService, indexSettings);

        // Building the schema validates the mapping's `multi_value` declarations: getSchema throws
        // for a field whose type has no list support, turning what would otherwise be a
        // per-document indexing failure into an immediate error at creation time.
        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        ParquetSettings.validateFieldConfigurations(
            fieldEncodings,
            fieldCompressions,
            fieldBloomFilterEnabled,
            lowCardinalityEnabledFields,
            schema,
            mapperService
        );
    }

    /**
     * Rejects {@code index.sort.field} entries that are mapped {@code multi_value: true}.
     * <p>
     * An index sort needs one total order over rows, but a multi-valued cell has no canonical
     * scalar to order by — any of min/max/lexicographic would be a silent choice the user never
     * made. Lucene rejects index sorting on multi-valued fields for the same reason; failing here
     * keeps parity and turns what would otherwise be a merge-time failure (the native k-way merge
     * cannot compare LIST sort keys) into an immediate, actionable error at creation time.
     */
    private static void validateSortFieldsAreSingleValued(MapperService mapperService, IndexSettings indexSettings) {
        for (String sortField : IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(indexSettings.getSettings())) {
            MappedFieldType fieldType = mapperService.fieldType(sortField);
            if (fieldType != null && fieldType.isMultiValued()) {
                throw new IllegalArgumentException(
                    "Cannot use field ["
                        + sortField
                        + "] in [index.sort.field]: the field is mapped [multi_value: true] and a "
                        + "multi-valued field has no single value to sort on"
                );
            }
        }
    }
}
