/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.plugins.fields;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.fields.core.metadata.DocCountParquetField;
import com.parquet.parquetdataformat.fields.core.metadata.SizeParquetField;
import org.opensearch.index.mapper.DocCountFieldMapper;

import java.util.HashMap;
import java.util.Map;

public class MetadataFieldPlugin implements ParquetFieldPlugin {

    @Override
    public Map<String, ParquetField> getParquetFields() {
        final Map<String, ParquetField> fieldMap = new HashMap<>();

        // Register metadata field types
        registerMetadataFields(fieldMap);

        return fieldMap;
    }

    /**
     * Registers all metadata field type mappings.
     *
     * @param fieldMap the map to populate with metadata field mappings
     */
    private static void registerMetadataFields(final Map<String, ParquetField> fieldMap) {
        fieldMap.put(DocCountFieldMapper.CONTENT_TYPE, new DocCountParquetField());
        fieldMap.put("_size", new SizeParquetField());
    }
}
