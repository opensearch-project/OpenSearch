/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.plugins.fields;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import com.parquet.parquetdataformat.fields.core.metadata.IdParquetField;
import com.parquet.parquetdataformat.fields.core.metadata.IgnoredParquetField;
import com.parquet.parquetdataformat.fields.core.metadata.RoutingParquetField;
import com.parquet.parquetdataformat.fields.core.metadata.SizeParquetField;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

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
        fieldMap.put(DocCountFieldMapper.CONTENT_TYPE, new LongParquetField());
        fieldMap.put("_size", new SizeParquetField());
        fieldMap.put(RoutingFieldMapper.CONTENT_TYPE, new RoutingParquetField());
        fieldMap.put(IgnoredFieldMapper.CONTENT_TYPE, new IgnoredParquetField());
        fieldMap.put(IdFieldMapper.CONTENT_TYPE, new IdParquetField());
        fieldMap.put(SeqNoFieldMapper.CONTENT_TYPE, new LongParquetField());
        fieldMap.put(VersionFieldMapper.CONTENT_TYPE, new LongParquetField());
    }
}
