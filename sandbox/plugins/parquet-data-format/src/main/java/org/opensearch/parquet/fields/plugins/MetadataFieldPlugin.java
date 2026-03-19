/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.plugins;

import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;
import org.opensearch.parquet.fields.core.metadata.IdParquetField;
import org.opensearch.parquet.fields.core.metadata.IgnoredParquetField;
import org.opensearch.parquet.fields.core.metadata.RoutingParquetField;
import org.opensearch.parquet.fields.core.metadata.SizeParquetField;

import java.util.HashMap;
import java.util.Map;

/**
 * Metadata fields plugin providing Parquet field implementations for OpenSearch metadata fields.
 */
public class MetadataFieldPlugin implements ParquetFieldPlugin {

    @Override
    public Map<String, ParquetField> getParquetFields() {
        final Map<String, ParquetField> fieldMap = new HashMap<>();
        fieldMap.put(DocCountFieldMapper.CONTENT_TYPE, new LongParquetField());
        fieldMap.put("_size", new SizeParquetField());
        fieldMap.put(RoutingFieldMapper.CONTENT_TYPE, new RoutingParquetField());
        fieldMap.put(IgnoredFieldMapper.CONTENT_TYPE, new IgnoredParquetField());
        fieldMap.put(IdFieldMapper.CONTENT_TYPE, new IdParquetField());
        fieldMap.put(SeqNoFieldMapper.CONTENT_TYPE, new LongParquetField());
        fieldMap.put(VersionFieldMapper.CONTENT_TYPE, new LongParquetField());
        return fieldMap;
    }
}
