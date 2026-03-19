/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.plugins;

import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.fields.core.data.BinaryParquetField;
import org.opensearch.parquet.fields.core.data.BooleanParquetField;
import org.opensearch.parquet.fields.core.data.date.DateNanosParquetField;
import org.opensearch.parquet.fields.core.data.date.DateParquetField;
import org.opensearch.parquet.fields.core.data.number.ByteParquetField;
import org.opensearch.parquet.fields.core.data.number.DoubleParquetField;
import org.opensearch.parquet.fields.core.data.number.FloatParquetField;
import org.opensearch.parquet.fields.core.data.number.HalfFloatParquetField;
import org.opensearch.parquet.fields.core.data.number.IntegerParquetField;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;
import org.opensearch.parquet.fields.core.data.number.ShortParquetField;
import org.opensearch.parquet.fields.core.data.number.TokenCountParquetField;
import org.opensearch.parquet.fields.core.data.number.UnsignedLongParquetField;
import org.opensearch.parquet.fields.core.data.text.IpParquetField;
import org.opensearch.parquet.fields.core.data.text.KeywordParquetField;
import org.opensearch.parquet.fields.core.data.text.TextParquetField;

import java.util.HashMap;
import java.util.Map;

/**
 * Core data fields plugin providing Parquet field implementations for all built-in OpenSearch field types.
 */
public class CoreDataFieldPlugin implements ParquetFieldPlugin {

    @Override
    public Map<String, ParquetField> getParquetFields() {
        final Map<String, ParquetField> fieldMap = new HashMap<>();
        registerNumericFields(fieldMap);
        registerTemporalFields(fieldMap);
        registerBooleanFields(fieldMap);
        registerTextFields(fieldMap);
        registerBinaryFields(fieldMap);
        return fieldMap;
    }

    private static void registerNumericFields(Map<String, ParquetField> fieldMap) {
        fieldMap.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(), new HalfFloatParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.FLOAT.typeName(), new FloatParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.BYTE.typeName(), new ByteParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.SHORT.typeName(), new ShortParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.INTEGER.typeName(), new IntegerParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(), new UnsignedLongParquetField());
        fieldMap.put("token_count", new TokenCountParquetField());
        fieldMap.put("scaled_float", new LongParquetField());
    }

    private static void registerTemporalFields(Map<String, ParquetField> fieldMap) {
        fieldMap.put(DateFieldMapper.CONTENT_TYPE, new DateParquetField());
        fieldMap.put(DateFieldMapper.DATE_NANOS_CONTENT_TYPE, new DateNanosParquetField());
    }

    private static void registerBooleanFields(Map<String, ParquetField> fieldMap) {
        fieldMap.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanParquetField());
    }

    private static void registerTextFields(Map<String, ParquetField> fieldMap) {
        fieldMap.put(TextFieldMapper.CONTENT_TYPE, new TextParquetField());
        fieldMap.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordParquetField());
        fieldMap.put(IpFieldMapper.CONTENT_TYPE, new IpParquetField());
    }

    private static void registerBinaryFields(Map<String, ParquetField> fieldMap) {
        fieldMap.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryParquetField());
    }
}
