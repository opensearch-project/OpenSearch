/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.plugins.fields;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.fields.core.data.TokenCountParquetField;
import com.parquet.parquetdataformat.fields.core.data.BooleanParquetField;
import com.parquet.parquetdataformat.fields.core.data.DateParquetField;
import com.parquet.parquetdataformat.fields.core.data.IpParquetField;
import com.parquet.parquetdataformat.fields.core.data.KeywordParquetField;
import com.parquet.parquetdataformat.fields.core.data.TextParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.ByteParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.DoubleParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.FloatParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.HalfFloatParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.IntegerParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.ShortParquetField;
import com.parquet.parquetdataformat.fields.core.data.number.UnsignedLongParquetField;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Core data fields plugin that provides Parquet field implementations for all built-in OpenSearch field types.
 * This plugin is automatically registered and provides the foundation field type support for Parquet storage.
 */
public class CoreDataFieldPlugin implements ParquetFieldPlugin {

    @Override
    public Map<String, ParquetField> getParquetFields() {
        final Map<String, ParquetField> fieldMap = new HashMap<>();

        // Register numeric field types
        registerNumericFields(fieldMap);

        // Register temporal field types
        registerTemporalFields(fieldMap);

        // Register boolean field types
        registerBooleanFields(fieldMap);

        // Register text-based field types
        registerTextFields(fieldMap);

        return fieldMap;
    }

    /**
     * Registers all numeric field type mappings.
     *
     * @param fieldMap the map to populate with numeric field mappings
     */
    private static void registerNumericFields(final Map<String, ParquetField> fieldMap) {
        // Floating point types
        fieldMap.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(), new HalfFloatParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.FLOAT.typeName(), new FloatParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleParquetField());

        // Integer types
        fieldMap.put(NumberFieldMapper.NumberType.BYTE.typeName(), new ByteParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.SHORT.typeName(), new ShortParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.INTEGER.typeName(), new IntegerParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongParquetField());
        fieldMap.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(), new UnsignedLongParquetField());
        fieldMap.put("token_count", new TokenCountParquetField());
    }

    /**
     * Registers all temporal field type mappings.
     *
     * @param fieldMap the map to populate with temporal field mappings
     */
    private static void registerTemporalFields(final Map<String, ParquetField> fieldMap) {
        fieldMap.put(DateFieldMapper.CONTENT_TYPE, new DateParquetField());
    }

    /**
     * Registers all boolean field type mappings.
     *
     * @param fieldMap the map to populate with boolean field mappings
     */
    private static void registerBooleanFields(final Map<String, ParquetField> fieldMap) {
        fieldMap.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanParquetField());
    }

    /**
     * Registers all text-based field type mappings.
     *
     * @param fieldMap the map to populate with text field mappings
     */
    private static void registerTextFields(final Map<String, ParquetField> fieldMap) {
        fieldMap.put(TextFieldMapper.CONTENT_TYPE, new TextParquetField());
        fieldMap.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordParquetField());
        fieldMap.put(IpFieldMapper.CONTENT_TYPE, new IpParquetField());
    }
}
