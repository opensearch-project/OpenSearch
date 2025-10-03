/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.number.ByteParquetField;
import com.parquet.parquetdataformat.fields.number.DoubleParquetField;
import com.parquet.parquetdataformat.fields.number.FloatParquetField;
import com.parquet.parquetdataformat.fields.number.HalfFloatParquetField;
import com.parquet.parquetdataformat.fields.number.IntegerParquetField;
import com.parquet.parquetdataformat.fields.number.LongParquetField;
import com.parquet.parquetdataformat.fields.number.ShortParquetField;
import com.parquet.parquetdataformat.fields.number.UnsignedLongParquetField;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.HashMap;
import java.util.Map;

public class ArrowFieldRegistry {

    private static final Map<String, FieldType> FIELD_TYPE_MAP = new HashMap<>();
    private static final Map<String, ParquetField> PARQUET_FIELD_MAP = new HashMap<>();

    static {
        //TODO: darsaga check which fields can be nullable and which can not be

        // Number types
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(),
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.FLOAT.typeName(),
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.DOUBLE.typeName(),
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.BYTE.typeName(),
            FieldType.nullable(new ArrowType.Int(8, true)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.SHORT.typeName(),
            FieldType.nullable(new ArrowType.Int(16, true)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.INTEGER.typeName(),
            FieldType.nullable(new ArrowType.Int(32, true)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.LONG.typeName(),
            FieldType.nullable(new ArrowType.Int(64, true)));
        FIELD_TYPE_MAP.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(),
            FieldType.nullable(new ArrowType.Int(64, false)));

        // Other types
        FIELD_TYPE_MAP.put(DateFieldMapper.CONTENT_TYPE,
            FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
        FIELD_TYPE_MAP.put(BooleanFieldMapper.CONTENT_TYPE,
            FieldType.nullable(new ArrowType.Bool()));
        FIELD_TYPE_MAP.put(KeywordFieldMapper.CONTENT_TYPE,
            FieldType.nullable(new ArrowType.Utf8()));
        FIELD_TYPE_MAP.put(TextFieldMapper.CONTENT_TYPE,
            FieldType.nullable(new ArrowType.Utf8()));

        setUpParquetFieldMap();
    }

    private static void setUpParquetFieldMap() {

        //Number fields
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(), new HalfFloatParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.FLOAT.typeName(), new FloatParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.BYTE.typeName(), new ByteParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.SHORT.typeName(), new ShortParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.INTEGER.typeName(), new IntegerParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongParquetField());
        PARQUET_FIELD_MAP.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(), new UnsignedLongParquetField());

        //Date field
        PARQUET_FIELD_MAP.put(DateFieldMapper.CONTENT_TYPE, new DateParquetField());

        //Boolean field
        PARQUET_FIELD_MAP.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanParquetField());

        //Text field
        PARQUET_FIELD_MAP.put(TextFieldMapper.CONTENT_TYPE, new TextParquetField());

        //Keyword field
        PARQUET_FIELD_MAP.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordParquetField());
    }

    public static FieldType getFieldType(String typeName) {
        return FIELD_TYPE_MAP.get(typeName);
    }

    public static ParquetField getParquetField(String typeName) {
        return PARQUET_FIELD_MAP.get(typeName);
    }
}
