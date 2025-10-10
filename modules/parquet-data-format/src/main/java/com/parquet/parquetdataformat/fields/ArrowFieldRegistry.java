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
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for mapping OpenSearch field types to their corresponding Parquet field implementations.
 * This class maintains a centralized mapping between OpenSearch field type names and their
 * Arrow/Parquet field representations, enabling efficient field type resolution during
 * schema creation and data processing.
 *
 * <p>The registry is initialized once during class loading and provides thread-safe
 * read-only access to field mappings.</p>
 */
public final class ArrowFieldRegistry {

    /**
     * Immutable map containing the mapping from OpenSearch field type names to ParquetField instances.
     * This map is populated during class initialization and remains constant throughout the application lifecycle.
     */
    private static final Map<String, ParquetField> PARQUET_FIELD_MAP;

    // Static initialization block to populate the field registry
    static {
        PARQUET_FIELD_MAP = Collections.unmodifiableMap(createParquetFieldMap());
    }

    // Private constructor to prevent instantiation of utility class
    private ArrowFieldRegistry() {
        throw new UnsupportedOperationException("Registry class should not be instantiated");
    }

    /**
     * Creates and populates the mapping between OpenSearch field types and ParquetField instances.
     * This method is called once during class initialization to set up all supported field type mappings.
     *
     * @return a mutable map containing all field type mappings
     */
    private static Map<String, ParquetField> createParquetFieldMap() {
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
    }

    /**
     * Retrieves the ParquetField instance associated with the specified OpenSearch field type name.
     * This method provides thread-safe access to the field registry.
     *
     * @param typeName the OpenSearch field type name to look up
     * @return the corresponding ParquetField instance, or null if the type is not supported
     * @throws IllegalArgumentException if typeName is null or empty
     */
    public static ParquetField getParquetField(final String typeName) {
        if (typeName == null || typeName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field type name cannot be null or empty");
        }

        return PARQUET_FIELD_MAP.get(typeName);
    }

}
