/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import org.opensearch.dsl.aggregation.FieldTypeLookup;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds a stubbed {@link FieldTypeLookup} from a golden file's {@code indexMapping}
 * (field name → SQL type name), mirroring how {@link CalciteTestInfra} builds the schema
 * from the same map. Terms rendering resolves key types and formats through it exactly
 * as production resolves them through the real index mapping.
 */
public final class TestMapperServices {

    private TestMapperServices() {}

    /**
     * Creates a field-type lookup whose {@code fieldType(name)} resolves each mapped field to the
     * {@link MappedFieldType} matching its golden SQL type; unmapped names resolve to null.
     *
     * @param indexMapping field name → SQL type name, as in golden files
     */
    public static FieldTypeLookup fromSqlMapping(Map<String, String> indexMapping) {
        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        for (Map.Entry<String, String> entry : indexMapping.entrySet()) {
            fieldTypes.put(entry.getKey(), toFieldType(entry.getKey(), entry.getValue()));
        }
        return fieldTypes::get;
    }

    private static MappedFieldType toFieldType(String name, String sqlType) {
        switch (sqlType) {
            case "VARCHAR":
                return new KeywordFieldMapper.KeywordFieldType(name);
            case "INTEGER":
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.INTEGER);
            case "BIGINT":
            case "UNSIGNED_LONG":
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
            case "DOUBLE":
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.DOUBLE);
            case "FLOAT":
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.FLOAT);
            case "BOOLEAN":
                return new BooleanFieldMapper.BooleanFieldType(name);
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP_NANOS":
                return new DateFieldMapper.DateFieldType(name);
            default:
                if (sqlType.startsWith("SCALED_FLOAT")) {
                    return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.DOUBLE);
                }
                return new KeywordFieldMapper.KeywordFieldType(name);
        }
    }
}
