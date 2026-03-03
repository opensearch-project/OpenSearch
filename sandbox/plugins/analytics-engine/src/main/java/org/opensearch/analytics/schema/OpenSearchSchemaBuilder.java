/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.Map;

/**
 * Builds a Calcite {@link SchemaPlus} from OpenSearch {@link ClusterState} index mappings.
 *
 * <p>One Calcite table per index. Reads field types from index mapping properties.
 * Navigates: IndexMetadata -> MappingMetadata -> sourceAsMap() -> "properties" -> per-field "type".
 * // TODO: The engine should provide this, move it there and consume here
 */
public class OpenSearchSchemaBuilder {

    private OpenSearchSchemaBuilder() {}

    /**
     * Builds a Calcite SchemaPlus from the given ClusterState.
     * Each index becomes a table; each mapped field becomes a column.
     */
    public static SchemaPlus buildSchema(ClusterState clusterState) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();

        for (Map.Entry<String, IndexMetadata> entry : clusterState.metadata().indices().entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata indexMetadata = entry.getValue();
            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> sourceMap = mapping.sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) sourceMap.get("properties");
            if (properties == null) {
                continue;
            }

            schemaPlus.add(indexName, buildTable(properties));
        }

        return schemaPlus;
    }

    /**
     * Maps an OpenSearch field type string to a Calcite SqlTypeName.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>keyword/text -> VARCHAR</li>
     *   <li>long -> BIGINT</li>
     *   <li>integer -> INTEGER</li>
     *   <li>short -> SMALLINT</li>
     *   <li>byte -> TINYINT</li>
     *   <li>double -> DOUBLE</li>
     *   <li>float -> FLOAT</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>date -> TIMESTAMP</li>
     *   <li>ip -> VARCHAR</li>
     *   <li>nested/object -> skip (not mapped)</li>
     *   <li>unknown -> VARCHAR (default)</li>
     * </ul>
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        switch (opensearchType) {
            case "keyword":
            case "text":
            case "ip":
                return SqlTypeName.VARCHAR;
            case "long":
                return SqlTypeName.BIGINT;
            case "integer":
                return SqlTypeName.INTEGER;
            case "short":
                return SqlTypeName.SMALLINT;
            case "byte":
                return SqlTypeName.TINYINT;
            case "double":
                return SqlTypeName.DOUBLE;
            case "float":
                return SqlTypeName.FLOAT;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            case "date":
                return SqlTypeName.TIMESTAMP;
            default:
                return SqlTypeName.VARCHAR;
        }
    }

    private static AbstractTable buildTable(Map<String, Object> properties) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
                    String fieldName = fieldEntry.getKey();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldProps = (Map<String, Object>) fieldEntry.getValue();
                    String fieldType = (String) fieldProps.get("type");
                    if (fieldType == null) {
                        continue;
                    }
                    // Skip nested and object types
                    if ("nested".equals(fieldType) || "object".equals(fieldType)) {
                        continue;
                    }
                    SqlTypeName sqlType = mapFieldType(fieldType);
                    builder.add(fieldName, typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true));
                }
                return builder.build();
            }
        };
    }
}
