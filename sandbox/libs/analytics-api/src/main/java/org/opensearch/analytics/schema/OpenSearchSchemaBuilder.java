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
 * // TODO: This is for illustation - use version sql plugin has built and re-purpose to not call node-client
 */
public class OpenSearchSchemaBuilder {

    private OpenSearchSchemaBuilder() {}

    /**
     * Builds a Calcite SchemaPlus from the given ClusterState.
     * Each index becomes a table; each mapped field becomes a column.
     *
     * @param clusterState the current cluster state to derive schema from
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
     *   <li>keyword/text/match_only_text -> VARCHAR</li>
     *   <li>long -> BIGINT</li>
     *   <li>unsigned_long -> BIGINT</li>
     *   <li>integer -> INTEGER</li>
     *   <li>short -> SMALLINT</li>
     *   <li>byte -> TINYINT</li>
     *   <li>double -> DOUBLE</li>
     *   <li>float -> REAL</li>
     *   <li>scaled_float -> BIGINT</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>date -> TIMESTAMP</li>
     *   <li>date_nanos -> TIMESTAMP</li>
     *   <li>ip -> VARBINARY</li>
     *   <li>binary -> VARBINARY</li>
     *   <li>nested/object -> skip (not mapped)</li>
     *   <li>unknown -> throws IllegalArgumentException</li>
     * </ul>
     *
     * @param opensearchType the OpenSearch field type string
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        switch (opensearchType) {
            case "keyword":
            case "text":
            case "match_only_text":
                return SqlTypeName.VARCHAR;
            case "long":
            case "unsigned_long":
                // unsigned_long: values above 2^63 - 1 wrap into negatives because BIGINT is
                // signed and Substrait has no unsigned integer types. Smaller values are safe.
            case "scaled_float":
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
                return SqlTypeName.REAL;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            case "date":
            case "date_nanos":
                return SqlTypeName.TIMESTAMP;
            case "ip":
            case "binary":
                return SqlTypeName.VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported OpenSearch field type: " + opensearchType);
        }
    }

    private static AbstractTable buildTable(Map<String, Object> properties) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                addLeafFields(builder, typeFactory, properties, "");
                return builder.build();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static void addLeafFields(
        RelDataTypeFactory.Builder builder,
        RelDataTypeFactory typeFactory,
        Map<String, Object> properties,
        String pathPrefix
    ) {
        for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
            String fieldName = pathPrefix.isEmpty() ? fieldEntry.getKey() : pathPrefix + "." + fieldEntry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) fieldEntry.getValue();
            String fieldType = (String) fieldProps.get("type");
            // Object types: implicit when "properties" is present without "type", or explicit "type: object".
            // Recurse into sub-properties so dotted leaf paths ("city.location.latitude") appear as flat columns.
            if (fieldType == null || "object".equals(fieldType)) {
                Map<String, Object> nested = (Map<String, Object>) fieldProps.get("properties");
                if (nested != null) {
                    addLeafFields(builder, typeFactory, nested, fieldName);
                }
                continue;
            }
            // Nested type (array-of-sub-docs) is a different beast — deferred.
            if ("nested".equals(fieldType)) {
                continue;
            }
            SqlTypeName sqlType = mapFieldType(fieldType);
            builder.add(fieldName, typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true));
        }
    }
}
