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
     * Maps an OpenSearch field type string to a Calcite SqlTypeName, or {@code null} when the type
     * has no scalar Calcite representation here (geo_point, geo_shape, nested, completion, …) or
     * is unrecognized. Callers omit the column from the schema so a query referencing it surfaces
     * a Calcite "column not found" via the validator rather than a planner-time crash.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>keyword/text/match_only_text -> VARCHAR</li>
     *   <li>long/unsigned_long/scaled_float -> BIGINT</li>
     *   <li>integer -> INTEGER, short -> SMALLINT, byte -> TINYINT</li>
     *   <li>double -> DOUBLE, float/half_float -> REAL</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>date/date_nanos -> TIMESTAMP</li>
     *   <li>ip/binary -> VARBINARY</li>
     *   <li>everything else (geo_point, geo_shape, nested, object, flat_object, completion,
     *       constant_keyword, wildcard, alias, dense_vector, sparse_vector, percolator,
     *       *_range, token_count, version, plus genuinely unknown plugin types) -> {@code null}</li>
     * </ul>
     *
     * @param opensearchType the OpenSearch field type string
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        if (opensearchType == null) {
            return null;
        }
        switch (opensearchType) {
            case "keyword":
            case "text":
            case "match_only_text":
                return SqlTypeName.VARCHAR;
            case "long":
            case "unsigned_long":
                // unsigned_long: values above 2^63 - 1 wrap into negatives because BIGINT is
                // signed and Substrait has no unsigned integer types. Smaller values are safe.
                // TODO: values above 2^63 - 1 wrap into negatives. Drop the UInt64 → Int64 narrowing
                // (see schema_coerce.rs) when we have a proper solution.
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
            case "half_float":
                // half_float lands as Arrow Float16 on disk. Calcite has no fp16 type; widen to
                // REAL so the planner sees the same shape as a regular float column. The parquet
                // reader's SchemaAdapter casts Float16 → Float32 per batch.
                // TODO: every record batch goes through a Float16 → Float32 cast (see
                // schema_coerce.rs) and downstream operators see Float32. Drop the widening when
                // we have a proper solution.
                return SqlTypeName.REAL;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            case "date":
            case "date_nanos":
                return SqlTypeName.TIMESTAMP;
            case "ip":
            case "binary":
                // TODO: differentiate ip and binary as separate UDTs instead of collapsing both
                // to VARBINARY. With the type preserved, literals can be converted into the
                // on-disk byte form the planner expects.
                return SqlTypeName.VARBINARY;
            default:
                return null;
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
            if (sqlType == null) {
                // Unsupported (geo_point/shape/completion/…) or unknown plugin type. Drop the
                // column; a query referencing it surfaces a Calcite "column not found" via the
                // validator rather than a planning-time IllegalArgumentException.
                continue;
            }
            builder.add(fieldName, typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true));
        }
    }
}
