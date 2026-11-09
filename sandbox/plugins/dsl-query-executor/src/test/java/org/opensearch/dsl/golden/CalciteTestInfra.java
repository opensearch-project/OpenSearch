/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.analytics.schema.UnsignedLongType;
import org.opensearch.dsl.converter.DslTypeSystems;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Builds Calcite planning infrastructure from a golden file's index mapping.
 *
 * <p>Mirrors the pattern in {@code TestUtils} and {@code SearchSourceConverter}'s
 * constructor, but constructs the schema dynamically from the golden file's
 * {@code indexMapping} field instead of using a hardcoded schema.
 */
public class CalciteTestInfra {

    private CalciteTestInfra() {}

    /**
     * Builds a complete Calcite infrastructure from a golden file's index mapping.
     *
     * @param indexName    the index name to register in the schema
     * @param indexMapping field name → SQL type name (e.g. "VARCHAR", "INTEGER")
     * @return an {@link InfraResult} containing the cluster, table, and schema
     * @throws IllegalArgumentException if indexMapping contains an unsupported type
     */
    public static InfraResult buildFromMapping(String indexName, Map<String, String> indexMapping) {
        Objects.requireNonNull(indexName, "indexName must not be null");
        Objects.requireNonNull(indexMapping, "indexMapping must not be null");

        // Mirrors SearchSourceConverter: TIMESTAMP max precision raised to 9 for date_nanos support.
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(DslTypeSystems.NANO_TIMESTAMP);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add(indexName, new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                RelDataTypeFactory.Builder builder = tf.builder();
                for (Map.Entry<String, String> entry : indexMapping.entrySet()) {
                    RelDataType fieldType = toRelDataType(entry.getValue(), tf);
                    builder.add(entry.getKey(), tf.createTypeWithNullability(fieldType, true));
                }
                return builder.build();
            }
        });

        CalciteCatalogReader reader = new CalciteCatalogReader(
            CalciteSchema.from(schema),
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );
        RelOptTable table = Objects.requireNonNull(reader.getTable(List.of(indexName)), "Table not found in schema: " + indexName);

        return new InfraResult(cluster, table, schema);
    }

    /**
     * Resolves a golden file type string to a full {@link RelDataType}, supporting both standard
     * SqlTypeNames and user-defined types (UNSIGNED_LONG, SCALED_FLOAT).
     */
    private static RelDataType toRelDataType(String goldenType, RelDataTypeFactory tf) {
        if ("UNSIGNED_LONG".equals(goldenType)) {
            return new UnsignedLongType(DslTypeSystems.NANO_TIMESTAMP, false);
        }
        if (goldenType.startsWith("SCALED_FLOAT")) {
            int factor = precisionFor(goldenType);
            if (factor < 0) {
                throw new IllegalArgumentException("SCALED_FLOAT requires a scaling factor, e.g. SCALED_FLOAT(100)");
            }
            return new ScaledFloatType(DslTypeSystems.NANO_TIMESTAMP, false, factor);
        }
        SqlTypeName sqlType = toSqlTypeName(goldenType);
        int precision = precisionFor(goldenType);
        return precision >= 0 ? tf.createSqlType(sqlType, precision) : tf.createSqlType(sqlType);
    }

    /**
     * Maps a golden file type string to a Calcite {@link SqlTypeName}.
     * TIMESTAMP is created with precision 3 (milliseconds) matching standard OpenSearch date fields.
     *
     * @throws IllegalArgumentException for unsupported type strings
     */
    private static SqlTypeName toSqlTypeName(String goldenType) {
        switch (goldenType) {
            case "VARCHAR":
                return SqlTypeName.VARCHAR;
            case "INTEGER":
                return SqlTypeName.INTEGER;
            case "BIGINT":
                return SqlTypeName.BIGINT;
            case "DOUBLE":
                return SqlTypeName.DOUBLE;
            case "FLOAT":
                return SqlTypeName.FLOAT;
            case "BOOLEAN":
                return SqlTypeName.BOOLEAN;
            case "DATE":
                return SqlTypeName.DATE;
            case "TIMESTAMP":
                return SqlTypeName.TIMESTAMP;
            case "TIMESTAMP_NANOS":
                return SqlTypeName.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unsupported SQL type in golden file indexMapping: " + goldenType);
        }
    }

    /** Returns the precision (or scaling factor) for a given golden file type string. TIMESTAMP defaults to 3 (millis), TIMESTAMP_NANOS to 9. A parenthesised number like SCALED_FLOAT(100) returns 100. */
    static int precisionFor(String goldenType) {
        if ("TIMESTAMP".equals(goldenType)) {
            return 3;
        }
        if ("TIMESTAMP_NANOS".equals(goldenType)) {
            return 9;
        }
        int open = goldenType.indexOf('(');
        if (open >= 0 && goldenType.endsWith(")")) {
            return Integer.parseInt(goldenType.substring(open + 1, goldenType.length() - 1));
        }
        return -1;
    }

    /** Result record containing the Calcite infrastructure built from a golden file mapping. */
    public record InfraResult(RelOptCluster cluster, RelOptTable table, SchemaPlus schema) {
    }
}
