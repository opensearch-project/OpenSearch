/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Shared test utilities for creating Calcite objects.
 * Mockito can't mock Calcite classes due to classloader conflicts with OpenSearch's
 * RandomizedRunner, so tests use real objects built here.
 *
 * Standard test schema: name (VARCHAR), price (INTEGER), brand (VARCHAR), rating (DOUBLE).
 */
public class TestUtils {

    private TestUtils() {}

    /** Creates a LogicalTableScan backed by the standard test schema. */
    public static LogicalTableScan createTestRelNode() {
        Infra infra = buildInfra();
        return LogicalTableScan.create(infra.cluster, infra.table, List.of());
    }

    /** Creates a ConversionContext with the given search source and standard test schema. */
    public static ConversionContext createContext(SearchSourceBuilder searchSource) {
        Infra infra = buildInfra();
        return new ConversionContext(searchSource, infra.cluster, infra.table);
    }

    /** Creates a ConversionContext with an empty search source and standard test schema. */
    public static ConversionContext createContext() {
        return createContext(new SearchSourceBuilder());
    }

    private static Infra buildInfra() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("name", SqlTypeName.VARCHAR)
                    .add("price", SqlTypeName.INTEGER)
                    .add("brand", SqlTypeName.VARCHAR)
                    .add("rating", SqlTypeName.DOUBLE)
                    .build();
            }
        });

        CalciteCatalogReader reader = new CalciteCatalogReader(
            CalciteSchema.from(schema),
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );
        RelOptTable table = Objects.requireNonNull(reader.getTable(List.of("test")));
        return new Infra(cluster, table);
    }

    private record Infra(RelOptCluster cluster, RelOptTable table) {}
}
