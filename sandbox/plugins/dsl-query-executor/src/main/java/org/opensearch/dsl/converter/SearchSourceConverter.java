/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Converts {@link SearchSourceBuilder} DSL into Calcite {@link QueryPlans}.
 *
 * <p>Builds its own Calcite planning infrastructure from the {@link SchemaPlus} provided
 * by the analytics engine.
 *
 */
public class SearchSourceConverter {

    private static final Logger logger = LogManager.getLogger(SearchSourceConverter.class);

    private final RelOptCluster cluster;
    private final CalciteCatalogReader catalogReader;

    /**
     * Initializes planning infrastructure from the given schema.
     *
     * @param schema Calcite schema with index tables from the analytics engine
     */
    public SearchSourceConverter(SchemaPlus schema) {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        CalciteSchema rootSchema = CalciteSchema.from(schema);
        this.catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );
    }

    /**
     * Converts DSL for the given index into query plans.
     *
     * @param searchSource the DSL query
     * @param indexName target index
     * @return one or more query plans
     */
    public QueryPlans convert(SearchSourceBuilder searchSource, String indexName) {

        RelOptTable table = catalogReader.getTable(List.of(indexName));
        if (table == null) {
            throw new IllegalArgumentException("Index not found in schema: " + indexName);
        }

        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        // TODO: build full plan: Scan → Filter → Project → Sort / Aggregate

        return new QueryPlans.Builder()
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, scan))
            .build();
    }
}
