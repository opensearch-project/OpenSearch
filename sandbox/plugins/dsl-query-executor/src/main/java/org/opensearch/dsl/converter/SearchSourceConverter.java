/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Converts {@link SearchSourceBuilder} DSL into Calcite {@link QueryPlans}.
 *
 * <p>Builds its own Calcite planning infrastructure from the {@link SchemaPlus} provided
 * by the analytics engine.
 */
public class SearchSourceConverter {

    private final RelOptCluster cluster;
    private final CalciteCatalogReader catalogReader;
    private final FilterConverter filterConverter;
    private final ProjectConverter projectConverter;
    private final SortConverter sortConverter;

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

        this.filterConverter = new FilterConverter(QueryRegistryFactory.create());
        this.projectConverter = new ProjectConverter();
        this.sortConverter = new SortConverter();
    }

    /**
     * Converts DSL for the given index into query plans.
     *
     * @param searchSource the DSL query
     * @param indexName target index
     * @return one or more query plans
     * @throws ConversionException if DSL conversion fails
     */
    public QueryPlans convert(SearchSourceBuilder searchSource, String indexName) throws ConversionException {
        RelOptTable table = catalogReader.getTable(List.of(indexName));
        if (table == null) {
            throw new IllegalArgumentException("Index not found in schema: " + indexName);
        }

        ConversionContext ctx = new ConversionContext(searchSource, cluster, table);

        // Shared base: Scan → Filter
        RelNode base = LogicalTableScan.create(cluster, table, List.of());
        base = filterConverter.convert(base, ctx);

        int size = searchSource.size() != -1 ? searchSource.size() : SearchService.DEFAULT_SIZE;
        boolean hasAggs = hasAggregations(searchSource);

        QueryPlans.Builder builder = new QueryPlans.Builder();

        // Hits path: Scan → Filter → Project → Sort
        if (size > 0 || !hasAggs) {
            RelNode hits = projectConverter.convert(base, ctx);
            hits = sortConverter.convert(hits, ctx);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, hits));
        }

        // Aggregation path: Scan → Filter → Aggregate
        if (hasAggs) {
            RelNode aggs = base;
            // TODO: aggs = applyAggregate(aggs, ctx);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, aggs));
        }

        return builder.build();
    }

    private static boolean hasAggregations(SearchSourceBuilder searchSource) {
        return searchSource.aggregations() != null
            && searchSource.aggregations().getAggregatorFactories() != null
            && !searchSource.aggregations().getAggregatorFactories().isEmpty();
    }
}
