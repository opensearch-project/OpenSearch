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
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.BucketsPathResolver;
import org.opensearch.dsl.aggregation.pipeline.PipelinePlanComposer;
import org.opensearch.dsl.aggregation.pipeline.PipelineRegistry;
import org.opensearch.dsl.aggregation.pipeline.PipelineTranslator;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private final AggregateConverter aggConverter;
    private final PostAggregateConverter postAggConverter;
    private final AggregationTreeWalker treeWalker;
    private final AggregationRegistry aggRegistry;
    private final PipelineRegistry pipelineRegistry;

    /**
     * Initializes planning infrastructure from the given schema.
     *
     * @param schema Calcite schema with index tables from the analytics engine
     */
    public SearchSourceConverter(SchemaPlus schema) {
        // TODO: Once Analytics plugin starts providing the RelOptTable, use it directly —
        // no need to reconstruct typeFactory, CatalogReader, and planning infrastructure here.
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
        this.aggConverter = new AggregateConverter();
        this.postAggConverter = new PostAggregateConverter();

        this.aggRegistry = AggregationRegistryFactory.create();
        this.treeWalker = new AggregationTreeWalker(aggRegistry);
        this.pipelineRegistry = PipelineRegistry.create();
    }

    /** Returns the aggregation registry used by this converter. */
    public AggregationRegistry getAggregationRegistry() {
        return aggRegistry;
    }

    /** Returns the pipeline aggregation registry used by this converter. */
    public PipelineRegistry getPipelineRegistry() {
        return pipelineRegistry;
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
        // size=0 skips hits — total doc count comes from analytics plugin metadata
        if (size > 0) {
            RelNode hits = projectConverter.convert(base, ctx);
            hits = sortConverter.convert(hits, ctx);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, hits));
        }

        // Aggregation path: Scan → Filter → Aggregate → PostAggregate (one per granularity level)
        List<AggregationMetadata> metadataList = List.of();
        Map<AggregationMetadata, RelNode> preSortAggregates = new LinkedHashMap<>();
        if (hasAggs) {
            metadataList = treeWalker.walk(
                searchSource.aggregations().getAggregatorFactories(),
                table.getRowType(),
                cluster.getTypeFactory()
            );
            for (AggregationMetadata metadata : metadataList) {
                ConversionContext aggCtx = ctx.withAggregationMetadata(metadata);
                RelNode aggs = aggConverter.convert(base, metadata);
                preSortAggregates.put(metadata, aggs);
                aggs = postAggConverter.convert(aggs, aggCtx);
                builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, aggs, metadata));
            }
        }

        // Pipeline path: sibling aggregate shaped to its visible buckets → second-level aggregate
        convertPipelines(searchSource, builder, metadataList, preSortAggregates);

        return builder.build();
    }

    /**
     * Converts sibling pipeline aggregations into {@link QueryPlans.Type#PIPELINE} plans.
     * Pipelines targeting the same sibling share one plan; results map back by column name.
     */
    private void convertPipelines(
        SearchSourceBuilder searchSource,
        QueryPlans.Builder builder,
        List<AggregationMetadata> metadataList,
        Map<AggregationMetadata, RelNode> preSortAggregates
    ) throws ConversionException {
        if (searchSource.aggregations() == null) {
            return;
        }
        Collection<PipelineAggregationBuilder> pipelines = searchSource.aggregations().getPipelineAggregatorFactories();
        if (pipelines == null || pipelines.isEmpty()) {
            return;
        }
        Collection<AggregationBuilder> rootAggs = searchSource.aggregations().getAggregatorFactories();

        Map<TermsAggregationBuilder, List<PipelinePlanComposer.PipelineTarget>> bySibling = new LinkedHashMap<>();
        for (PipelineAggregationBuilder pipeline : pipelines) {
            PipelineTranslator<PipelineAggregationBuilder> translator = pipelineRegistry.get(pipeline.getClass());
            if (translator == null) {
                throw new ConversionException(
                    "pipeline aggregation [" + pipeline.getName() + "] of type [" + pipeline.getWriteableName() + "] is not supported"
                );
            }
            BucketsPathResolver.ResolvedBucketsPath resolved = BucketsPathResolver.resolve(pipeline, rootAggs, aggRegistry);
            bySibling.computeIfAbsent(resolved.sibling(), s -> new ArrayList<>())
                .add(new PipelinePlanComposer.PipelineTarget(pipeline, resolved.metricColumn()));
        }

        for (Map.Entry<TermsAggregationBuilder, List<PipelinePlanComposer.PipelineTarget>> entry : bySibling.entrySet()) {
            TermsAggregationBuilder sibling = entry.getKey();
            AggregationMetadata metadata = findSiblingMetadata(sibling, metadataList);
            RelNode plan = PipelinePlanComposer.compose(
                entry.getValue(),
                sibling,
                metadata,
                preSortAggregates.get(metadata),
                cluster.getRexBuilder(),
                pipelineRegistry
            );
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.PIPELINE, plan, null));
        }
    }

    /** Finds the walker metadata whose GROUP BY matches the sibling's own grouping fields. */
    private AggregationMetadata findSiblingMetadata(TermsAggregationBuilder sibling, List<AggregationMetadata> metadataList)
        throws ConversionException {
        BucketTranslator<AggregationBuilder> bucketTranslator = aggRegistry.getBucket(sibling.getClass());
        if (bucketTranslator == null) {
            throw new ConversionException("No bucket translator registered for sibling aggregation [" + sibling.getName() + "]");
        }
        List<String> groupFields = bucketTranslator.getGrouping(sibling).getFieldNames();
        for (AggregationMetadata metadata : metadataList) {
            if (metadata.getGroupByFieldNames().equals(groupFields)) {
                return metadata;
            }
        }
        throw new ConversionException("No aggregation plan produced for pipeline sibling [" + sibling.getName() + "]");
    }

    private static boolean hasAggregations(SearchSourceBuilder searchSource) {
        return searchSource.aggregations() != null
            && searchSource.aggregations().getAggregatorFactories() != null
            && !searchSource.aggregations().getAggregatorFactories().isEmpty();
    }
}
