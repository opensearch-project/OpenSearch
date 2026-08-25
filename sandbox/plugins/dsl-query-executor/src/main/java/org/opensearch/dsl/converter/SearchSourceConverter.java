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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Converts {@link SearchSourceBuilder} DSL into Calcite {@link QueryPlans}.
 *
 * <p>Builds its own Calcite planning infrastructure from the {@link SchemaPlus} provided
 * by the analytics engine.
 */
public class SearchSourceConverter {

    /** Immutable after creation with stateless translators — shared across all requests. */
    private static final QueryRegistry QUERY_REGISTRY = QueryRegistryFactory.create();

    private final RelDataTypeFactory typeFactory;
    private final CalciteCatalogReader catalogReader;
    private final FilterConverter filterConverter;
    private final ProjectConverter projectConverter;
    private final SortConverter sortConverter;
    private final AggregateConverter aggConverter;
    private final PreAggregateConverter preAggConverter;
    private final PostAggregateConverter postAggConverter;
    private final AggregationRegistry aggRegistry;
    private final AggregationTreeWalker treeWalker;

    /**
     * Initializes planning infrastructure without mapping resolution — conversion-only use;
     * terms rendering fails without a MapperService. Intended for tests.
     *
     * @param schema Calcite schema with index tables from the analytics engine
     */
    public SearchSourceConverter(SchemaPlus schema) {
        this(schema, () -> null);
    }

    /**
     * Initializes planning infrastructure from the given schema.
     *
     * @param schema Calcite schema with index tables from the analytics engine
     * @param mapperServiceSupplier supplies the target index's MapperService for response key
     *        type and format resolution; evaluated lazily. Supplying null skips
     *        mapping-dependent validation and fails terms rendering.
     */
    public SearchSourceConverter(SchemaPlus schema, Supplier<MapperService> mapperServiceSupplier) {
        // TODO: Once Analytics plugin starts providing the RelOptTable, use it directly —
        // no need to reconstruct typeFactory, CatalogReader, and planning infrastructure here.
        this.typeFactory = new SqlTypeFactoryImpl(DslTypeSystems.NANO_TIMESTAMP);

        CalciteSchema rootSchema = CalciteSchema.from(schema);
        this.catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );

        this.filterConverter = new FilterConverter(QUERY_REGISTRY);
        this.projectConverter = new ProjectConverter();
        this.sortConverter = new SortConverter();
        this.aggConverter = new AggregateConverter();
        this.preAggConverter = new PreAggregateConverter();
        this.postAggConverter = new PostAggregateConverter();

        this.aggRegistry = AggregationRegistryFactory.create(mapperServiceSupplier);
        this.treeWalker = new AggregationTreeWalker(aggRegistry);
    }

    /** Returns the aggregation registry used by this converter. */
    public AggregationRegistry getAggregationRegistry() {
        return aggRegistry;
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

        // Request-scoped workspace for the hits/aggregation plans. The count and eligible-count
        // plans below build in their own workspaces: they are submitted to the engine
        // concurrently with these plans and must not share a metadata cache (see newCluster).
        RelOptCluster cluster = newCluster();
        ConversionContext ctx = new ConversionContext(searchSource, cluster, table);
        RelNode base = buildBase(ctx);

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

        // Aggregation path: Scan → Filter → PreAggregate → Aggregate → [Having] → bound+order
        // (one plan per bucket aggregation — size, min_doc_count, and order are baked in;
        // nested levels additionally semi-join their parent level's plan for the per-parent bound)
        List<AggregationMetadata> metadataList = hasAggs
            ? treeWalker.walk(searchSource.aggregations().getAggregatorFactories(), table.getRowType(), cluster.getTypeFactory())
            : List.of();
        // The walker emits parents before their children, so a child's parent plan is
        // always already built when the child needs it as its semi-join input.
        Map<String, RelNode> builtPlansByPath = new LinkedHashMap<>();
        for (AggregationMetadata metadata : metadataList) {
            ConversionContext aggCtx = ctx.withAggregationMetadata(metadata);
            if (metadata.getPerParentFetch() != null) {
                List<String> path = metadata.getAggNamePath();
                RelNode parentPlan = builtPlansByPath.get(aggPathKey(path.subList(0, path.size() - 1)));
                if (parentPlan == null) {
                    throw new ConversionException(
                        "Parent plan not built before nested plan [" + String.join(",", path) + "] — walker order broken"
                    );
                }
                aggCtx = aggCtx.withParentPlan(parentPlan);
            }
            RelNode aggInput = preAggConverter.convert(base, aggCtx);
            RelNode aggs = aggConverter.convert(aggInput, metadata);
            aggs = postAggConverter.convert(aggs, aggCtx);
            builtPlansByPath.put(aggPathKey(metadata.getAggNamePath()), aggs);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, aggs, metadata));
        }

        // Accounting: a bounded root plan discards its tail engine-side, so each needs the
        // eligible-doc total that sum_other_doc_count is subtracted from. Nested levels carry
        // theirs inline as the _parent_eligible window column; unbounded plans discard nothing.
        Map<String, Integer> eligibleFieldByAggName = new LinkedHashMap<>();
        boolean totalServesAsEligibleCount = false;
        for (AggregationMetadata metadata : metadataList) {
            if (metadata.getFetch() == null) {
                continue;
            }
            String aggName = metadata.getAggNamePath().get(0); // fetch implies a root-level, single-field plan
            if (metadata.getHavingMinDocCount() != null) {
                // min_doc_count > 1: the eligible count must exclude below-threshold groups
                // (classic reduce drops them without counting them as "other"), which no flat
                // count can see — it needs its own HAVING-filtered plan. Checked before missing:
                // the threshold trumps substitution, and the plan's pre-aggregate input applies
                // the same substitution anyway.
                builder.add(
                    new QueryPlans.QueryPlan(QueryPlans.Type.COUNT, buildHavingEligibleCountPlan(searchSource, table, metadata, aggName))
                );
            } else if (metadata.eligibleDocCountIsTotal()) {
                // missing substitution makes every matching doc eligible — COUNT(*) serves
                totalServesAsEligibleCount = true;
            } else {
                String fieldName = metadata.getGroupByFieldNames().get(0);
                RelDataTypeField field = base.getRowType().getField(fieldName, false, false);
                if (field == null) {
                    throw new ConversionException("Group-by field '" + fieldName + "' not found in schema");
                }
                eligibleFieldByAggName.put(aggName, field.getIndex());
            }
        }
        RelNode flatCountPlan = buildFlatCountPlan(table, eligibleFieldByAggName, searchSource, totalServesAsEligibleCount);
        if (flatCountPlan != null) {
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.COUNT, flatCountPlan));
        }

        return builder.build();
    }

    /** Canonical plan key — see {@link AggregationMetadata#pathKey}. */
    private static String aggPathKey(List<String> aggNamePath) {
        return AggregationMetadata.pathKey(aggNamePath);
    }

    /**
     * Creates a fresh workspace (cluster) for one plan family. Calcite's per-cluster metadata
     * cache is not thread-safe, and its cycle detection assumes a single planning thread —
     * concurrent metadata queries over a shared cache misread each other's in-progress markers
     * as cycles ({@code CyclicMetadataException}). Plans the engine may plan concurrently must
     * therefore not share a cluster, nor any {@code RelNode}: a node is permanently bound to
     * the cluster it was created in, and metadata lookups follow {@code node.getCluster()}.
     */
    private RelOptCluster newCluster() {
        return RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), new RexBuilder(typeFactory));
    }

    /** Builds a plan family's private base — Scan → Filter — inside the context's own workspace. */
    private RelNode buildBase(ConversionContext ctx) throws ConversionException {
        RelNode scan = LogicalTableScan.create(ctx.getCluster(), ctx.getTable(), List.of());
        return filterConverter.convert(scan, ctx);
    }

    /**
     * Builds the single-row flat COUNT plan: {@code COUNT(*)} for {@code hits.total}, plus one
     * null-skipping {@code COUNT(field)} per bounded root aggregation as its
     * eligible-doc count (the total {@code sum_other_doc_count} is subtracted from).
     *
     * <p>Returns null when nothing needs counting: {@code track_total_hits: false}, no
     * eligible-count columns, and no {@code missing}-substituted aggregation riding on
     * {@code COUNT(*)}.
     */
    private RelNode buildFlatCountPlan(
        RelOptTable table,
        Map<String, Integer> eligibleFieldByAggName,
        SearchSourceBuilder searchSource,
        boolean totalServesAsEligibleCount
    ) throws ConversionException {
        boolean trackTotalHitsDisabled = searchSource.trackTotalHitsUpTo() != null
            && searchSource.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_DISABLED;
        if (trackTotalHitsDisabled && eligibleFieldByAggName.isEmpty() && !totalServesAsEligibleCount) {
            return null;
        }

        // Private workspace + rebuilt Scan → Filter: this plan executes concurrently with the
        // main plans and must not share their cluster or nodes (see newCluster). The rebuilt
        // base has the same row type, so the eligible-count field indices stay valid.
        ConversionContext countCtx = new ConversionContext(searchSource, newCluster(), table);
        RelNode base = buildBase(countCtx);

        RelDataType bigint = countCtx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        List<AggregateCall> calls = new ArrayList<>();
        calls.add(
            AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                List.of(),
                -1,
                RelCollations.EMPTY,
                bigint,
                QueryPlans.COUNT_TOTAL_COLUMN
            )
        );
        for (Map.Entry<String, Integer> entry : eligibleFieldByAggName.entrySet()) {
            calls.add(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(entry.getValue()),
                    -1,
                    RelCollations.EMPTY,
                    bigint,
                    QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + entry.getKey()
                )
            );
        }
        return LogicalAggregate.create(base, ImmutableBitSet.of(), null, calls);
    }

    /**
     * Builds the eligible-count plan for a {@code min_doc_count} > 1
     * aggregation: total documents in the groups that pass the threshold.
     *
     * <pre>
     * Aggregate(SUM(_count) AS _eligible$&lt;aggName&gt;)
     *   Filter(_count &gt;= min_doc_count)
     *     Aggregate(GROUP BY field, COUNT(*) AS _count)
     *       &lt;pre-aggregate input, rebuilt in this plan's own workspace&gt;
     * </pre>
     *
     * Built over an identically-constructed pre-aggregate input — its own workspace and nodes
     * (see {@link #newCluster}), same converters — so null exclusion and {@code missing}
     * substitution agree between the aggregation plan and its eligible count.
     */
    private RelNode buildHavingEligibleCountPlan(
        SearchSourceBuilder searchSource,
        RelOptTable table,
        AggregationMetadata metadata,
        String aggName
    ) throws ConversionException {
        ConversionContext ctx = new ConversionContext(searchSource, newCluster(), table).withAggregationMetadata(metadata);
        RelNode aggInput = preAggConverter.convert(buildBase(ctx), ctx);

        RelDataType bigint = ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);

        AggregateCall groupCount = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            -1,
            RelCollations.EMPTY,
            bigint,
            AggregationMetadataBuilder.IMPLICIT_COUNT_NAME
        );
        RelNode grouped = LogicalAggregate.create(aggInput, metadata.getGroupByBitSet(), null, List.of(groupCount));

        RelDataTypeField countField = grouped.getRowType().getField(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME, false, false);
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RexNode threshold = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(countField.getType(), countField.getIndex()),
            rexBuilder.makeLiteral(metadata.getHavingMinDocCount(), bigint, false)
        );
        RelNode filtered = LogicalFilter.create(grouped, threshold);

        RelDataType nullableBigint = ctx.getCluster().getTypeFactory().createTypeWithNullability(bigint, true);
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(countField.getIndex()),
            -1,
            RelCollations.EMPTY,
            nullableBigint,
            QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + aggName
        );
        return LogicalAggregate.create(filtered, ImmutableBitSet.of(), null, List.of(sum));
    }

    private static boolean hasAggregations(SearchSourceBuilder searchSource) {
        return searchSource.aggregations() != null
            && searchSource.aggregations().getAggregatorFactories() != null
            && !searchSource.aggregations().getAggregatorFactories().isEmpty();
    }
}
