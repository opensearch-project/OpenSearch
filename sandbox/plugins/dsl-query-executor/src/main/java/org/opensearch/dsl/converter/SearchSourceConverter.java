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
        // Fail loudly (400) on features this path does not implement, instead of silently
        // ignoring them and returning wrong results with 200.
        UnsupportedSearchParameters.reject(searchSource);

        RelOptTable table = catalogReader.getTable(List.of(indexName));
        if (table == null) {
            throw new IllegalArgumentException("Index not found in schema: " + indexName);
        }

        // Request-scoped workspace for the hits/aggregation plans. The count and eligible-count
        // plans below build in their own workspaces: they are submitted to the engine
        // concurrently with these plans and must not share a metadata cache (see newCluster).
        int size = searchSource.size() != -1 ? searchSource.size() : SearchService.DEFAULT_SIZE;
        boolean hasAggs = hasAggregations(searchSource);

        QueryPlans.Builder builder = new QueryPlans.Builder();

        // Every plan below gets its own workspace — its own cluster and its own Scan → Filter
        // nodes. The engine plans the plans of one request on different threads, and a plan's
        // metadata lookups follow the cluster of the nodes they touch, so one shared cluster or
        // one shared node puts two threads on the same unguarded metadata cache (see newCluster).

        // Hits path: Scan → Filter → Project → Sort
        // size=0 skips hits — total doc count comes from analytics plugin metadata
        if (size > 0) {
            ConversionContext hitsCtx = new ConversionContext(searchSource, newCluster(), table);
            RelNode hits = projectConverter.convert(buildBase(hitsCtx), hitsCtx);
            hits = sortConverter.convert(hits, hitsCtx);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, hits));
        }

        // Aggregation path: Scan → Filter → PreAggregate → Aggregate → [Having] → bound+order
        // (one plan per bucket aggregation — size, min_doc_count, and order are baked in;
        // nested levels additionally semi-join their parent level's plan for the per-parent bound)
        List<AggregationMetadata> metadataList = hasAggs
            ? treeWalker.walk(searchSource.aggregations().getAggregatorFactories(), table.getRowType(), typeFactory)
            : List.of();
        // The walker emits parents before their children, so a child's parent metadata is always
        // already registered by the time the child rebuilds that level in its own workspace.
        Map<String, AggregationMetadata> metadataByPath = new LinkedHashMap<>();
        for (AggregationMetadata metadata : metadataList) {
            metadataByPath.put(aggPathKey(metadata.getAggNamePath()), metadata);
            ConversionContext planCtx = new ConversionContext(searchSource, newCluster(), table);
            RelNode aggs = buildAggPlan(metadata, planCtx, buildBase(planCtx), metadataByPath);
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
                // The table's row type, not a plan's: a plan's base is Scan → Filter and a
                // Filter's row type is its input's, so this is the same row type every plan sees
                // — and no plan's nodes have to be reachable from here to read an index off it.
                RelDataTypeField field = table.getRowType().getField(fieldName, false, false);
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

        QueryPlans plans = builder.build();
        // Translating the query clause is what validates it, and every translation now happens
        // inside some plan's own buildBase. A request that emits no plan at all — size=0, no
        // aggregations, track_total_hits:false, so not even a COUNT plan — would therefore never
        // look at its query, and a malformed one would come back as an empty 200 the caller cannot
        // tell apart from "no results" instead of the 400 the conversion error becomes. Translate
        // it once here and drop the result: this workspace is fresh and unshared, so it shares no
        // planning state with anything, and a valid query still yields the normal empty result.
        if (plans.getAll().isEmpty()) {
            buildBase(new ConversionContext(searchSource, newCluster(), table));
        }
        return plans;
    }

    /**
     * Builds one aggregation level's plan inside {@code ctx}'s own workspace, rebuilding any
     * ancestor level it semi-joins there too.
     *
     * @param metadata the level to build
     * @param ctx this plan's workspace context, carrying neither metadata nor a parent plan
     * @param base this plan's own {@code Scan → Filter} subtree
     * @param metadataByPath every level the walker has emitted so far, by canonical path key
     * @return the level's plan, entirely inside {@code ctx}'s cluster
     * @throws ConversionException if conversion fails, or the walker emitted a child before its parent
     */
    private RelNode buildAggPlan(
        AggregationMetadata metadata,
        ConversionContext ctx,
        RelNode base,
        Map<String, AggregationMetadata> metadataByPath
    ) throws ConversionException {
        ConversionContext aggCtx = ctx.withAggregationMetadata(metadata);
        if (metadata.getPerParentFetch() != null) {
            List<String> path = metadata.getAggNamePath();
            AggregationMetadata parent = metadataByPath.get(aggPathKey(path.subList(0, path.size() - 1)));
            if (parent == null) {
                throw new ConversionException(
                    "Parent plan not built before nested plan [" + String.join(",", path) + "] — walker order broken"
                );
            }
            aggCtx = aggCtx.withParentPlan(buildAggPlan(parent, ctx, base, metadataByPath));
        }
        RelNode aggInput = preAggConverter.convert(base, aggCtx);
        return postAggConverter.convert(aggConverter.convert(aggInput, metadata), aggCtx);
    }

    /** Canonical plan key — see {@link AggregationMetadata#pathKey}. */
    private static String aggPathKey(List<String> aggNamePath) {
        return AggregationMetadata.pathKey(aggNamePath);
    }

    /**
     * Creates a fresh workspace (cluster) for one emitted plan — called once per plan, with no
     * exceptions. Calcite's per-cluster metadata cache is not thread-safe: {@code mq} is neither
     * volatile nor guarded and {@code getMetadataQuery()} is an unsynchronized check-then-act that
     * re-reads the field on its return path, so one thread's {@code invalidateMetadataQuery()} —
     * which the engine and {@code logPlan} both issue per plan — can land inside another's lazy
     * init and make the call return null. The cache is also the cycle-detection structure, so two
     * threads sharing it misread each other's in-progress markers as cycles
     * ({@code CyclicMetadataException}) and clear each other's entries. Plans the engine may plan
     * concurrently must therefore not share a cluster, nor any {@code RelNode}: a node is
     * permanently bound to the cluster it was created in, and metadata lookups follow
     * {@code node.getCluster()}.
     */
    private RelOptCluster newCluster() {
        return RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), new RexBuilder(typeFactory));
    }

    /** Builds one plan's private base — Scan → Filter — inside the context's own workspace. */
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
