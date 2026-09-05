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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SearchSourceConverterTests extends OpenSearchTestCase {

    /** Invalidate/get rounds each plan's thread runs against its own cluster. */
    private static final int METADATA_ROUNDS = 200;

    /** Bound on the start barrier so a stuck worker fails the test instead of hanging it. */
    private static final int BARRIER_TIMEOUT_SECONDS = 30;

    /** Bound on joining each worker. */
    private static final int JOIN_TIMEOUT_SECONDS = 60;

    private SchemaPlus schema;
    private SearchSourceConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                // Nullable fields — matches OpenSearchSchemaBuilder behavior
                return typeFactory.builder()
                    .add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("brand", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("rating", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true))
                    .build();
            }
        });
        // Same fields plus a date one, so the date-math tests can put a `now`-relative range in the
        // query clause without changing the row type every other test asserts on.
        schema.add("date-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("brand", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("event_time", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), true))
                    .build();
            }
        });
        converter = new SearchSourceConverter(schema);
    }

    public void testConvertProducesHitsPlan() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        // hits plan plus the COUNT plan that supplies hits.total
        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.COUNT));

        // Even a default request carries fetch=size (10) so the limit pushes down to the engine.
        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertTrue(plan.relNode() instanceof LogicalSort);
        LogicalSort sort = (LogicalSort) plan.relNode();
        assertNotNull(sort.fetch);
        assertTrue(sort.getInput() instanceof LogicalTableScan);
    }

    public void testConvertResolvesFieldNames() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertEquals(4, plan.relNode().getRowType().getFieldCount());
        assertEquals(List.of("name", "price", "brand", "rating"), plan.relNode().getRowType().getFieldNames());
    }

    public void testConvertThrowsForMissingIndex() {
        expectThrows(IllegalArgumentException.class, () -> converter.convert(new SearchSourceBuilder(), "nonexistent-index"));
    }

    public void testAggsWithSizeZeroProducesAggregationAndCountPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(2, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testAggsWithSizeGreaterThanZeroProducesAllPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(10).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(3, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testNoAggsProducesHitsAndCountPlans() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testSizeZeroNoAggsProducesOnlyCountPlan() throws ConversionException {
        // size=0 with no aggs is the count-only query: hits.total comes from the COUNT plan
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(1, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testTrackTotalHitsDisabledSkipsCountPlan() throws ConversionException {
        // no eligible counts needed and totals explicitly not tracked → nothing to count
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false);
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.COUNT));
    }

    public void testAggPlanIncludesPostAggSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.key(true))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
        QueryPlans plans = converter.convert(source, "test-index");

        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        // Aggregation plan should be wrapped with LogicalSort for bucket order
        assertTrue(plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode() instanceof LogicalSort);
    }

    public void testMetricOnlyAggPlanHasNoPostAggSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        // Metric-only agg has no bucket orders, so no LogicalSort wrapper
        assertFalse(plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode() instanceof LogicalSort);
    }

    // ---- Top-K pushdown plan shapes ----

    public void testTermsSizeBecomesFetchOnAggregationSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").size(7));
        QueryPlans plans = converter.convert(source, "test-index");

        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("sort must carry the fetch: " + plan, plan.contains("fetch=[7]"));
    }

    public void testTermsDefaultNullExclusionBecomesPreAggFilter() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"));
        QueryPlans plans = converter.convert(source, "test-index");

        // brand is column 2 in the test schema
        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("null keys must be excluded below the aggregate: " + plan, plan.contains("LogicalFilter(condition=[IS NOT NULL($2)])"));
    }

    public void testMinDocCountBecomesHavingWithFetch() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5));
        QueryPlans plans = converter.convert(source, "test-index");

        // post-agg schema is [brand, _count] — the HAVING filter references _count at $1,
        // and the LIMIT rides the sort above it (filter-before-truncate)
        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("min_doc_count must become a HAVING filter: " + plan, plan.contains(">=($1, 5)"));
        assertTrue("the plan stays bounded: " + plan, plan.contains("fetch=[10]"));
    }

    public void testMinDocCountGetsOwnEligibleCountPlan() throws ConversionException {
        // The eligible count must exclude below-threshold groups, which COUNT(field)
        // cannot see — the aggregation gets its own HAVING-filtered SUM plan.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5));
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertEquals(2, countPlans.size());
        // flat plan: hits.total only — the eligible count moved to the dedicated plan
        QueryPlans.QueryPlan flat = countPlanWithColumn(countPlans, QueryPlans.COUNT_TOTAL_COLUMN);
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN), flat.relNode().getRowType().getFieldNames());
        // dedicated plan: SUM over the HAVING-filtered per-group counts
        QueryPlans.QueryPlan eligibleCountPlan = countPlanWithColumn(countPlans, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand");
        assertEquals(
            List.of(QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand"),
            eligibleCountPlan.relNode().getRowType().getFieldNames()
        );
        String plan = eligibleCountPlan.relNode().explain();
        assertTrue("eligible count must be HAVING-filtered: " + plan, plan.contains(">=($1, 5)"));
        assertTrue("eligible count must sum the surviving group counts: " + plan, plan.contains("SUM($1)"));
    }

    public void testMissingWithMinDocCountGetsOwnEligibleCountPlan() throws ConversionException {
        // The eligible count must exclude below-threshold groups, which COUNT(field)
        // cannot see — the aggregation gets its own HAVING-filtered SUM plan.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5).missing("N/A"));
        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertEquals(2, countPlans.size());
        // flat plan: hits.total only — the eligible count must not ride COUNT(*)
        QueryPlans.QueryPlan flat = countPlanWithColumn(countPlans, QueryPlans.COUNT_TOTAL_COLUMN);
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN), flat.relNode().getRowType().getFieldNames());
        QueryPlans.QueryPlan eligibleCountPlan = countPlanWithColumn(countPlans, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand");
        assertEquals(
            List.of(QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand"),
            eligibleCountPlan.relNode().getRowType().getFieldNames()
        );
        String plan = eligibleCountPlan.relNode().explain();
        assertTrue("eligible count must be HAVING-filtered: " + plan, plan.contains(">=($1, 5)"));
        assertTrue("eligible count must carry the missing substitution: " + plan, plan.contains("CASE(IS NOT NULL($2)"));
        assertTrue("eligible count must sum the surviving group counts: " + plan, plan.contains("SUM($1)"));
    }

    /** Finds the COUNT plan carrying the given output column — plan order is not part of the contract. */
    private static QueryPlans.QueryPlan countPlanWithColumn(List<QueryPlans.QueryPlan> countPlans, String column) {
        return countPlans.stream()
            .filter(p -> p.relNode().getRowType().getFieldNames().contains(column))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no COUNT plan with column [" + column + "]"));
    }

    public void testSameFieldSiblingsProduceTwoBoundedPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("top_brands").field("brand"))
            .aggregation(new TermsAggregationBuilder("brands_by_key").field("brand").order(BucketOrder.key(true)));
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());
        for (QueryPlans.QueryPlan plan : aggPlans) {
            assertTrue("each sibling's plan is bounded: " + plan.relNode().explain(), plan.relNode().explain().contains("fetch=[10]"));
        }
        // one eligible-count column per sibling, keyed by aggregation name
        List<String> countColumns = plans.get(QueryPlans.Type.COUNT).get(0).relNode().getRowType().getFieldNames();
        assertEquals(
            List.of(
                QueryPlans.COUNT_TOTAL_COLUMN,
                QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "top_brands",
                QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "brands_by_key"
            ),
            countColumns
        );
    }

    public void testMissingBecomesCaseProjectionWithoutNullFilter() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").missing("unknown"));
        QueryPlans plans = converter.convert(source, "test-index");

        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("missing must substitute via CASE: " + plan, plan.contains("CASE(IS NOT NULL($2)"));
        assertFalse("missing fields keep their null keys: " + plan, plan.contains("LogicalFilter(condition=[IS NOT NULL($2)])"));
    }

    public void testCountPlanCarriesTotalAndEligibleColumns() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"));
        QueryPlans plans = converter.convert(source, "test-index");

        List<String> columns = plans.get(QueryPlans.Type.COUNT).get(0).relNode().getRowType().getFieldNames();
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand"), columns);
    }

    public void testCountPlanWithMissingCarriesOnlyTotal() throws ConversionException {
        // missing substitution makes every matching doc eligible — COUNT(*) is the eligible count
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").missing("unknown"));
        QueryPlans plans = converter.convert(source, "test-index");

        List<String> columns = plans.get(QueryPlans.Type.COUNT).get(0).relNode().getRowType().getFieldNames();
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN), columns);
    }

    public void testNestedTermsPlanIsBoundedPerParent() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .size(2)
                    .subAggregation(new TermsAggregationBuilder("by_name").field("name").size(3))
            );
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());

        String parentPlan = aggPlans.get(0).relNode().explain();
        assertTrue("parent is a flat top-N: " + parentPlan, parentPlan.contains("fetch=[2]"));

        QueryPlans.QueryPlan child = aggPlans.get(1);
        String childPlan = child.relNode().explain();
        // restricted to the parent plan's winners
        assertTrue("child must semi-join the parent plan: " + childPlan, childPlan.contains("joinType=[semi]"));
        // bounded per parent, ordered by the child's bucket order inside the window
        assertTrue("child must rank within the parent partition: " + childPlan, childPlan.contains("ROW_NUMBER() OVER (PARTITION BY"));
        assertTrue("rank filter must keep the per-parent top K: " + childPlan, childPlan.contains("<=($"));
        // the eligible count rides the rows
        assertTrue("per-parent eligible total must ride the rows: " + childPlan, childPlan.contains("SUM($"));
        assertTrue(
            "child schema carries the parent-eligible column",
            child.relNode().getRowType().getFieldNames().contains("_parent_eligible")
        );
        // The child's own bound is the window rank, not a flat LIMIT: its size (3) must never
        // appear as a fetch. (The parent's fetch=[2] legitimately appears inside the child
        // plan — the parent top-N subtree is the semi-join input.)
        assertFalse("child size must not be a flat fetch: " + childPlan, childPlan.contains("fetch=[3]"));
        assertNull(child.aggregationMetadata().getFetch());
    }

    public void testUnsupportedTermsParameterRejectedEndToEnd() {
        TermsAggregationBuilder terms = new TermsAggregationBuilder("by_brand").field("brand");
        terms.includeExclude(new org.opensearch.search.aggregations.bucket.terms.IncludeExclude("Brand.*", null));
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(terms);

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertTrue(e.getMessage().contains("include"));
    }

    // ---- Per-plan planning isolation ----

    public void testEveryEmittedPlanGetsItsOwnCluster() throws ConversionException {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        // Non-vacuity: the shape has to actually emit several plans, or "all distinct" is trivial.
        assertEquals("expected 1 HITS + 2 AGGREGATION + 1 COUNT: " + planTypes(all), 4, all.size());

        Set<RelOptCluster> clusters = Collections.newSetFromMap(new IdentityHashMap<>());
        for (QueryPlans.QueryPlan plan : all) {
            clusters.add(plan.relNode().getCluster());
        }
        assertEquals("one RelOptCluster per emitted plan, but " + planTypes(all) + " used " + clusters.size(), all.size(), clusters.size());
    }

    public void testEmittedPlansShareNoRelNodes() throws ConversionException {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals("expected 1 HITS + 2 AGGREGATION + 1 COUNT: " + planTypes(all), 4, all.size());

        List<Set<RelNode>> nodesPerPlan = all.stream().map(plan -> collectNodes(plan.relNode())).collect(Collectors.toList());
        for (Set<RelNode> nodes : nodesPerPlan) {
            // The query clause is a term query, so every plan's base is Scan → Filter. A shared
            // base would show up as a shared LogicalFilter, so the walk has to reach one — without
            // this the disjointness assertion could hold over trees that never include the base.
            assertTrue("plan has no LogicalFilter to share", nodes.stream().anyMatch(n -> n instanceof LogicalFilter));
        }

        for (int i = 0; i < nodesPerPlan.size(); i++) {
            for (int j = i + 1; j < nodesPerPlan.size(); j++) {
                Set<RelNode> shared = Collections.newSetFromMap(new IdentityHashMap<>());
                shared.addAll(nodesPerPlan.get(i));
                shared.retainAll(nodesPerPlan.get(j));
                assertTrue("plans " + i + " and " + j + " share RelNodes: " + shared, shared.isEmpty());
            }
        }
    }

    public void testEveryNodeOfAPlanLivesInThatPlansCluster() throws ConversionException {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals("expected 1 HITS + 2 AGGREGATION + 1 COUNT: " + planTypes(all), 4, all.size());

        for (QueryPlans.QueryPlan plan : all) {
            RelOptCluster planCluster = plan.relNode().getCluster();
            Set<RelNode> nodes = collectNodes(plan.relNode());
            assertTrue("plan " + plan.type() + " has a single node only", nodes.size() > 1);
            for (RelNode node : nodes) {
                // A cross-cluster subtree would leave this plan's metadata lookups on a sibling
                // plan's cluster, which another thread is concurrently invalidating — the same race
                assertSame(
                    "plan " + plan.type() + " reaches a " + node.getRelTypeName() + " in another plan's cluster",
                    planCluster,
                    node.getCluster()
                );
            }
        }
    }

    public void testNestedChildRebuildsTheParentLevelInItsOwnCluster() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .query(QueryBuilders.termQuery("brand", "acme"))
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .size(2)
                    .subAggregation(new TermsAggregationBuilder("by_name").field("name").size(3))
            );
        List<QueryPlans.QueryPlan> aggPlans = converter.convert(source, "test-index").get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());

        RelNode parent = aggPlans.get(0).relNode();
        RelNode child = aggPlans.get(1).relNode();
        RelNode semiJoinRight = semiJoinRightInputOf(child);

        // The parent level is the child's semi-join input. Reusing the emitted parent plan's own
        // node would put two dispatched plans on one subtree; rebuilding it in the child's cluster
        assertNotSame("the child must not reuse the emitted parent plan's nodes", parent, semiJoinRight);
        assertNotSame("the two plans must not share a cluster", parent.getCluster(), child.getCluster());
        assertSame("the rebuilt parent level must live in the child's cluster", child.getCluster(), semiJoinRight.getCluster());
        assertEquals("the rebuilt parent level must be identical to the emitted parent plan", parent.explain(), semiJoinRight.explain());
        assertEquals(parent.getRowType(), semiJoinRight.getRowType());
    }

    public void testAllPlansShareOneRelOptTable() throws ConversionException {
        // The deliberately shared half: catalogReader.getTable runs once per request, so every plan
        // scans the same RelOptTable instance — one interned row type, hence field indices that
        // agree between a plan and a rebuilt sibling level. Read back out of the emitted RelNodes,
        // so resolving a table per plan (each getTable builds a fresh RelOptTableImpl) fails this.
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals("expected 1 HITS + 2 AGGREGATION + 1 COUNT: " + planTypes(all), 4, all.size());

        Set<RelOptTable> tables = Collections.newSetFromMap(new IdentityHashMap<>());
        for (QueryPlans.QueryPlan plan : all) {
            Set<RelOptTable> planTables = scanTablesOf(plan.relNode());
            // Every plan must reach a scan, or "one table everywhere" would hold over nothing.
            assertFalse("plan " + plan.type() + " has no TableScan", planTables.isEmpty());
            tables.addAll(planTables);
        }
        assertEquals("one RelOptTable identity across all plans, but got " + tables, 1, tables.size());
    }

    /**
     * Replays, on all plans of one request at once, the pair of calls made per plan on the thread
     * that dispatches it — set {@code THREAD_PROVIDERS}, then invalidate that plan's metadata query
     * (see {@code DslQueryPlanExecutor#logPlan}, and the engine's own executor). Without per-plan
     * isolation those plans share one {@code RelOptCluster} whose {@code mq} field is neither
     */
    public void testConcurrentMetadataAccessAcrossPlansIsIsolated() throws Exception {
        List<QueryPlans.QueryPlan> all = converter.convert(nestedThreeLevelSource(), "test-index").getAll();
        assertEquals("expected 1 HITS + 2 AGGREGATION + 1 COUNT: " + planTypes(all), 4, all.size());

        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        CyclicBarrier barrier = new CyclicBarrier(all.size());
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            RelNode relNode = all.get(i).relNode();
            threads.add(new Thread(() -> {
                try {
                    // A thread that already carries a provider would mask a missing set() below and
                    // make the probe vacuous.
                    RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                    RelOptCluster cluster = relNode.getCluster();
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    for (int round = 0; round < METADATA_ROUNDS; round++) {
                        RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(cluster.getMetadataProvider()));
                        cluster.invalidateMetadataQuery();
                        RelMdUtil.clearCache(relNode);
                        assertNotNull(cluster.getMetadataQuery().getRowCount(relNode));
                    }
                } catch (Throwable t) {
                    errors.offer(t);
                } finally {
                    RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                }
            }, "dsl-plan-metadata-" + i));
        }

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(JOIN_TIMEOUT_SECONDS));
            assertFalse("worker did not finish: " + thread.getName(), thread.isAlive());
        }
        assertTrue(errors.toString(), errors.isEmpty());
    }

    // ---- Query validation on the zero-plan path ----

    public void testSizeZeroNoAggsWithInvalidQueryIsRejected() {
        // Translating the query clause is what validates it, and after per-plan isolation that only
        // happens inside a plan's own base. This shape still emits the flat COUNT plan, so its base
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).query(QueryBuilders.termQuery("nope", "x"));

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertTrue(e.getMessage(), e.getMessage().contains("nope"));
    }

    public void testZeroPlanRequestWithInvalidQueryIsRejectedNotAnsweredEmpty() {
        // size=0 + no aggregations + track_total_hits:false is the one request that emits no plan
        // at all — not even a COUNT plan — so nothing translates its query unless convert() does so
        // deliberately. Without that, a malformed query is an empty 200 a caller cannot tell apart
        // from "no results" instead of the 400 this ConversionException becomes at the transport.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).trackTotalHits(false).query(QueryBuilders.termQuery("nope", "x"));

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertTrue(e.getMessage(), e.getMessage().contains("nope"));
    }

    public void testZeroPlanRequestWithValidQueryStillProducesNoPlans() throws ConversionException {
        // The guard above must not start rejecting legitimate empty requests: a resolvable query on
        // the zero-plan path is still a normal empty result, not a failure.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .trackTotalHits(false)
            .query(QueryBuilders.termQuery("brand", "acme"));

        assertEquals(0, converter.convert(source, "test-index").getAll().size());
    }

    /** 3-level nesting with hits: emits HITS, AGGREGATION(by_brand), AGGREGATION(by_brand,by_name), COUNT. */
    private static SearchSourceBuilder nestedThreeLevelSource() {
        return new SearchSourceBuilder().size(10)
            .query(QueryBuilders.termQuery("brand", "acme"))
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new TermsAggregationBuilder("by_name").field("name")
                            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    )
            );
    }

    /** Plan types in emit order, for assertion messages. */
    private static String planTypes(List<QueryPlans.QueryPlan> plans) {
        return plans.stream().map(p -> p.type().name()).collect(Collectors.joining(","));
    }

    /** Collects a plan's RelNodes into an identity set. */
    private static Set<RelNode> collectNodes(RelNode root) {
        Set<RelNode> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<RelNode> pending = new ArrayDeque<>();
        pending.push(root);
        while (pending.isEmpty() == false) {
            RelNode current = pending.pop();
            if (nodes.add(current)) {
                current.getInputs().forEach(pending::push);
            }
        }
        return nodes;
    }

    /** Returns the identity set of tables scanned anywhere in a plan — all branches, not just input 0. */
    private static Set<RelOptTable> scanTablesOf(RelNode root) {
        Set<RelOptTable> tables = Collections.newSetFromMap(new IdentityHashMap<>());
        for (RelNode node : collectNodes(root)) {
            if (node instanceof TableScan) {
                tables.add(node.getTable());
            }
        }
        return tables;
    }

    /** Returns the right input of the plan's single semi-join — the parent level a nested plan bounds against. */
    private static RelNode semiJoinRightInputOf(RelNode root) {
        List<RelNode> rights = new ArrayList<>();
        for (RelNode node : collectNodes(root)) {
            if (node instanceof Join join && join.getJoinType() == JoinRelType.SEMI) {
                rights.add(join.getInput(1));
            }
        }
        assertEquals("expected exactly one semi-join in " + root.explain(), 1, rights.size());
        return rights.get(0);
    }

    // ---- Golden file driven RelNode generation tests ----

    /**
     * Auto-discovers all golden JSON files and validates that each inputDsl
     * produces the expected RelNode plan via SearchSourceConverter.convert().
     * Adding a new test case only requires adding a new JSON file — no new
     * Java method needed.
     */
    public void testGoldenFileRelNodeGeneration() throws Exception {
        URL goldenDir = getClass().getClassLoader().getResource("golden");
        assertNotNull("Golden file resource directory not found", goldenDir);

        List<Path> goldenFiles;
        try (var stream = Files.list(Path.of(goldenDir.toURI()))) {
            goldenFiles = stream.filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList());
        }
        assertFalse("No golden files found", goldenFiles.isEmpty());

        List<String> failures = new ArrayList<>();
        for (Path file : goldenFiles) {
            String fileName = file.getFileName().toString();
            try {
                GoldenTestCase tc = GoldenFileLoader.load(fileName);
                CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

                SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
                SearchSourceConverter conv = new SearchSourceConverter(infra.schema());
                QueryPlans plans = conv.convert(searchSource, tc.getIndexName());

                QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
                List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
                if (matchingPlans.isEmpty()) {
                    failures.add(fileName + ": No " + expectedType + " plan produced");
                    continue;
                }

                RelNode relNode = matchingPlans.get(0).relNode();
                String actualPlan = relNode.explain().trim();
                String expectedPlan = String.join("\n", tc.getExpectedRelNodePlan());

                if (!expectedPlan.equals(actualPlan)) {
                    failures.add(fileName + ": RelNode plan mismatch\n  Expected: " + expectedPlan + "\n  Actual:   " + actualPlan);
                }

                List<String> actualFields = relNode.getRowType().getFieldNames();
                if (!tc.getMockResultFieldNames().equals(actualFields)) {
                    failures.add(
                        fileName + ": Field names mismatch\n  Expected: " + tc.getMockResultFieldNames() + "\n  Actual:   " + actualFields
                    );
                }
            } catch (Exception e) {
                failures.add(fileName + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            fail("Golden file RelNode generation failures:\n" + String.join("\n", failures));
        }
    }

    public void testNanoTimestampTypeSystemReportsMaxPrecisionNine() {
        assertEquals(9, DslTypeSystems.NANO_TIMESTAMP.getMaxPrecision(SqlTypeName.TIMESTAMP));
    }

    private SearchSourceBuilder parseSearchSource(Map<String, Object> inputDsl) throws IOException {
        String json;
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(inputDsl);
            json = builder.toString();
        }
        NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents()
        );
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(registry, DeprecationHandler.IGNORE_DEPRECATIONS, json)) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }
}
