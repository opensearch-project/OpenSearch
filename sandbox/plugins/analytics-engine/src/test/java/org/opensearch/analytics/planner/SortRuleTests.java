/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rules.OpenSearchFilterRule;
import org.opensearch.analytics.planner.rules.OpenSearchSortRule;
import org.opensearch.analytics.planner.rules.OpenSearchTableScanRule;

import java.util.List;
import java.util.Set;

/**
 * Tests for sort rule: marking of Sort(Filter(Scan)) and Sort(Agg(Filter(Scan)))
 * with and without fetch (LIMIT), verifying viableBackends at each pipeline level.
 */
public class SortRuleTests extends BasePlannerRulesTests {

    private PlannerContext defaultContext() {
        return buildContext("parquet", 1, intFields());
    }

    private void assertSortPipeline(
        RelNode result,
        List<Class<? extends org.opensearch.analytics.planner.rel.OpenSearchRelNode>> types,
        int fetch
    ) {
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(result, types, Set.of(MockDataFusionBackend.NAME));
        // After QTF rewrite the wrapper sits at the root; the anchor Sort is its input.
        OpenSearchSort sort = (OpenSearchSort) (result instanceof OpenSearchLateMaterialization wrap ? wrap.getInput() : result);
        if (fetch < 0) {
            assertNull("Sort without limit must have null fetch", sort.fetch);
        } else {
            assertNotNull("Sort with limit must have non-null fetch", sort.fetch);
        }
    }

    /** Sort(Filter(Scan)) with and without fetch. */
    public void testSortOnFilteredScan() {
        RelNode filter = makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200));
        List<Class<? extends org.opensearch.analytics.planner.rel.OpenSearchRelNode>> types = List.of(
            OpenSearchSort.class,
            OpenSearchFilter.class,
            OpenSearchTableScan.class
        );

        assertSortPipeline(runPlanner(makeSort(filter, -1), defaultContext()), types, -1);
        assertSortPipeline(
            runPlanner(
                makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10),
                defaultContext()
            ),
            types,
            10
        );
    }

    /** The marked OpenSearchSort's trait set must carry the sort's collation. */
    public void testSortRuleStampsCollationOnTraitSet() {
        PlannerContext context = defaultContext();
        RelNode logicalSort = makeSort(
            makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
            -1
        );

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        programBuilder.addRuleCollection(
            List.of(new OpenSearchTableScanRule(context), new OpenSearchFilterRule(context), new OpenSearchSortRule(context))
        );
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
        hepPlanner.setRoot(logicalSort);
        RelNode marked = hepPlanner.findBestExp();

        assertTrue("expected OpenSearchSort root, got " + marked.getClass().getSimpleName(), marked instanceof OpenSearchSort);
        OpenSearchSort osSort = (OpenSearchSort) marked;
        RelCollation collation = osSort.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        assertNotNull("OpenSearchSort trait set must carry the sort's collation (rule must call .plus(sort.getCollation()))", collation);
        assertEquals("Trait set collation must match the sort's declared collation", osSort.getCollation(), collation);
    }

    /** Sort(Agg(Filter(Scan))) with and without fetch — full OLAP pipeline.
     *  Single-shard: SOURCE(SINGLETON) scan satisfies root RESULT(SINGLETON), aggregate
     *  stays SINGLE (no split), no ER. Expect Sort → Agg → Filter → Scan.
     *  The filter pins {@code size} (field 1, a non-group column) so the aggregate keeps an
     *  unbounded group count — otherwise filtering the group key {@code status} to a single
     *  value would bound it to one row and {@code SORT_REMOVE_REDUNDANT} would correctly drop
     *  the now-redundant Sort (covered by {@link #testSortFetchOnScalarAggregateDropped}). */
    public void testSortOnAggregateOnFilteredScan() {
        List<Class<? extends org.opensearch.analytics.planner.rel.OpenSearchRelNode>> types = List.of(
            OpenSearchSort.class,
            OpenSearchAggregate.class,
            OpenSearchFilter.class,
            OpenSearchTableScan.class
        );

        assertSortPipeline(
            runPlanner(
                makeSort(
                    makeAggregate(
                        makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(1, SqlTypeName.INTEGER, 200)),
                        sumCall()
                    ),
                    -1
                ),
                defaultContext()
            ),
            types,
            -1
        );

        assertSortPipeline(
            runPlanner(
                makeSort(
                    makeAggregate(
                        makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(1, SqlTypeName.INTEGER, 200)),
                        sumCall()
                    ),
                    10
                ),
                defaultContext()
            ),
            types,
            10
        );
    }

    private static final List<Class<? extends org.opensearch.analytics.planner.rel.OpenSearchRelNode>> SORT_AGG_SCAN = List.of(
        OpenSearchSort.class,
        OpenSearchAggregate.class,
        OpenSearchTableScan.class
    );

    /** Asserts the planned pipeline has NO OpenSearchSort root and bottoms out at a scalar Aggregate. */
    private void assertLimitDropped(RelNode result) {
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertFalse("limit must be dropped (no OpenSearchSort)", result instanceof OpenSearchSort);
        assertTrue("root must be the scalar OpenSearchAggregate", result instanceof OpenSearchAggregate);
        assertTrue("scalar aggregate has empty group set", ((OpenSearchAggregate) result).getGroupSet().isEmpty());
    }

    private RelNode scalarAgg() {
        return makeAggregate(
            stubScan(mockTable("test_index", "status", "size")),
            org.apache.calcite.util.ImmutableBitSet.of(),
            countStarCall()
        );
    }

    private RelNode groupedAgg() {
        // makeAggregate(input, aggCalls) defaults to group set {0} — a grouped aggregate.
        return makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
    }

    /** Grouped agg, head N: kept — the limit caps the number of groups (user-visible). */
    public void testHeadOnGroupedAggregatePreservesFetch() {
        assertSortPipeline(runPlanner(makeLimit(groupedAgg(), 5), defaultContext()), SORT_AGG_SCAN, 5);
    }

    /** Scalar agg, collation-less head N: dropped — LIMIT over one row is a no-op. */
    public void testHeadOnScalarAggregateDropsLimit() {
        assertLimitDropped(runPlanner(makeLimit(scalarAgg(), 5), defaultContext()));
    }

    /** Scalar agg, ORDER BY + fetch (non-empty collation): dropped — a 1-row relation is already
     *  sorted and a fetch >= 1 cannot remove its single row, so the whole Sort is redundant
     *  (Calcite SORT_REMOVE_REDUNDANT). */
    public void testSortFetchOnScalarAggregateDropped() {
        assertLimitDropped(runPlanner(makeSort(scalarAgg(), 5), defaultContext()));
    }

    /** Scalar agg, OFFSET (no collation): kept — OFFSET 1 over one row yields zero rows, so it is NOT a no-op. */
    public void testOffsetOnScalarAggregatePreserved() {
        RelNode offsetLimit = org.apache.calcite.rel.logical.LogicalSort.create(
            scalarAgg(),
            org.apache.calcite.rel.RelCollations.EMPTY,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(offsetLimit, defaultContext());
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("offset-bearing Sort must be preserved", result instanceof OpenSearchSort);
        assertNotNull("offset preserved", ((OpenSearchSort) result).offset);
    }

    /**
     * Stacked collation-less limits over a grouped aggregate — a user {@code head 5} above the
     * frontend's system size-cap {@code fetch=10000} — collapse to a SINGLE {@code OpenSearchSort}
     * carrying the tighter fetch (5), via {@code CoreRules.LIMIT_MERGE}. The grouped aggregate is
     * many-row so the limit is preserved (not dropped); the point here is one Sort, not two.
     */
    public void testStackedLimitsOverGroupedAggregateMergeToOne() {
        // head 5 over system-cap 10000 over `stats sum(x) by k`.
        RelNode input = makeLimit(makeLimit(groupedAgg(), 10000), 5);
        RelNode result = runPlanner(input, defaultContext());
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        assertTrue("root must be an OpenSearchSort", result instanceof OpenSearchSort);
        OpenSearchSort sort = (OpenSearchSort) result;
        assertEquals("merged fetch must be the tighter limit (5)", 5, RexLiteral.intValue(sort.fetch));
        assertFalse("no second stacked OpenSearchSort below", sort.getInput() instanceof OpenSearchSort);
        assertTrue("Sort sits directly over the Aggregate", sort.getInput() instanceof OpenSearchAggregate);
    }
}
