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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
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
        if (fetch < 0) {
            assertNull("Sort without limit must have null fetch", ((OpenSearchSort) result).fetch);
        } else {
            assertNotNull("Sort with limit must have non-null fetch", ((OpenSearchSort) result).fetch);
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
     *  stays SINGLE (no split), no ER. Expect Sort → Agg → Filter → Scan. */
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
                        makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
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
                        makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
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
}
