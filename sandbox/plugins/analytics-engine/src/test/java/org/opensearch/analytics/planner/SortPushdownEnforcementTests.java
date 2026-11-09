/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.join.DistributionEnforcementPass;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.ArrayList;
import java.util.List;

/**
 * Interaction tests for {@link org.opensearch.analytics.planner.rules.OpenSearchSortPushdownRewriter}
 * and {@link DistributionEnforcementPass} on NON-aggregate top-N (`sort … | head N`).
 *
 * <p>These two run back-to-back in {@code DefaultPlanExecutor}: {@code PlannerImpl.createPlan} ends with
 * the sort-pushdown rewrite (a shard-local {@code Sort+fetch} below the ER, so each shard ships only its
 * local top-N), and then, when {@code analytics.mpp.enabled} is set, the enforcement pass walks that
 * output. {@code OpenSearchSort} is NOT {@link org.opensearch.analytics.planner.rel.DistributionAware},
 * so without the {@code perPartition} marker it would land in the pass's non-aware branch, which
 * re-gathers the child and thereby HOISTS the shard Sort above the ER — leaving the shard fragment a bare
 * scan that streams its entire scan to the coordinator. That was a measured ClickBench regression
 * (`sort … | head 10` over 10M rows: 51ms mpp-off vs 5391ms mpp-on); the rewriter now marks the
 * pushed-down Sort {@code perPartition} and the pass rides it on its child's distribution.
 *
 * <p>Coverage gap this closes: {@code SortPushdownPlanShapeTests} asserts the rewriter's output but never
 * runs the enforcement pass, and {@code CascadeShuffleProbeTests}'s only Sort case is a global sort ABOVE
 * an aggregate. Non-aggregate top-N under MPP was untested.
 */
public class SortPushdownEnforcementTests extends PlanShapeTestBase {

    /** Matches the shuffle partition count the general scheduler is given in the other MPP tests. */
    private static final int CLUSTER_DATA_NODES = 3;

    private RelNode collatedSortWithFetch(RelNode input, int fetch) {
        return LogicalSort.create(
            input,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    private RelNode bareLimit(RelNode input, int fetch) {
        return LogicalSort.create(
            input,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    private RelNode enforce(RelNode plan, PlannerContext context) {
        return DistributionEnforcementPass.enforce(
            plan,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
    }

    private static List<OpenSearchSort> sorts(RelNode plan) {
        List<OpenSearchSort> found = new ArrayList<>();
        collectSorts(plan, found);
        return found;
    }

    private static void collectSorts(RelNode node, List<OpenSearchSort> out) {
        if (node instanceof OpenSearchSort sort) {
            out.add(sort);
        }
        for (RelNode input : node.getInputs()) {
            collectSorts(input, out);
        }
    }

    /**
     * Counting Sorts is NOT enough — the regression kept both Sorts but HOISTED the shard-local one above
     * the gather, leaving the shard fragment a bare scan. What matters is that a Sort remains strictly
     * BELOW the ExchangeReducer, i.e. inside the shard fragment.
     */
    private static boolean hasSortBelowReducer(RelNode plan) {
        if (plan instanceof org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer reducer) {
            return sorts(reducer.getInput(0)).isEmpty() == false;
        }
        for (RelNode input : plan.getInputs()) {
            if (hasSortBelowReducer(input)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code sort field | head 10} over 2 shards: the rewriter pushes a shard-local Sort below the ER, and
     * the enforcement pass must LEAVE IT THERE. If the pass hoists it above the gather, the shard fragment
     * degenerates to a bare scan that ships every row (51ms → 5391ms on 10M ClickBench rows).
     */
    public void testCollatedTopN_shardSortStaysBelowGather() {
        PlannerContext context = multiShardContext();
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode planned = runPlanner(collatedSortWithFetch(scan, 10), context);
        assertEquals("rewriter pushed a shard-local Sort below the ER", 2, sorts(planned).size());
        assertTrue("precondition: the rewriter's Sort is below the ER", hasSortBelowReducer(planned));

        RelNode enforced = enforce(planned, context);
        assertEquals(
            "the pushed-down shard Sort must survive the enforcement pass:\n" + RelOptUtil.toString(enforced),
            2,
            sorts(enforced).size()
        );
        assertTrue(
            "the shard Sort must remain BELOW the gather — hoisting it above the ER leaves the shard "
                + "fragment a bare scan streaming every row:\n"
                + RelOptUtil.toString(enforced),
            hasSortBelowReducer(enforced)
        );
    }

    /**
     * The surviving shard-local Sort must keep its {@code fetch}. A Sort that loses its limit still sorts
     * every shard row and ships all of them — the same regression with the collation intact.
     */
    public void testCollatedTopN_shardSortKeepsFetch() {
        PlannerContext context = multiShardContext();
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode enforced = enforce(runPlanner(collatedSortWithFetch(scan, 10), context), context);
        assertTrue(
            "at least one Sort must still carry fetch=10 below the gather:\n" + RelOptUtil.toString(enforced),
            sorts(enforced).stream().anyMatch(s -> s.fetch != null)
        );
        assertEquals(
            "BOTH the shard-local and coordinator Sort carry the fetch:\n" + RelOptUtil.toString(enforced),
            2,
            sorts(enforced).stream().filter(s -> s.fetch != null).count()
        );
    }

    /**
     * Bare {@code head 10} (no ORDER BY) — the cheapest shape, and the one measured at 15ms mpp-off vs
     * 47ms mpp-on. The shard-local fetch must survive enforcement too.
     */
    public void testBareLimit_shardFetchStaysBelowGather() {
        PlannerContext context = multiShardContext();
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode planned = runPlanner(bareLimit(scan, 10), context);
        assertEquals("rewriter pushed a shard-local fetch below the ER", 2, sorts(planned).size());

        RelNode enforced = enforce(planned, context);
        assertEquals(
            "the pushed-down shard fetch must survive the enforcement pass:\n" + RelOptUtil.toString(enforced),
            2,
            sorts(enforced).size()
        );
        assertTrue("the shard-local fetch must remain BELOW the gather:\n" + RelOptUtil.toString(enforced), hasSortBelowReducer(enforced));
    }

    /**
     * The enforcement pass must not multiply gathers either: a single-scan top-N needs exactly ONE
     * ExchangeReducer. A second gather would add a redundant coordinator round-trip.
     */
    public void testCollatedTopN_singleGather() {
        PlannerContext context = multiShardContext();
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode enforced = enforce(runPlanner(collatedSortWithFetch(scan, 10), context), context);
        long reducers = RelOptUtil.toString(enforced).lines().filter(l -> l.contains("OpenSearchExchangeReducer")).count();
        assertEquals("exactly one gather for a single-scan top-N:\n" + RelOptUtil.toString(enforced), 1L, reducers);
    }
}
