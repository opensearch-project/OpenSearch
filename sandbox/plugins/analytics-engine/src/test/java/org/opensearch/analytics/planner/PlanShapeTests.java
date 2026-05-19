/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Combinations file. Per-operator plan shapes live in dedicated files:
 * {@link ScanPlanShapeTests}, {@link FilterPlanShapeTests}, {@link ProjectPlanShapeTests},
 * {@link SortPlanShapeTests}, {@link AggregatePlanShapeTests},
 * {@link JoinPlanShapeTests}, {@link UnionPlanShapeTests}.
 *
 * <p>This file covers <em>multi-operator pipelines</em> — the interesting interactions
 * (filter+stats+sort, join over aggregates, etc.) where trait propagation across
 * operator boundaries matters most.
 */
public class PlanShapeTests extends PlanShapeTestBase {

    /**
     * PPL: {@code | stats count() as cnt by k | sort cnt | head 2 | fields k, cnt}
     *
     * <p>The PPL frontend emits a redundant outer Sort (no fetch, same collation as the inner)
     * plus an inner Sort with fetch above a column-swap Project above the Aggregate. With both
     * Sorts present, DataFusion's logical-plan optimizer eliminates the inner Sort as redundant
     * but keeps the Limit, then physical planning pushes the Limit BELOW the SortExec into a
     * {@code CoalescePartitionsExec(fetch=N)} on the FINAL Aggregate output. Result: fetch is
     * applied to the unsorted Aggregate output, and Sort runs on the wrong N rows.
     *
     * <p>{@link org.opensearch.analytics.planner.rules.OpenSearchSortRule} drops the redundant
     * outer Sort during HEP marking. After-CBO must contain at most one Sort over the FINAL
     * Aggregate, with that Sort carrying the fetch.
     */
    public void testSortHeadAfterStats_dropsRedundantOuterSort() {
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ true);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Without the redundant outer Sort the planner must NOT drop the inner Sort+fetch — the
     * fetch is the only thing preserving top-K semantics.
     */
    public void testSortHeadAfterStats_singleSortFetchPreserved() {
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ false);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * If the outer Sort sorts by a DIFFERENT key than the inner Sort+fetch, the outer is NOT
     * redundant — dropping it would change result ordering. The rule's collation comparison
     * (after remapping through the Project) must reject the drop.
     */
    public void testSortHeadAfterStats_outerSortWithDifferentKeyKept() {
        // Inner sort by cnt ($0 below swap), outer sort by k ($0 above swap which maps to k).
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ true, /* outerSortField */ 0);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                      OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                            OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Each side of the join pre-aggregates with {@code count()} before the join. The planner
     * splits each Aggregate into PARTIAL + FINAL with a gather between them. The join's
     * {@code convert(input, SINGLETON)} is a no-op because each FINAL already delivers
     * SINGLETON — no extra ER wraps the FINAL on either side.
     *
     * <pre>
     * OpenSearchJoin(INNER, $0 = $2)           ← stamped SINGLETON by JoinSplitRule
     *   ├── OpenSearchAggregate(FINAL)
     *   │     └── OpenSearchExchangeReducer
     *   │           └── OpenSearchAggregate(PARTIAL)
     *   │                 └── OpenSearchTableScan(test_index)
     *   └── OpenSearchAggregate(FINAL)
     *         └── OpenSearchExchangeReducer
     *               └── OpenSearchAggregate(PARTIAL)
     *                     └── OpenSearchTableScan(test_index)
     * </pre>
     *
     * <p>Exactly two ERs — one between PARTIAL and FINAL on each branch. No top-level
     * redundant ER: the join delivers SINGLETON directly.
     */
    public void testJoinWithAggregate_erBetweenPartialAndFinal_noExtraErAboveFinal() {
        PlannerContext context = multiShardContext();

        RelNode leftAgg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode rightAgg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());

        // Equi-join on grouping key ($0 on each side). Left rowType is (status, cnt); right is
        // (status, cnt). After join the output has 4 columns; condition references left.$0 and
        // right.$0 (offset by left fieldCount=2).
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(leftAgg, rightAgg, List.of(), condition, Set.of(), JoinRelType.INNER);

        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── builders ───────────────────────────────────────────────────────────────

    private RelNode topKAfterStats(boolean withRedundantOuterSort) {
        return topKAfterStats(withRedundantOuterSort, /* outerSortField */ 1);
    }

    /**
     * Builds the Calcite tree the PPL frontend emits for
     * {@code | stats count() as cnt by k | sort cnt | head 2 | fields k, cnt}:
     *
     * <pre>
     * LogicalSort(sort0=$outerSortField)?              -- outer "sort cnt", optional
     *   LogicalProject(k=$1, cnt=$0)                   -- "fields k, cnt" (swap)
     *     LogicalSort(sort0=$0, fetch=2)               -- "sort cnt | head 2"
     *       LogicalAggregate(group=[{0}], cnt=COUNT()) -- "stats count() by k"
     *         StubTableScan
     * </pre>
     */
    private RelNode topKAfterStats(boolean withRedundantOuterSort, int outerSortField) {
        // Aggregate: group by column 0 (k), count as column 1 (cnt). Output: (k, cnt).
        AggregateCall countCall = countStarCall();
        RelNode agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countCall);

        // Inner Sort+fetch: sort by cnt ($1 in agg's output: column 1).
        // Wait — agg's output is (k=$0, cnt=$1). We want to sort by cnt, so sort field = 1.
        // But the BasePlannerRulesTests' makeSort hardcodes field 0. Use a custom builder.
        RelNode innerSort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        // Project that swaps to (k=$1 from cnt-after-sort wait no — keep original schema).
        // Match PPL output: (cnt=cnt, k=k) reorder. Actually PPL's "fields k, cnt" produces output
        // (k, cnt). Inner sort's output is still (k=$0, cnt=$1) because Sort doesn't change schema.
        // After Project (k=$0, cnt=$1) — identity. But the real PPL plan has a SWAP — the inner sort
        // in real PPL sees (cnt=$0, k=$1) due to a prior swap. Mimic that by adding swaps both sides.
        //
        // Simpler: just use the swap that mirrors the After-CBO plan we observed:
        // Project(k=$1, cnt=$0) over an input whose output is (cnt=$0, k=$1).
        // To produce that, add a swap project BELOW the inner sort too.
        RelNode innerSwap = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 0)),
            List.of("cnt", "k")
        );
        RelNode innerSortOverSwap = LogicalSort.create(
            innerSwap,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode outerSwap = LogicalProject.create(
            innerSortOverSwap,
            List.of(),
            List.of(rexBuilder.makeInputRef(innerSortOverSwap, 1), rexBuilder.makeInputRef(innerSortOverSwap, 0)),
            List.of("k", "cnt")
        );

        if (!withRedundantOuterSort) {
            return outerSwap;
        }

        // Outer Sort: collation field = `outerSortField` ($1 = cnt for redundant, $0 = k for non-redundant).
        return LogicalSort.create(
            outerSwap,
            RelCollations.of(new RelFieldCollation(outerSortField, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
    }

    // ── Multi-operator pipelines below ────────────────────────────────────

    // testLimitAfterScan_multiShard_noForceSingleton + _singleShard_noTopER removed —
    // covered by SortPlanShapeTests.testPureLimit_2shard / _1shard.

    /**
     * PPL: {@code source=t | sort score | fields name, score | head 3}
     *
     * <p>Inner collated Sort (by score) forces {@code Sort ← ER ← Scan} via
     * {@link org.opensearch.analytics.planner.rules.OpenSearchSortSplitRule} — concat gather
     * can't preserve global order. A narrowing Project over that Sort preserves the SINGLETON
     * input (project is single-input, passthrough-marked, no ER required). Outer pure-LIMIT
     * Sort (no collation) sits at the top; its input is already SINGLETON, so no additional ER.
     */
    public void testSortThenProjectThenLimit_multiShard() {
        Map<String, Map<String, Object>> fields = Map.of("name", Map.of("type", "keyword"), "score", Map.of("type", "integer"));
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "score" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        RelNode scan = stubScan(table);

        // Inner Sort: ORDER BY score (field 1).
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );

        // Project: name ($0), score ($1) — identity here but exercises the OpenSearchProjectRule
        // path (single-input passthrough) between the inner Sort and the outer LIMIT.
        RelNode project = LogicalProject.create(
            innerSort,
            List.of(),
            List.of(rexBuilder.makeInputRef(innerSort, 0), rexBuilder.makeInputRef(innerSort, 1)),
            List.of("name", "score")
        );

        // Outer LIMIT (fetch=3, no collation).
        RelNode limit = LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(3, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        RelNode result = runPlanner(limit, buildContext("parquet", 3, fields));
        assertPlanShape(
            """
                OpenSearchSort(fetch=[3], viableBackends=[[mock-parquet]])
                  OpenSearchProject(name=[$0], score=[$1], viableBackends=[[mock-parquet]])
                    OpenSearchSort(sort0=[$1], dir0=[ASC], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // testPureScan_multiShard_rootER + _singleShard_noTopER removed —
    // covered by ScanPlanShapeTests.testBareScan_2shard / _1shard.

    /**
     * PPL: {@code stats count() by status | inner join ... [source=u | stats count() by size]}
     * Two aggregates with different group keys, joined on some key. Both FINALs deliver SINGLETON;
     * JoinSplit's convert() is a no-op per side; no extra ER above either FINAL. Total ERs: 2
     * (one per branch's PARTIAL→FINAL).
     */
    public void testJoinWithDifferentGroupKeys_multiShard() {
        RelNode plan = buildJoinWithDifferentGroupKeys();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], s=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], s=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{1}], s=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $0)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{1}], s=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $0)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testJoinWithDifferentGroupKeys_singleShard() {
        // Both sides scan the same 1-shard index → co-location fast path even though the
        // group keys differ. Aggregates run at the shard, Join runs there. Locality-agnostic
        // root demand is satisfied by the SHARD+SINGLETON output of the Join — no top ER.
        RelNode plan = buildJoinWithDifferentGroupKeys();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], s=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{1}], s=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $0)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // testJoinMixedShards_* removed — covered by JoinPlanShapeTests
    // (testInnerJoin_mixedShards / _leftMulti_rightSingle / _differentTables_1shard).

    // testUnion_twoArmScans_* removed — covered by UnionPlanShapeTests
    // (testUnion_sameTable_2shard, testUnion_sameTable_1shard).

    /**
     * Each arm has stats — arm-level aggregate split produces FINAL at EXECUTION(SINGLETON).
     * The Union-arm ER that OpenSearchUnionRule wrapped over the marked input gets deduped
     * by Volcano because FINAL already delivers EXECUTION(SINGLETON) — the ConverterImpl
     * ER lands in the same RelSet as the FINAL and is redundant. Each Union input is the
     * FINAL directly.
     */
    public void testUnion_twoArmsWithStats_multiShard() {
        RelNode union = buildUnionOfTwoStatsArms("test_index");
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 3));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_twoArmsWithStats_singleShard() {
        // Both arms scan the same 1-shard index → co-location fast path. Aggregates run at
        // the shard (no PARTIAL/FINAL split needed at 1 shard), Union runs there. Root demand
        // (locality-agnostic SINGLETON) is satisfied directly — no top ER.
        RelNode union = buildUnionOfTwoStatsArms("test_index");
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 1));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // testUnion_twoArmsDifferentIndices_* removed — covered by UnionPlanShapeTests
    // (testUnion_differentTables_1shard / _2shard).

    // ── Downstream-of-Join / Union combinations ──────────────────────────────

    /**
     * Join → Aggregate (multi-shard, different tables). Common PPL shape:
     * {@code source=a | inner join b ON ... | stats count() by k}.
     * Each Join input is gathered via per-side ER, the Join runs at coord, then a SINGLE
     * aggregate runs above it (no PARTIAL/FINAL split — the Join's output is already
     * coord-local SINGLETON, no shuffle to split across).
     */
    public void testJoinThenAggregate_2shard() {
        RelNode plan = org.apache.calcite.rel.logical.LogicalAggregate.create(
            buildJoinOfTwoScans("left_idx", "right_idx"),
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(countStarCall())
        );
        RelNode result = runPlanner(plan, perIndexContext(Map.of("left_idx", 2, "right_idx", 2)));
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Join → Sort (multi-shard, different tables). PPL: {@code | join b | sort x}.
     * Join gathers each side to coord; Sort over the Join output stays at coord (Join
     * already delivers SINGLETON). No extra ER between Join and Sort.
     */
    public void testJoinThenSort_2shard() {
        RelNode join = buildJoinOfTwoScans("left_idx", "right_idx");
        RelNode plan = LogicalSort.create(
            join,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, perIndexContext(Map.of("left_idx", 2, "right_idx", 2)));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Union → Sort (multi-shard, same table). Union gathers each arm to coord; Sort runs
     * at coord over the unioned result. No extra ER between Union and Sort.
     */
    public void testUnionThenSort_2shard() {
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("test_index", "status", "size")), stubScan(mockTable("test_index", "status", "size"))),
            /* all */ true
        );
        RelNode plan = LogicalSort.create(
            union,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, unionContextSingleIndex("test_index", 2));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Chained inner join: {@code (a ⨝ b) ⨝ c}. Nested join: each leaf Join input has its
     * own per-side ER, the outer Join also has per-side ERs. Verifies trait propagation
     * through a nested Join doesn't degenerate (e.g. duplicate ERs) and that all three
     * inputs end up gathered exactly once.
     */
    public void testChainedJoin_2shard() {
        RelOptTable a = mockTable("a", "status", "size");
        RelOptTable b = mockTable("b", "status", "size");
        RelOptTable c = mockTable("c", "status", "size");
        RelNode aScan = stubScan(a);
        RelNode bScan = stubScan(b);
        RelNode cScan = stubScan(c);
        RexNode ab = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode abJoin = LogicalJoin.create(aScan, bScan, List.of(), ab, Set.of(), JoinRelType.INNER);
        RexNode abc = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 4)
        );
        RelNode plan = LogicalJoin.create(abJoin, cScan, List.of(), abc, Set.of(), JoinRelType.INNER);
        RelNode result = runPlanner(plan, perIndexContext(Map.of("a", 2, "b", 2, "c", 2)));
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $4)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[a]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[b]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[c]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── Union builders / contexts ─────────────────────────────────────────────

    private RelNode buildUnionOfTwoStatsArms(String table) {
        RelNode arm1 = org.apache.calcite.rel.logical.LogicalAggregate.create(
            stubScan(mockTable(table, "status", "size")),
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(countStarCall())
        );
        RelNode arm2 = org.apache.calcite.rel.logical.LogicalAggregate.create(
            stubScan(mockTable(table, "status", "size")),
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(countStarCall())
        );
        return LogicalUnion.create(List.of(arm1, arm2), /* all */ true);
    }

    /**
     * Planner context with UNION engine capability declared, for a single index.
     */
    private PlannerContext unionContextSingleIndex(String indexName, int shardCount) {
        return buildContextPerIndex("parquet", Map.of(indexName, shardCount), intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    /**
     * MockDataFusionBackend with EngineCapability.UNION declared.
     */
    private static final class UnionCapableBackend extends MockDataFusionBackend {
        @Override
        protected Set<org.opensearch.analytics.spi.EngineCapability> supportedEngineCapabilities() {
            Set<org.opensearch.analytics.spi.EngineCapability> caps = new java.util.HashSet<>(super.supportedEngineCapabilities());
            caps.add(org.opensearch.analytics.spi.EngineCapability.UNION);
            return caps;
        }
    }

    // ── Logical-plan builders ────────────────────────────────────────────────

    /** Inner equi-join {@code left.$0 = right.$0} on two bare scans. */
    private RelNode buildJoinOfTwoScans(String leftTable, String rightTable) {
        RelNode leftScan = stubScan(mockTable(leftTable, "status", "size"));
        RelNode rightScan = stubScan(mockTable(rightTable, "status", "size"));
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(leftScan, rightScan, List.of(), cond, Set.of(), JoinRelType.INNER);
    }

    private RelNode buildJoinWithDifferentGroupKeys() {
        RelOptTable table = mockTable("test_index", "status", "size");
        // Left: stats sum(size) by status (group=0, aggregation output INTEGER)
        RelNode leftScan = stubScan(table);
        RelNode leftAgg = org.apache.calcite.rel.logical.LogicalAggregate.create(
            leftScan,
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(sumCallOn(leftScan, /* sumField */ 1))
        );
        // Right: stats sum(status) by size (group=1, aggregation output INTEGER — different group key)
        RelNode rightScan = stubScan(table);
        RelNode rightAgg = org.apache.calcite.rel.logical.LogicalAggregate.create(
            rightScan,
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(1),
            null,
            List.of(sumCallOn(rightScan, /* sumField */ 0))
        );
        // Join on left.$0 (status, INTEGER) == right.$0 (size, INTEGER).
        // Arbitrary equi-condition; we only care about plan shape, not join semantics.
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(leftAgg, rightAgg, List.of(), cond, Set.of(), JoinRelType.INNER);
    }

    private AggregateCall sumCallOn(RelNode input, int sumField) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(sumField),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "s"
        );
    }

    // Plan-shape and structural-pipeline assertions are inherited from PlanShapeTestBase.
}
