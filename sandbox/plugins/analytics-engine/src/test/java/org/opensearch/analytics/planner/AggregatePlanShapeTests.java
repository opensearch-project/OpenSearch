/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchAggregate}.
 *
 * <p>1-shard inputs: {@code Aggregate(SINGLE)} runs at the shard, ER above.
 * <p>Multi-shard: {@code OpenSearchAggregateSplitRule} splits into PARTIAL/FINAL with an
 * ER in between.
 */
public class AggregatePlanShapeTests extends PlanShapeTestBase {

    // ---- Non-prefix groupSet: agg arg projected BEFORE the group key (the `avg(x) by span(y,5)` ----
    // ---- shape). PARTIAL fronts the key per Calcite's contract, so FINAL must group on the prefix ----
    // ---- range {0}, NOT the original {1}. Regression guard for the multi-shard span bug where FINAL ----
    // ---- grouped on the partial-state column and produced spurious un-coalesced buckets. ----

    /** Builds sum(col0) grouped by col1 → non-prefix groupSet {1}. */
    private RelNode nonPrefixGroupedSum() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total_status"
        );
        return makeAggregate(scan, ImmutableBitSet.of(1), sum);
    }

    public void testNonPrefixGroupSet_1shard() {
        RelNode result = runPlanner(nonPrefixGroupedSum(), singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{1}], total_status=[SUM($0)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testNonPrefixGroupSet_2shard() {
        // FINAL groups on {0} (key fronted by PARTIAL), NOT the original {1}. Before the fix FINAL
        // carried {1} and grouped on the SUM column instead of the key.
        RelNode result = runPlanner(nonPrefixGroupedSum(), multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_status=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{1}], total_status=[SUM($0)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsCountStarByKey_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, countStarCall(scan));
        RelNode result = runPlanner(plan, singleShardContext());
        // Field trimming inserts Project(status) above the scan — count-by-key needs only the group key.
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testStatsCountStarByKey_2shard() {
        // FINAL's COUNT is rebuilt as SUM($1) by the COUNT→SUM swap in FinalAggCallBuilder.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, countStarCall(scan));
        RelNode result = runPlanner(plan, multiShardContext());
        // Field trimming inserts Project(status) above the scan — count-by-key needs only the group key.
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsSumByKey_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, sumCall(scan));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testStatsSumByKey_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, sumCall(scan));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsAvgByKey_1shard() {
        // AVG → SUM/COUNT primitives plus a Project for the quotient; SINGLE only.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), avgCall(scan));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], avg_size=[ANNOTATED_PROJECT_EXPR(id=3, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=2, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsAvgByKey_2shard() {
        // AVG decomposes pre-split; FINAL receives the reduced primitives.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), avgCall(scan));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], avg_size=[ANNOTATED_PROJECT_EXPR(id=3, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=2, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], $f1=[SUM($1)], $f2=[SUM($2)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsSumCountByKey_1shard() {
        // SINGLE aggregate carries both calls; no split, no rebase.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), sumCall(scan), countStarCall(scan));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testStatsSumCountByKey_2shard() {
        // FINAL: SUM stays (engine-native merge), COUNT→SUM($2) via FinalAggCallBuilder.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), sumCall(scan), countStarCall(scan));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], cnt=[SUM($2)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Empty-group count(), single-shard. SINGLE alternative; no split, no wrap. */
    public void testStatsCountStar_emptyGroup_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(), countStarCall(scan));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{}], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * Empty-group count(), multi-shard. The Project on top is the CAST-wrap from
     * {@code OpenSearchAggregateSplitRule.wrapWithCastIfNeeded} — without it, Volcano rejects
     * FINAL's nullable BIGINT against SINGLE's BIGINT NOT NULL.
     */
    public void testStatsCountStar_emptyGroup_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(), countStarCall(scan));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(cnt=[CAST($0):BIGINT NOT NULL], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{}], cnt=[SUM($0)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ---- COUNT(DISTINCT x) → APPROX_COUNT_DISTINCT(x) (engine-native HLL sketch merge) ----

    /**
     * 1-shard {@code COUNT(DISTINCT x)} — the HEP {@code OpenSearchDistinctCountRule} rewrites to
     * {@code APPROX_COUNT_DISTINCT(x)}, then no split (single shard). SINGLE at the shard.
     */
    public void testCountDistinct_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, countDistinctCall(scan));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * Multi-shard {@code COUNT(DISTINCT x)} — rewritten to {@code APPROX_COUNT_DISTINCT(x)} and then
     * split via {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule}. FINAL
     * keeps the {@code APPROX_COUNT_DISTINCT} operator (engine-native merge: reducer == self), reads
     * column $1 of the gathered exchange. {@code DistributedAggregateRewriter} retypes the exchange
     * column to {@code VARBINARY} (HLL sketch state) downstream during DAG-cut + fragment conversion.
     */
    public void testCountDistinct_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeAggregate(scan, countDistinctCall(scan));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
